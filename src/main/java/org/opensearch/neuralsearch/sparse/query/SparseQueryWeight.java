/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;
import org.opensearch.neuralsearch.sparse.query.explain.SparseExplanationBuilder;

import java.io.IOException;
import java.util.Locale;

import static org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE;

/**
 * Weight class for SparseVectorQuery
 */
@Log4j2
public class SparseQueryWeight extends Weight {
    private final float boost;
    private final Weight fallbackQueryWeight;
    private final ForwardIndexCache forwardIndexCache;

    public SparseQueryWeight(
        SparseVectorQuery query,
        IndexSearcher searcher,
        ScoreMode scoreMode,
        float boost,
        ForwardIndexCache forwardIndexCache
    ) throws IOException {
        super(query);
        this.boost = boost;
        this.forwardIndexCache = forwardIndexCache;
        this.fallbackQueryWeight = query.getFallbackQuery().createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        final SparseVectorQuery query = (SparseVectorQuery) parentQuery;

        SegmentInfo info = Lucene.segmentReader(context.reader()).getSegmentInfo().info;
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(query.getFieldName());
        final boolean isNativeEngine = SparseEngine.NATIVE == SparseFieldUtils.getSparseEngine(fieldInfo);
        final boolean isSeismicSegment = PredicateUtils.shouldRunSeisPredicate.test(info, fieldInfo);

        // The fallback query scores FeatureFields, which a native field never writes (see
        // SparseVectorFieldMapper#parseCreateField), so for the native engine it would explain a
        // noMatch for a document the native scorer did score. Native explains every segment itself.
        if (!isNativeEngine && !isSeismicSegment) {
            // Fallback to plain neural sparse query explanation
            return fallbackQueryWeight.explain(context, doc);
        }

        SparseExplanationBuilder.SparseExplanationBuilderBuilder builder = SparseExplanationBuilder.builder()
            .context(context)
            .docId(doc)
            .query(query)
            .boost(boost)
            .fieldInfo(fieldInfo)
            .nativeEngine(isNativeEngine);

        if (isNativeEngine) {
            // The raw vectors reach disk through the delegate consumer for native fields too
            // (BaseSparseDocValuesConsumer#addBinaryField), so the document is recomputable from doc
            // values alone -- no need to read it back out of the native index. Deliberately not
            // routed through the forward index cache: a native segment's index is an mmap'd file and
            // nothing else populates that cache for it, so filling it here would charge the circuit
            // breaker for memory no query benefits from.
            BinaryDocValues docValues = context.reader().getBinaryDocValues(query.getFieldName());
            if (docValues == null) {
                return Explanation.noMatch(
                    String.format(Locale.ROOT, "field '%s' has no doc values in this segment", query.getFieldName())
                );
            }
            if (isSeismicSegment) {
                // Scored by a quantized seismic index, so the byte-code breakdown applies.
                builder.reader(new SparseBinaryDocValuesPassThrough(docValues, info, fieldInfo));
            } else {
                // Scored by nsparse's inverted index, which holds unquantized floats and computes an
                // exact dot product, so quantizing here would explain a score nothing produced.
                builder.reader(SparseVectorReader.NOOP_READER).rawDocValues(docValues);
            }
        } else if (info != null) {
            CacheKey key = new CacheKey(info, query.getFieldName());
            ForwardIndexCacheItem cacheItem = forwardIndexCache.getOrCreate(key, info.maxDoc());
            builder.reader(getCacheGatedForwardIndexReader(cacheItem, context.reader(), query.getFieldName()));
        } else {
            builder.reader(SparseVectorReader.NOOP_READER);
        }

        return builder.build().explain();
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final SparseVectorQuery query = (SparseVectorQuery) parentQuery;
        SegmentInfo info = Lucene.segmentReader(context.reader()).getSegmentInfo().info;
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(query.getFieldName());
        boolean isNativeEngine = SparseEngine.NATIVE == SparseFieldUtils.getSparseEngine(fieldInfo);
        // fallback to plain neural sparse query
        if (!isNativeEngine && !PredicateUtils.shouldRunSeisPredicate.test(info, fieldInfo)) {
            return fallbackQueryWeight.scorerSupplier(context);
        }
        final Scorer scorer = selectScorer(query, context, info);
        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                return scorer;
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                return new BulkScorer() {
                    // We ignore the max value as our algorithm can't limit the docId to range of (min, max)
                    // so, to ensure it's only called once, we return the maxDoc
                    @Override
                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                        collector.setScorer(scorer);
                        DocIdSetIterator iter = scorer.iterator();
                        int docId = iter.nextDoc();
                        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                            collector.collect(docId);
                            docId = iter.nextDoc();
                        }
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }
                };
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @VisibleForTesting
    Scorer selectScorer(SparseVectorQuery query, LeafReaderContext context, SegmentInfo segmentInfo) throws IOException {
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(query.getFieldName());

        BitSetIterator filterBitIterator = null;
        // Kept alongside the iterator: cardinality() is a scan of the bitset's words, and the exact
        // match decision below needs the same number the iterator was built with.
        int filterCardinality = 0;
        if (query.getFilterResults() != null) {
            BitSet filter = query.getFilterResults().get(context.id());
            if (filter != null) {
                filterCardinality = filter.cardinality();
                filterBitIterator = new BitSetIterator(filter, filterCardinality);
            }
        }
        if (SparseEngine.NATIVE == SparseFieldUtils.getSparseEngine(fieldInfo)) {
            return new NativeIndexScorer(
                fieldInfo,
                query.getQueryContext(),
                query.getRawQueryTokens(),
                context.reader(),
                segmentInfo,
                context.reader().getLiveDocs(),
                filterBitIterator,
                boost
            );
        }
        SparseVectorReader cacheGatedForwardIndexReader = SparseVectorReader.NOOP_READER;
        float rescaledBoost = boost * ByteQuantizationUtil.getCeilingValueIngest(fieldInfo) * ByteQuantizationUtil.getCeilingValueSearch(
            fieldInfo
        ) / MAX_UNSIGNED_BYTE_VALUE / MAX_UNSIGNED_BYTE_VALUE;

        if (segmentInfo != null) {
            CacheKey key = new CacheKey(segmentInfo, query.getFieldName());
            ForwardIndexCacheItem cacheItem = forwardIndexCache.getOrCreate(key, segmentInfo.maxDoc());
            cacheGatedForwardIndexReader = getCacheGatedForwardIndexReader(cacheItem, context.reader(), query.getFieldName());
        }
        Similarity.SimScorer simScorer = ByteQuantizationUtil.getSimScorer(rescaledBoost);
        if (filterBitIterator != null && filterCardinality <= query.getQueryContext().getK()) {
            return new ExactMatchScorer(filterBitIterator, query.getQueryVector(), cacheGatedForwardIndexReader, simScorer);
        }
        return new OrderedPostingWithClustersScorer(
            query.getFieldName(),
            query.getQueryContext(),
            query.getQueryVector(),
            context.reader(),
            context.reader().getLiveDocs(),
            cacheGatedForwardIndexReader,
            simScorer,
            filterBitIterator
        );
    }

    private SparseVectorReader getCacheGatedForwardIndexReader(SparseVectorForwardIndex index, LeafReader leafReader, String fieldName)
        throws IOException {
        BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldName);
        if (docValues instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough) {
            return new CacheGatedForwardIndexReader(index.getReader(), index.getWriter(), sparseBinaryDocValuesPassThrough);
        }
        return SparseVectorReader.NOOP_READER;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
