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
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizerUtil;

import java.io.IOException;

/**
 * Weight class for SparseVectorQuery
 */
@Log4j2
public class SparseQueryWeight extends Weight {
    private final float boost;
    private final float quantizationCeilSearch;
    private final float quantizationCeilIngest;
    private final Weight fallbackQueryWeight;
    private final ForwardIndexCache forwardIndexCache;

    public SparseQueryWeight(
        SparseVectorQuery query,
        IndexSearcher searcher,
        ScoreMode scoreMode,
        float boost,
        float quantizationCeilSearch,
        float quantizationCeilIngest,
        ForwardIndexCache forwardIndexCache
    ) throws IOException {
        super(query);
        this.boost = boost;
        this.quantizationCeilSearch = quantizationCeilSearch;
        this.quantizationCeilIngest = quantizationCeilIngest;
        this.forwardIndexCache = forwardIndexCache;
        this.fallbackQueryWeight = query.getFallbackQuery().createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final SparseVectorQuery query = (SparseVectorQuery) parentQuery;
        SegmentInfo info = Lucene.segmentReader(context.reader()).getSegmentInfo().info;
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(query.getFieldName());
        // fallback to plain neural sparse query
        if (!PredicateUtils.shouldRunSeisPredicate.test(info, fieldInfo)) {
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
        SparseVectorReader cacheGatedForwardIndexReader = SparseVectorReader.NOOP_READER;
        if (segmentInfo != null) {
            CacheKey key = new CacheKey(segmentInfo, query.getFieldName());
            ForwardIndexCacheItem cacheItem = forwardIndexCache.getOrCreate(key, segmentInfo.maxDoc());
            cacheGatedForwardIndexReader = getCacheGatedForwardIndexReader(cacheItem, context.reader(), query.getFieldName());
        }
        Similarity.SimScorer simScorer = ByteQuantizerUtil.getSimScorer(boost, quantizationCeilSearch, quantizationCeilIngest);
        BitSetIterator filterBitIterator = null;
        if (query.getFilterResults() != null) {
            BitSet filter = query.getFilterResults().get(context.id());
            if (filter != null) {
                int ord = filter.cardinality();
                filterBitIterator = new BitSetIterator(filter, ord);
                if (ord <= query.getQueryContext().getK()) {
                    return new ExactMatchScorer(filterBitIterator, query.getQueryVector(), cacheGatedForwardIndexReader, simScorer);
                }
            }
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
