/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;

import static org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil.MAX_UNSIGNED_BYTE_VALUE;

/**
 * Selects appropriate scorer based on segment characteristics and query configuration.
 * Chooses between SEISMIC-optimized scorers and fallback rank feature scorers.
 */
@AllArgsConstructor
@Setter
@Getter
public class ScorerSelector {

    private SparseQueryWeight sparseQueryWeight;

    /**
     * Selects a scorer supplier for the given context.
     * Uses SEISMIC scorer if segment supports it, otherwise falls back to rank feature query.
     *
     * @param context the leaf reader context
     * @param query the sparse vector query
     * @param filterBitIterator optional filter for document filtering
     * @return appropriate scorer supplier for the segment
     */
    public ScorerSupplier select(LeafReaderContext context, SparseVectorQuery query, BitSetIterator filterBitIterator) throws IOException {
        SegmentInfo info = Lucene.segmentReader(context.reader()).getSegmentInfo().info;
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(query.getFieldName());
        // fallback to plain neural sparse query
        if (!PredicateUtils.shouldRunSeisPredicate.test(info, fieldInfo)) {
            Weight rankFeaturePhaseOneWeight = sparseQueryWeight.getRankFeaturePhaseOneWeight();
            Weight rankFeaturePhaseTwoWeight = sparseQueryWeight.getRankFeaturePhaseTwoWeight();
            Weight fallbackQueryWeight = sparseQueryWeight.getFallbackQueryWeight();
            SparseQueryTwoPhaseInfo sparseQueryTwoPhaseInfo = query.getSparseQueryTwoPhaseInfo();
            if (sparseQueryTwoPhaseInfo != null && rankFeaturePhaseOneWeight != null && rankFeaturePhaseTwoWeight != null) {
                ScorerSupplier phaseOneScorerSupplier = rankFeaturePhaseOneWeight.scorerSupplier(context);
                ScorerSupplier phaseTwoScorerSupplier = rankFeaturePhaseTwoWeight.scorerSupplier(context);
                if (phaseOneScorerSupplier == null) {
                    phaseOneScorerSupplier = new GeneralScorerSupplier(new EmptyScorer());
                }

                if (phaseTwoScorerSupplier == null) {
                    phaseTwoScorerSupplier = new GeneralScorerSupplier(new EmptyScorer());
                }

                return new TwoPhaseScorerSupplier(
                    phaseOneScorerSupplier,
                    phaseTwoScorerSupplier,
                    filterBitIterator,
                    (int) (query.getQueryContext().getK() * query.getSparseQueryTwoPhaseInfo().getExpansionRatio())
                );
            } else {
                return fallbackQueryWeight.scorerSupplier(context);
            }
        }
        final Scorer scorer = selectSeismicScorer(query, context, info, filterBitIterator);
        return new GeneralScorerSupplier(scorer);
    }

    @VisibleForTesting
    Scorer selectSeismicScorer(
        SparseVectorQuery query,
        LeafReaderContext context,
        SegmentInfo segmentInfo,
        BitSetIterator filterBitIterator
    ) throws IOException {

        SparseVectorReader cacheGatedForwardIndexReader = SparseVectorReader.NOOP_READER;
        FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(query.getFieldName());
        float rescaledBoost = sparseQueryWeight.getBoost() * ByteQuantizationUtil.getCeilingValueIngest(fieldInfo) * ByteQuantizationUtil
            .getCeilingValueSearch(fieldInfo) / MAX_UNSIGNED_BYTE_VALUE / MAX_UNSIGNED_BYTE_VALUE;

        if (segmentInfo != null) {
            CacheKey key = new CacheKey(segmentInfo, query.getFieldName());
            ForwardIndexCacheItem cacheItem = sparseQueryWeight.getForwardIndexCache().getOrCreate(key, segmentInfo.maxDoc());
            cacheGatedForwardIndexReader = getCacheGatedForwardIndexReader(cacheItem, context.reader(), query.getFieldName());
        }
        Similarity.SimScorer simScorer = ByteQuantizationUtil.getSimScorer(rescaledBoost);
        if (filterBitIterator != null) {
            int ord = (int) filterBitIterator.cost();
            if (ord <= query.getQueryContext().getK()) {
                return new ExactMatchScorer(filterBitIterator, query.getQueryVector(), cacheGatedForwardIndexReader, simScorer);
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

    @AllArgsConstructor
    @VisibleForTesting
    static class GeneralScorerSupplier extends ScorerSupplier {

        private Scorer scorer;

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

    @VisibleForTesting
    static class EmptyScorer extends Scorer {

        @Override
        public int docID() {
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                @Override
                public int docID() {
                    return DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public int nextDoc() throws IOException {
                    return DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) throws IOException {
                    return DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return 0;
        }

        @Override
        public float score() throws IOException {
            return 0;
        }
    }
}
