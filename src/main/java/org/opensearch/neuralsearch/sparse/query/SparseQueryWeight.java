/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;

import java.io.IOException;

/**
 * Weight class for SparseVectorQuery
 */
@Log4j2
@Getter
public class SparseQueryWeight extends Weight {
    private final float boost;
    private final Weight fallbackQueryWeight;
    private final Weight rankFeaturePhaseOneWeight;
    private final Weight rankFeaturePhaseTwoWeight;
    private final ForwardIndexCache forwardIndexCache;
    private final ScorerSelector selector;

    public SparseQueryWeight(
        SparseVectorQuery query,
        IndexSearcher searcher,
        ScoreMode scoreMode,
        float boost,
        ForwardIndexCache forwardIndexCache,
        ScorerSelector scorerSelector
    ) throws IOException {
        super(query);
        this.boost = boost;
        this.forwardIndexCache = forwardIndexCache;
        this.fallbackQueryWeight = query.getFallbackQuery().createWeight(searcher, scoreMode, boost);
        this.rankFeaturePhaseOneWeight = query.getRankFeaturesPhaseOneQuery() != null
            ? query.getRankFeaturesPhaseOneQuery().createWeight(searcher, scoreMode, boost)
            : null;
        this.rankFeaturePhaseTwoWeight = query.getRankFeaturesPhaseTwoQuery() != null
            ? query.getRankFeaturesPhaseTwoQuery().createWeight(searcher, scoreMode, boost)
            : null;
        if (scorerSelector == null) {
            selector = new ScorerSelector(this);
        } else {
            selector = scorerSelector;
            selector.setSparseQueryWeight(this);
        }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final SparseVectorQuery query = (SparseVectorQuery) parentQuery;
        BitSetIterator filterBitIterator = getFilterBitIterator(query, context);
        return selector.select(context, query, filterBitIterator);
    }

    @VisibleForTesting
    BitSetIterator getFilterBitIterator(SparseVectorQuery query, LeafReaderContext context) {
        BitSetIterator filterBitIterator = null;
        if (query.getFilterResults() != null) {
            BitSet filter = query.getFilterResults().get(context.id());
            if (filter != null) {
                int ord = filter.cardinality();
                filterBitIterator = new BitSetIterator(filter, ord);
            }
        }
        return filterBitIterator;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
