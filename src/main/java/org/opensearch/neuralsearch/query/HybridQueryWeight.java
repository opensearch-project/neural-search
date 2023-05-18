/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Calculates query weights and build query scorers for hybrid query.
 */
class HybridQueryWeight extends Weight {

    private final HybridQuery queries;
    // The Weights for our subqueries, in 1-1 correspondence
    protected final ArrayList<Weight> weights;

    private final ScoreMode scoreMode;

    /**
     * Construct the Weight for this Query searched by searcher. Recursively construct subquery weights.
     */
    public HybridQueryWeight(HybridQuery hybridQuery, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        super(hybridQuery);
        this.queries = hybridQuery;
        weights = new ArrayList<>();
        for (Query query : hybridQuery.getSubQueries()) {
            weights.add(searcher.createWeight(query, scoreMode, boost));
        }
        this.scoreMode = scoreMode;
    }

    /**
     * Returns Matches for a specific document, or null if the document does not match the parent query
     *
     * @param context the reader's context to create the {@link Matches} for
     * @param doc     the document's id relative to the given context's reader
     * @return
     * @throws IOException
     */
    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
        List<Matches> mis = new ArrayList<>();
        for (Weight weight : weights) {
            Matches mi = weight.matches(context, doc);
            if (mi != null) {
                mis.add(mi);
            }
        }
        return MatchesUtils.fromSubMatches(mis);
    }

    /**
     * Create the scorer used to score our associated Query
     *
     * @param context the {@link LeafReaderContext} for which to return the
     *                {@link Scorer}.
     * @return scorer of hybrid query that contains scorers of each sub-query, null if there are no matches in any sub-query
     * @throws IOException
     */
    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        Scorer[] scorers = new Scorer[weights.size()];
        for (int i = 0; i < weights.size(); i++) {
            Weight w = weights.get(i);
            Scorer subScorer = w.scorer(context);
            if (subScorer != null) {
                scorers[i] = subScorer;
            }
        }
        // if there are no matches in any of the scorers (sub-queries) we need to return
        // scorer as null to avoid problems with disi result iterators
        if (Arrays.stream(scorers).allMatch(Objects::isNull)) {
            return null;
        }
        return new HybridQueryScorer(this, scorers);
    }

    /**
     * Check if weight object can be cached
     *
     * @param ctx
     * @return true if the object can be cached against a given leaf
     */
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        for (Weight w : weights) {
            if (!w.isCacheable(ctx)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Explain is not supported for hybrid query
     *
     * @param context the readers context to create the {@link Explanation} for.
     * @param doc     the document's id relative to the given context's reader
     * @return
     * @throws IOException
     */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        throw new UnsupportedOperationException("Explain is not supported");
    }
}
