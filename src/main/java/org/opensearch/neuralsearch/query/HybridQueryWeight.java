/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Calculates query weights and build query scorers for hybrid query.
 */
public final class HybridQueryWeight extends Weight {

    private final HybridQuery queries;
    // The Weights for our subqueries, in 1-1 correspondence
    private final List<Weight> weights;

    private final ScoreMode scoreMode;

    /**
     * Construct the Weight for this Query searched by searcher. Recursively construct subquery weights.
     */
    public HybridQueryWeight(HybridQuery hybridQuery, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        super(hybridQuery);
        this.queries = hybridQuery;
        weights = hybridQuery.getSubQueries().stream().map(q -> {
            try {
                return searcher.createWeight(q, scoreMode, boost);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
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
        List<Matches> mis = weights.stream().map(weight -> {
            try {
                return weight.matches(context, doc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
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
        List<Scorer> scorers = weights.stream().map(w -> {
            try {
                return w.scorer(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        // if there are no matches in any of the scorers (sub-queries) we need to return
        // scorer as null to avoid problems with disi result iterators
        if (scorers.stream().allMatch(Objects::isNull)) {
            return null;
        }
        return new HybridQueryScorer(this, scorers.toArray(new Scorer[0]));
    }

    /**
     * Check if weight object can be cached
     *
     * @param ctx
     * @return true if the object can be cached against a given leaf
     */
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return weights.stream().allMatch(w -> w.isCacheable(ctx));
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
