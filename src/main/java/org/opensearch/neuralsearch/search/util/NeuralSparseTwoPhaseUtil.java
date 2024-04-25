/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.opensearch.neuralsearch.query.NeuralSparseQuery;
import org.opensearch.neuralsearch.query.NeuralSparseTwoPhaseParameters;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.QueryRescorer;
import org.opensearch.search.rescore.RescoreContext;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.Float.max;

/**
 * Util class for do two phase preprocess for the NeuralSparseQuery.
 * Include adding the second phase query to searchContext and set the currentQuery to highScoreTokenQuery.
 */
public class NeuralSparseTwoPhaseUtil {
    /**
     * This function determine any neuralSparseQuery from query, extract lowScoreTokenQuery from each of them,
     * And as that, the neuralSparseQuery be extracted 's currentQuery will be changed from allScoreTokenQuery to highScoreTokenQuery.
     * Then build a QueryRescoreContext of these extra lowScoreTokenQuery and add the built QueryRescoreContext to the searchContext.
     * Finally, the score of TopDocs will be sum of highScoreTokenQuery and lowScoreTokenQuery, which equals to allTokenQuery.
     * @param query         The whole query include neuralSparseQuery to executed.
     * @param searchContext The searchContext with this query.
     */
    public static void addRescoreContextFromNeuralSparseQuery(final Query query, final SearchContext searchContext) {
        Map<NeuralSparseQuery, Float> neuralSparseQuery2Weight = new HashMap<>();
        // Store all neuralSparse query and it's global weight in neuralSparseQuery2Weight, and get the max windowSizeExpansion of them..
        float windowSizeExpansion = populateQueryWeightsMapAndGetWindowSizeExpansion(query, neuralSparseQuery2Weight, 1.0f, 1.0f);
        Query twoPhaseQuery = getNestedTwoPhaseQueryFromNeuralSparseQuerySet(neuralSparseQuery2Weight);
        if (twoPhaseQuery == null) return;
        // Set the valid neural_sparse query's current query to it's highScoreTokenQuery.
        neuralSparseQuery2Weight.keySet().forEach(NeuralSparseQuery::setCurrentQueryToHighScoreTokenQuery);
        // Add two phase to searchContext's rescore list.
        addTwoPhaseQuery2RescoreContext(searchContext, windowSizeExpansion, twoPhaseQuery);
    }

    private static float populateQueryWeightsMapAndGetWindowSizeExpansion(
        final Query query,
        Map<NeuralSparseQuery, Float> query2weight,
        float weight,
        float windoSizeExpansion
    ) {
        if (query instanceof BoostQuery) {
            BoostQuery boostQuery = (BoostQuery) query;
            weight *= boostQuery.getBoost();
            windoSizeExpansion = max(
                windoSizeExpansion,
                populateQueryWeightsMapAndGetWindowSizeExpansion(boostQuery.getQuery(), query2weight, weight, windoSizeExpansion)
            );
        } else if (query instanceof BooleanQuery) {
            for (BooleanClause clause : (BooleanQuery) query) {
                if (clause.isScoring()) {
                    windoSizeExpansion = max(
                        windoSizeExpansion,
                        populateQueryWeightsMapAndGetWindowSizeExpansion(clause.getQuery(), query2weight, weight, windoSizeExpansion)
                    );
                }
            }
        } else if (query instanceof NeuralSparseQuery) {
            query2weight.put(((NeuralSparseQuery) query), weight);
            windoSizeExpansion = max(windoSizeExpansion, ((NeuralSparseQuery) query).getRescoreWindowSizeExpansion());
        }
        // ToDo Support for other compound query.
        return windoSizeExpansion;
    }

    private static float getOriginQueryWeightAfterRescore(final List<RescoreContext> rescoreContextList) {
        return rescoreContextList.stream()
            .filter(ctx -> ctx instanceof QueryRescorer.QueryRescoreContext)
            .map(ctx -> ((QueryRescorer.QueryRescoreContext) ctx).queryWeight())
            .reduce(1.0f, (a, b) -> a * b);
    }

    private static void addTwoPhaseQuery2RescoreContext(
        final SearchContext searchContext,
        final float windowSizeExpansion,
        Query twoPhaseQuery
    ) {
        int curWindowSize = (int) (searchContext.size() * windowSizeExpansion);
        if (curWindowSize < 0 || curWindowSize > NeuralSparseTwoPhaseParameters.MAX_WINDOW_SIZE) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Two phase final windowSize %d out of score with limit %d. "
                        + "You can change the value of cluster setting [plugins.neural_search.neural_sparse.two_phase.max_window_size] "
                        + "to a integer at least 50.",
                    curWindowSize,
                    NeuralSparseTwoPhaseParameters.MAX_WINDOW_SIZE
                )
            );
        }
        QueryRescorer.QueryRescoreContext rescoreContext = new QueryRescorer.QueryRescoreContext(curWindowSize);
        rescoreContext.setQuery(twoPhaseQuery);
        rescoreContext.setRescoreQueryWeight(getOriginQueryWeightAfterRescore(searchContext.rescore()));
        searchContext.addRescore(rescoreContext);
    }

    private static Query getNestedTwoPhaseQueryFromNeuralSparseQuerySet(final Map<NeuralSparseQuery, Float> originNeuralSparse2weight) {
        if (originNeuralSparse2weight.isEmpty()) return null;
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        originNeuralSparse2weight.forEach(
            (neuralSparseQuery, weight) -> builder.add(
                new BoostQuery(neuralSparseQuery.getLowScoreTokenQuery(), weight),
                BooleanClause.Occur.SHOULD
            )
        );
        return builder.build();
    }

}
