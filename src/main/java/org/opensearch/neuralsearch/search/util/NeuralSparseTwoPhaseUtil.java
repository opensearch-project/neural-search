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
import java.util.Map;

import static java.lang.Float.max;
import static java.lang.Integer.min;
import static org.opensearch.index.IndexSettings.MAX_RESCORE_WINDOW_SETTING;

public class NeuralSparseTwoPhaseUtil {

    private static float populateQueryWeightsMapAndGetWindowSizeExpansion(
        final Query query,
        Map<Query, Float> query2Weight,
        float weight,
        float windoSizeExpansion
    ) {
        if (query instanceof BoostQuery) {
            BoostQuery boostQuery = (BoostQuery) query;
            weight *= boostQuery.getBoost();
            windoSizeExpansion = max(
                windoSizeExpansion,
                populateQueryWeightsMapAndGetWindowSizeExpansion(boostQuery.getQuery(), query2Weight, weight, windoSizeExpansion)
            );
        } else if (query instanceof BooleanQuery) {
            for (BooleanClause clause : (BooleanQuery) query) {
                if (clause.isScoring()) {
                    windoSizeExpansion = max(
                        windoSizeExpansion,
                        populateQueryWeightsMapAndGetWindowSizeExpansion(clause.getQuery(), query2Weight, weight, windoSizeExpansion)
                    );
                }
            }
        } else if (query instanceof NeuralSparseQuery) {
            query2Weight.put(((NeuralSparseQuery) query).getLowScoreTokenQuery(), weight);
            ((NeuralSparseQuery) query).extractLowScoreToken();
            windoSizeExpansion = max(windoSizeExpansion, ((NeuralSparseQuery) query).getRescoreWindowSizeExpansion());
        }
        // ToDo Support for other compound query.
        return windoSizeExpansion;
    }

    private static float getOriginQueryWeightAfterRescore(List<RescoreContext> rescoreContextList) {
        return rescoreContextList.stream()
            .filter(ctx -> ctx instanceof QueryRescorer.QueryRescoreContext)
            .map(ctx -> ((QueryRescorer.QueryRescoreContext) ctx).queryWeight())
            .reduce(1.0f, (a, b) -> a * b);
    }

    private static Query getNestedTwoPhaseQuery(Map<Query, Float> query2weight) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        query2weight.forEach((query, weight) -> { builder.add(new BoostQuery(query, weight), BooleanClause.Occur.SHOULD); });
        return builder.build();
    }

    public static void addTwoPhaseNeuralSparseQuery(final Query query, SearchContext searchContext) {
        Map<Query, Float> query2weight = new HashMap<>();
        float windowSizeExpansion = populateQueryWeightsMapAndGetWindowSizeExpansion(query, query2weight, 1.0f, 1.0f);
        Query twoPhaseQuery;
        if (query2weight.isEmpty()) {
            return;
        } else if (query2weight.size() == 1) {
            Map.Entry<Query, Float> entry = query2weight.entrySet().stream().findFirst().get();
            twoPhaseQuery = new BoostQuery(entry.getKey(), entry.getValue());
        } else {
            twoPhaseQuery = getNestedTwoPhaseQuery(query2weight);
        }
        int curWindowSize = (int) (searchContext.size() * windowSizeExpansion);
        if (curWindowSize < 0
            || curWindowSize > min(
                NeuralSparseTwoPhaseParameters.MAX_WINDOW_SIZE,
                MAX_RESCORE_WINDOW_SETTING.get(searchContext.getQueryShardContext().getIndexSettings().getSettings())
            )) {
            throw new IllegalArgumentException(String.format("Two phase final windowSize out of score with value %d.", curWindowSize));
        }
        QueryRescorer.QueryRescoreContext rescoreContext = new QueryRescorer.QueryRescoreContext(curWindowSize);
        rescoreContext.setQuery(twoPhaseQuery);
        rescoreContext.setRescoreQueryWeight(getOriginQueryWeightAfterRescore(searchContext.rescore()));
        searchContext.addRescore(rescoreContext);
    }
}
