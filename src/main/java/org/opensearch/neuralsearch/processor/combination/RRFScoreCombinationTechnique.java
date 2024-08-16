/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.ToString;

/**
 * Abstracts combination of scores based on geometrical mean method
 */
@ToString(onlyExplicitlyIncluded = true)

public class RRFScoreCombinationTechnique implements ScoreCombinationTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    //public static final String PARAM_NAME_WEIGHTS = "weights";
    //private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    //private static final Float ZERO_SCORE = 0.0f;
    //private final List<Float> weights;
    //private final ScoreCombinationUtil scoreCombinationUtil;

    //public RRFScoreCombinationTechnique(final Map<String, Object> params, final ScoreCombinationUtil combinationUtil) {
    //    scoreCombinationUtil = combinationUtil;
    //    scoreCombinationUtil.validateParams(params, SUPPORTED_PARAMS);
    //    weights = scoreCombinationUtil.getWeights(params);
    //}

    /**
     * placeholder class for passing RRF rankScores to combiner,
     * as of 08/2024, weights are not supported for RRF technique
     */
    @Override
    public float combine(final float[] scores) {
        return 0.0f;
    }
        //scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
        //float weightedLnSum = 0;
        //float sumOfWeights = 0;
        //for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
        //    float score = scores[indexOfSubQuery];
        //    if (score <= 0) {
        //        // scores 0.0 need to be skipped, ln() of 0 is not defined
        //        continue;
        //    }
        //    float weight = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
        //    sumOfWeights += weight;
        //    weightedLnSum += weight * Math.log(score);
        //}
        //return sumOfWeights == 0 ? ZERO_SCORE : (float) Math.exp(weightedLnSum / sumOfWeights);
    //}

}
