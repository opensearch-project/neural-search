/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.describeCombinationTechnique;

public class ArithmeticMeanScoreCombinationWithNegativeSupportTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "arithmetic_mean_with_negative_support";
    public static final String PARAM_NAME_WEIGHTS = "weights";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Float> weights;
    private final ScoreCombinationUtil scoreCombinationUtil;

    public ArithmeticMeanScoreCombinationWithNegativeSupportTechnique(
        final Map<String, Object> params,
        final ScoreCombinationUtil combinationUtil
    ) {
        scoreCombinationUtil = combinationUtil;
        scoreCombinationUtil.validateParams(params, SUPPORTED_PARAMS);
        weights = scoreCombinationUtil.getWeights(params);
    }

    /**
     * Arithmetic mean method for combining scores.
     * score = (weight1*score1 + weight2*score2 +...+ weightN*scoreN)/(weight1 + weight2 + ... + weightN)
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    @Override
    public float combine(final float[] scores) {
        scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
        float combinedScore = 0.0f;
        float sumOfWeights = 0;
        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
            float score = scores[indexOfSubQuery];
            float weight = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
            score = score * weight;
            combinedScore += score;
            sumOfWeights += weight;
        }
        if (sumOfWeights == 0.0f) {
            return ZERO_SCORE;
        }
        return combinedScore / sumOfWeights;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, weights);
    }
}
