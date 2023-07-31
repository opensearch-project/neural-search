/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstracts combination of scores based on arithmetic mean method
 */
public class ArithmeticMeanScoreCombinationTechnique extends AbstractScoreCombinationTechnique implements ScoreCombinationTechnique {

    public static final String TECHNIQUE_NAME = "arithmetic_mean";
    public static final String PARAM_NAME_WEIGHTS = "weights";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Float> weights;

    public ArithmeticMeanScoreCombinationTechnique(final Map<String, Object> params) {
        validateParams(params);
        weights = getWeights(params);
    }

    /**
     * Arithmetic mean method for combining scores.
     * score = (weight1*score1 + weight2*score2 +...+ weightN*scoreN)/(weight1 + weight2 + ... + weightN)
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    @Override
    public float combine(final float[] scores) {
        float combinedScore = 0.0f;
        float weights = 0;
        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
            float score = scores[indexOfSubQuery];
            if (score >= 0.0) {
                float weight = getWeightForSubQuery(this.weights, indexOfSubQuery);
                score = score * weight;
                combinedScore += score;
                weights += weight;
            }
        }
        if (weights == 0.0f) {
            return ZERO_SCORE;
        }
        return combinedScore / weights;
    }

    @Override
    Set<String> getSupportedParams() {
        return SUPPORTED_PARAMS;
    }
}
