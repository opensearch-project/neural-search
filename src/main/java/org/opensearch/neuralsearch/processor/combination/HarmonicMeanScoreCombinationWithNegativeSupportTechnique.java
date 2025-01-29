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

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.describeCombinationTechnique;

public class HarmonicMeanScoreCombinationWithNegativeSupportTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "harmonic_mean_with_negative_support";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Float> weights;
    private final ScoreCombinationUtil scoreCombinationUtil;

    public HarmonicMeanScoreCombinationWithNegativeSupportTechnique(
        final Map<String, Object> params,
        final ScoreCombinationUtil combinationUtil
    ) {
        scoreCombinationUtil = combinationUtil;
        scoreCombinationUtil.validateParams(params, SUPPORTED_PARAMS);
        weights = scoreCombinationUtil.getWeights(params);
    }

    /**
     * Weighted harmonic mean method for combining scores.
     * score = sum(weight_1 + .... + weight_n)/sum(weight_1/score_1 + ... + weight_n/score_n)
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    @Override
    public float combine(final float[] scores) {
        scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
        float sumOfWeights = 0;
        float sumOfHarmonics = 0;
        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
            float score = scores[indexOfSubQuery];
            float weightOfSubQuery = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
            sumOfWeights += weightOfSubQuery;
            sumOfHarmonics += weightOfSubQuery / score;
        }
        return sumOfHarmonics > 0 ? sumOfWeights / sumOfHarmonics : ZERO_SCORE;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, weights);
    }
}
