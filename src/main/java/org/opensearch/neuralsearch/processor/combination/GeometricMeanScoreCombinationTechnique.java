/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.describeCombinationTechnique;

/**
 * Abstracts combination of scores based on geometrical mean method
 */
@ToString(onlyExplicitlyIncluded = true)
public class GeometricMeanScoreCombinationTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "geometric_mean";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Float> weights;
    private final ScoreCombinationUtil scoreCombinationUtil;

    public GeometricMeanScoreCombinationTechnique(final Map<String, Object> params, final ScoreCombinationUtil combinationUtil) {
        scoreCombinationUtil = combinationUtil;
        scoreCombinationUtil.validateParams(params, SUPPORTED_PARAMS);
        weights = scoreCombinationUtil.getWeights(params);
    }

    /**
     * Weighted geometric mean method for combining scores.
     *
     * We use formula below to calculate mean. It's based on fact that logarithm of geometric mean is the
     * weighted arithmetic mean of the logarithms of individual scores.
     *
     * geometric_mean = exp(sum(weight_1*ln(score_1) + .... + weight_n*ln(score_n))/sum(weight_1 + ... + weight_n))
     */
    @Override
    public float combine(final float[] scores) {
        scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
        float weightedLnSum = 0;
        float sumOfWeights = 0;
        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
            float score = scores[indexOfSubQuery];
            if (score <= 0) {
                // scores 0.0 need to be skipped, ln() of 0 is not defined
                continue;
            }
            float weight = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
            sumOfWeights += weight;
            weightedLnSum += weight * Math.log(score);
        }
        return sumOfWeights == 0 ? ZERO_SCORE : (float) Math.exp(weightedLnSum / sumOfWeights);
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, weights);
    }
}
