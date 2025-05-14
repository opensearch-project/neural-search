/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.describeCombinationTechnique;

/**
 * Abstracts combination of scores based on reciprocal rank fusion algorithm
 */
@Log4j2
@ToString(onlyExplicitlyIncluded = true)
public class RRFScoreCombinationTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "rrf";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    @ToString.Include
    private final List<Float> weights;
    private static final Float ZERO_SCORE = 0.0f;
    private final ScoreCombinationUtil scoreCombinationUtil;

    public RRFScoreCombinationTechnique(final Map<String, Object> params, final ScoreCombinationUtil combinationUtil) {
        scoreCombinationUtil = combinationUtil;
        scoreCombinationUtil.validateParams(params, SUPPORTED_PARAMS);
        weights = scoreCombinationUtil.getWeights(params);
    }

    @Override
    public float combine(final float[] scores) {
        if (Objects.isNull(scores)) {
            throw new IllegalArgumentException("scores array cannot be null");
        }
        scoreCombinationUtil.validateIfWeightsMatchScores(scores, weights);
        float sumScores = 0.0f;
        float sumOfWeights = 0;

        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.length; indexOfSubQuery++) {
            float score = scores[indexOfSubQuery];
            if (score >= 0.0) {
                float weight = scoreCombinationUtil.getWeightForSubQuery(weights, indexOfSubQuery);
                score = score * weight;
                sumScores += score;
                sumOfWeights += weight;
            }
        }
        if (sumOfWeights == 0.0f) {
            return ZERO_SCORE;
        }

        return sumScores;
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, List.of());
    }
}
