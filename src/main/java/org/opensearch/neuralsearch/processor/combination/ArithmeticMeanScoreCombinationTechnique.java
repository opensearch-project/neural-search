/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.opensearch.OpenSearchParseException;

/**
 * Abstracts combination of scores based on arithmetic mean method
 */
public class ArithmeticMeanScoreCombinationTechnique implements ScoreCombinationTechnique {

    public static final String TECHNIQUE_NAME = "arithmetic_mean";
    public static final String PARAM_NAME_WEIGHTS = "weights";
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Double> weights;

    public ArithmeticMeanScoreCombinationTechnique(final Map<String, Object> params) {
        validateParams(params);
        if (Objects.isNull(params) || params.isEmpty()) {
            weights = List.of();
            return;
        }
        // get weights, we don't need to check for instance as it's done during validation
        weights = (List<Double>) params.getOrDefault(PARAM_NAME_WEIGHTS, new ArrayList<>());
    }

    /**
     * Arithmetic mean method for combining scores.
     * cscore = (score1 + score2 +...+ scoreN)/N
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    @Override
    public float combine(final float[] scores) {
        float combinedScore = 0.0f;
        int count = 0;
        for (int i = 0; i < scores.length; i++) {
            float score = scores[i];
            if (score >= 0.0) {
                // apply weight for this sub-query if it's set for particular sub-query
                if (i < weights.size()) {
                    score = (float) (score * weights.get(i));
                }
                combinedScore += score;
                count++;
            }
        }
        if (count == 0) {
            return ZERO_SCORE;
        }
        return combinedScore / count;
    }

    private void validateParams(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return;
        }
        // check if only supported params are passed
        Set<String> supportedParams = Set.of(PARAM_NAME_WEIGHTS);
        Optional<String> optionalNotSupportedParam = params.keySet()
            .stream()
            .filter(paramName -> !supportedParams.contains(paramName))
            .findFirst();
        if (optionalNotSupportedParam.isPresent()) {
            throw new OpenSearchParseException("provided parameter for combination technique is not supported");
        }

        // check param types
        if (params.keySet().stream().anyMatch(PARAM_NAME_WEIGHTS::equalsIgnoreCase)) {
            if (!(params.get(PARAM_NAME_WEIGHTS) instanceof List)) {
                throw new OpenSearchParseException("parameter {} must be a collection of numbers", PARAM_NAME_WEIGHTS);
            }
        }
    }
}
