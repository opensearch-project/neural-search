/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstracts combination of scores based on arithmetic mean method
 */
public class HarmonicMeanScoreCombinationTechnique implements ScoreCombinationTechnique {

    public static final String TECHNIQUE_NAME = "arithmetic_mean";
    public static final String PARAM_NAME_WEIGHTS = "weights";
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    private static final Float ZERO_SCORE = 0.0f;
    private final List<Float> weights;

    public HarmonicMeanScoreCombinationTechnique(final Map<String, Object> params) {
        validateParams(params);
        weights = getWeights(params);
    }

    private List<Float> getWeights(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return List.of();
        }
        // get weights, we don't need to check for instance as it's done during validation
        return ((List<Double>) params.getOrDefault(PARAM_NAME_WEIGHTS, List.of())).stream()
            .map(Double::floatValue)
            .collect(Collectors.toUnmodifiableList());
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
                float weight = getWeightForSubQuery(indexOfSubQuery);
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

    private void validateParams(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return;
        }
        // check if only supported params are passed
        Optional<String> optionalNotSupportedParam = params.keySet()
            .stream()
            .filter(paramName -> !SUPPORTED_PARAMS.contains(paramName))
            .findFirst();
        if (optionalNotSupportedParam.isPresent()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "provided parameter for combination technique is not supported. supported parameters are [%s]",
                    SUPPORTED_PARAMS.stream().collect(Collectors.joining(","))
                )
            );
        }

        // check param types
        if (params.keySet().stream().anyMatch(PARAM_NAME_WEIGHTS::equalsIgnoreCase)) {
            if (!(params.get(PARAM_NAME_WEIGHTS) instanceof List)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "parameter [%s] must be a collection of numbers", PARAM_NAME_WEIGHTS)
                );
            }
        }
    }

    /**
     * Get weight for sub-query based on its index in the hybrid search query. Use user provided weight or 1.0 otherwise
     * @param indexOfSubQuery 0-based index of sub-query in the Hybrid Search query
     * @return weight for sub-query, use one that is set in processor/pipeline definition or 1.0 as default
     */
    private float getWeightForSubQuery(int indexOfSubQuery) {
        return indexOfSubQuery < weights.size() ? weights.get(indexOfSubQuery) : 1.0f;
    }
}
