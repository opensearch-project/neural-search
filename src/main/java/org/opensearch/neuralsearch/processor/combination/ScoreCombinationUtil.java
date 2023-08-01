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
 * Collection of utility methods for score combination technique classes
 */
public class ScoreCombinationUtil {
    private static final String PARAM_NAME_WEIGHTS = "weights";

    /**
     * Get collection of weights based on user provided config
     * @param params map of named parameters and their values
     * @return collection of weights
     */
    protected List<Float> getWeights(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return List.of();
        }
        // get weights, we don't need to check for instance as it's done during validation
        return ((List<Double>) params.getOrDefault(PARAM_NAME_WEIGHTS, List.of())).stream()
            .map(Double::floatValue)
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Validate config parameters for this technique
     * @param actualParams map of parameters in form of name-value
     * @param supportedParams collection of parameters that we should validate against, typically that's what is supported by exact technique
     */
    protected void validateParams(final Map<String, Object> actualParams, final Set<String> supportedParams) {
        if (Objects.isNull(actualParams) || actualParams.isEmpty()) {
            return;
        }
        // check if only supported params are passed
        Optional<String> optionalNotSupportedParam = actualParams.keySet()
            .stream()
            .filter(paramName -> !supportedParams.contains(paramName))
            .findFirst();
        if (optionalNotSupportedParam.isPresent()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "provided parameter for combination technique is not supported. supported parameters are [%s]",
                    supportedParams.stream().collect(Collectors.joining(","))
                )
            );
        }

        // check param types
        if (actualParams.keySet().stream().anyMatch(PARAM_NAME_WEIGHTS::equalsIgnoreCase)) {
            if (!(actualParams.get(PARAM_NAME_WEIGHTS) instanceof List)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "parameter [%s] must be a collection of numbers", PARAM_NAME_WEIGHTS)
                );
            }
        }
    }

    /**
     * Get weight for sub-query based on its index in the hybrid search query. Use user provided weight or 1.0 otherwise
     * @param weights collection of weights for sub-queries
     * @param indexOfSubQuery 0-based index of sub-query in the Hybrid Search query
     * @return weight for sub-query, use one that is set in processor/pipeline definition or 1.0 as default
     */
    protected float getWeightForSubQuery(final List<Float> weights, final int indexOfSubQuery) {
        return indexOfSubQuery < weights.size() ? weights.get(indexOfSubQuery) : 1.0f;
    }
}
