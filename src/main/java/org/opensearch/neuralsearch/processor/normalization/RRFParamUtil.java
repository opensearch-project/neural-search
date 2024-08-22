/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import lombok.extern.log4j.Log4j2;

/**
 * Collection of utility methods for score combination technique classes
 */
@Log4j2
public class RRFParamUtil {
    private static final String RANK_CONSTANT = "rank_constant";
    private static final int DEFAULT_RANK_CONSTANT = 60;

    /**
     * get rank constant parameter for use in RRF processing
     * validates rank constant type and value, uses default
     * value of 60 if not entered by user
     */

    public int getRankConstant(final Map<String, Object> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return DEFAULT_RANK_CONSTANT;
        }
        // get rankConstant, we don't need to check for instance as it's done during validation
        // input capable of handling list but will only return first item as int or the default
        // int value of 60 if empty
        int rankConstant = (int) params.getOrDefault(RANK_CONSTANT, DEFAULT_RANK_CONSTANT);
        validateRankConstant(rankConstant);
        return rankConstant;
    }

    /**
     * Validate config parameters for this technique
     * @param actualParams map of parameters in form of name-value
     * @param supportedParams collection of parameters that we should validate against, typically that's what is supported by exact technique
     */
    public void validateRRFParams(final Map<String, Object> actualParams, final Set<String> supportedParams) {
        if (Objects.isNull(actualParams) || actualParams.isEmpty()) {
            return;
        } /*
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
          }*/
    }

    private void validateRankConstant(final int rankConstant) {
        boolean isOutOfRange = rankConstant < 1 || rankConstant >= Integer.MAX_VALUE;
        if (isOutOfRange) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "rank constant must be >= 1 and < (2^31)-1, submitted rank constant: %d", rankConstant)
            );
        }
    }

}
