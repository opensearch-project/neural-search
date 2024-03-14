/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.Locale;

/**
 * Validate the parameter for text chunking processor and algorithms
 */
public class ChunkerParameterValidator {

    /**
     * Validate string type parameter
     */
    public static void validateStringParameter(final Map<String, Object> parameters, final String fieldName, final boolean allowEmpty) {
        if (!parameters.containsKey(fieldName)) {
            // all chunking algorithm parameters are optional
            return;
        }
        Object fieldValue = parameters.get(fieldName);
        if (!(fieldValue instanceof String)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] cannot be cast to [%s]", fieldName, String.class.getName())
            );
        }
        if (!allowEmpty && StringUtils.isEmpty(fieldValue.toString())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] should not be empty.", fieldName));
        }
    }

    /**
     * Validate integer type parameter with positive value
     */
    public static void validatePositiveIntegerParameter(
        final Map<String, Object> parameters,
        final String fieldName,
        final int defaultValue
    ) {
        if (!parameters.containsKey(fieldName)) {
            // all chunking algorithm parameters are optional
            return;
        }
        String fieldValueString = parameters.get(fieldName).toString();
        if (!(NumberUtils.isParsable(fieldValueString))) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] cannot be cast to [%s]", fieldName, Number.class.getName())
            );
        }
        int fieldValueInt = NumberUtils.createInteger(fieldValueString);
        // sometimes the parameter has negative default value, indicating that this parameter is not effective
        if (fieldValueInt != defaultValue && fieldValueInt <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] must be positive.", fieldName));
        }
    }

    /**
     * Validate double type parameter within range [lowerBound, upperBound]
     */
    public static void validateDoubleParameterWithinRange(
        final Map<String, Object> parameters,
        final String fieldName,
        final double lowerBound,
        final double upperBound
    ) {
        if (!parameters.containsKey(fieldName)) {
            // all chunking algorithm parameters are optional
            return;
        }
        String fieldValueString = parameters.get(fieldName).toString();
        if (!(NumberUtils.isParsable(fieldValueString))) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] cannot be cast to [%s]", fieldName, Number.class.getName())
            );
        }
        double fieldValueDouble = NumberUtils.createDouble(fieldValueString);
        if (fieldValueDouble < lowerBound || fieldValueDouble > upperBound) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s", fieldName, lowerBound, upperBound)
            );
        }
    }
}
