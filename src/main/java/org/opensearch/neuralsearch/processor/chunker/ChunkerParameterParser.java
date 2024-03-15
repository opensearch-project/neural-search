/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Locale;
import java.util.Map;

/**
 * Parse the parameter for text chunking processor and algorithms.
 * Throw IllegalArgumentException when parameters are invalid.
 */
public class ChunkerParameterParser {

    /**
     * Parse string type parameter.
     * Throw IllegalArgumentException if parameter is not a string or empty.
     */
    public static String parseStringParameter(final Map<String, Object> parameters, final String fieldName, final String defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all string parameters are optional
            return defaultValue;
        }
        Object fieldValue = parameters.get(fieldName);
        if (!(fieldValue instanceof String)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, String.class.getName())
            );
        }
        if (StringUtils.isEmpty(fieldValue.toString())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] should not be empty.", fieldName));
        }
        return fieldValue.toString();
    }

    /**
     * Parse Integer type parameter.
     * Throw IllegalArgumentException if parameter is not an integer.
     */
    public static int parseIntegerParameter(final Map<String, Object> parameters, final String fieldName, final int defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all integer parameters are optional
            return defaultValue;
        }
        String fieldValueString = parameters.get(fieldName).toString();
        try {
            return NumberUtils.createInteger(fieldValueString);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName())
            );
        }
    }

    /**
     * Parse Integer type parameter with positive value.
     * Throw IllegalArgumentException if parameter is not a positive integer.
     */
    public static int parsePositiveIntegerParameter(final Map<String, Object> parameters, final String fieldName, final int defaultValue) {
        int fieldValueInt = parseIntegerParameter(parameters, fieldName, defaultValue);
        if (fieldValueInt <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] must be positive.", fieldName));
        }
        return fieldValueInt;
    }

    /**
     * Parse double type parameter.
     * Throw IllegalArgumentException if parameter is not a double.
     */
    public static double parseDoubleParameter(final Map<String, Object> parameters, final String fieldName, final double defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all double parameters are optional
            return defaultValue;
        }
        String fieldValueString = parameters.get(fieldName).toString();
        try {
            return NumberUtils.createDouble(fieldValueString);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Double.class.getName())
            );
        }
    }
}
