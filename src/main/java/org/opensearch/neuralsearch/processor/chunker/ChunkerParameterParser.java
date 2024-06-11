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
public final class ChunkerParameterParser {

    private ChunkerParameterParser() {} // no instance of this util class

    /**
     * Parse String type parameter.
     * Throw IllegalArgumentException if parameter is not a string or an empty string.
     */
    public static String parseString(final Map<String, Object> parameters, final String fieldName) {
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
     * Parse String type parameter.
     * Return default value if the parameter is missing.
     * Throw IllegalArgumentException if parameter is not a string or an empty string.
     */
    public static String parseStringWithDefault(final Map<String, Object> parameters, final String fieldName, final String defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all string parameters are optional
            return defaultValue;
        }
        return parseString(parameters, fieldName);
    }

    /**
     * Parse integer type parameter with default value.
     * Throw IllegalArgumentException if the parameter is not an integer.
     */
    public static int parseInteger(final Map<String, Object> parameters, final String fieldName) {
        String fieldValueString = parameters.get(fieldName).toString();
        try {
            return NumberUtils.createInteger(fieldValueString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Integer.class.getName())
            );
        }
    }

    /**
     * Parse integer type parameter with default value.
     * Return default value if the parameter is missing.
     * Throw IllegalArgumentException if the parameter is not an integer.
     */
    public static int parseIntegerWithDefault(final Map<String, Object> parameters, final String fieldName, final int defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // return the default value when parameter is missing
            return defaultValue;
        }
        return parseInteger(parameters, fieldName);
    }

    /**
     * Parse integer type parameter with positive value.
     * Return default value if the parameter is missing.
     * Throw IllegalArgumentException if the parameter is not a positive integer.
     */
    public static int parsePositiveInteger(final Map<String, Object> parameters, final String fieldName) {
        int fieldValueInt = parseInteger(parameters, fieldName);
        if (fieldValueInt <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] must be positive.", fieldName));
        }
        return fieldValueInt;
    }

    /**
     * Parse integer type parameter with positive value.
     * Return default value if the parameter is missing.
     * Throw IllegalArgumentException if the parameter is not a positive integer.
     */
    public static int parsePositiveIntegerWithDefault(
        final Map<String, Object> parameters,
        final String fieldName,
        final Integer defaultValue
    ) {
        if (!parameters.containsKey(fieldName)) {
            // all double parameters are optional
            return defaultValue;
        }
        return parsePositiveInteger(parameters, fieldName);
    }

    /**
     * Parse double type parameter.
     * Throw IllegalArgumentException if parameter is not a double.
     */
    public static double parseDouble(final Map<String, Object> parameters, final String fieldName) {
        String fieldValueString = parameters.get(fieldName).toString();
        try {
            return NumberUtils.createDouble(fieldValueString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parameter [%s] must be of %s type", fieldName, Double.class.getName())
            );
        }
    }

    /**
     * Parse double type parameter.
     * Return default value if the parameter is missing.
     * Throw IllegalArgumentException if parameter is not a double.
     */
    public static double parseDoubleWithDefault(final Map<String, Object> parameters, final String fieldName, final double defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all double parameters are optional
            return defaultValue;
        }
        return parseDouble(parameters, fieldName);
    }
}
