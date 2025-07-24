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
     * Parses and validates a string parameter from the parameters map.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @return The validated string value from the parameters map
     * @throws IllegalArgumentException if the parameter is not a string or is empty
     */
    public static String parseString(final Map<String, Object> parameters, final String fieldName) throws IllegalArgumentException {
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
     * Parses and validates a string parameter from the parameters map with fallback to a default value.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @param defaultValue The default value to return if the parameter is not present
     * @return The validated string value from the parameters map if present, otherwise the default value
     * @throws IllegalArgumentException if the parameter is present but is not a string or is empty
     */
    public static String parseStringWithDefault(final Map<String, Object> parameters, final String fieldName, final String defaultValue)
        throws IllegalArgumentException {
        if (!parameters.containsKey(fieldName)) {
            // all string parameters are optional
            return defaultValue;
        }
        return parseString(parameters, fieldName);
    }

    /**
     * Parses and validates an integer value from the parameters map.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @return The parsed integer value from the parameters map
     * @throws IllegalArgumentException if the parameter is not an integer or is empty
     */
    public static int parseInteger(final Map<String, Object> parameters, final String fieldName) throws IllegalArgumentException {
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
     * Parses and validates an integer parameter from the parameters map with fallback to a default value.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @param defaultValue The default value to return if the parameter is not present
     * @return The integer value from the parameters map if present, otherwise the default value
     * @throws IllegalArgumentException if the parameter is present but cannot be converted to an integer
     */
    public static int parseIntegerWithDefault(final Map<String, Object> parameters, final String fieldName, final int defaultValue)
        throws IllegalArgumentException {
        if (!parameters.containsKey(fieldName)) {
            // return the default value when parameter is missing
            return defaultValue;
        }
        return parseInteger(parameters, fieldName);
    }

    /**
     * Parses and validates a positive integer parameter from the parameters map.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @return The parsed positive integer value
     * @throws IllegalArgumentException if the parameter is not a positive integer or cannot be converted to an integer
     */
    public static int parsePositiveInteger(final Map<String, Object> parameters, final String fieldName) throws IllegalArgumentException {
        int fieldValueInt = parseInteger(parameters, fieldName);
        if (fieldValueInt <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Parameter [%s] must be positive.", fieldName));
        }
        return fieldValueInt;
    }

    /**
     * Parses and validates a positive integer parameter from the parameters map with fallback to a default value.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @param defaultValue The default value to return if the parameter is not present
     * @return The positive integer value from the parameters map if present, otherwise the default value
     * @throws IllegalArgumentException if the parameter is present but is not a positive integer
     */
    public static int parsePositiveIntegerWithDefault(
        final Map<String, Object> parameters,
        final String fieldName,
        final Integer defaultValue
    ) throws IllegalArgumentException {
        if (!parameters.containsKey(fieldName)) {
            // all double parameters are optional
            return defaultValue;
        }
        return parsePositiveInteger(parameters, fieldName);
    }

    /**
     * Parses and validates a double value from the parameters map.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @return The parsed double value
     * @throws IllegalArgumentException if the parameter cannot be converted to a double
     */
    public static double parseDouble(final Map<String, Object> parameters, final String fieldName) throws IllegalArgumentException {
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
     * Parses and validates a double value from the parameters map with fallback to a default value.
     *
     * @param parameters The map containing chunking parameters
     * @param fieldName The name of the field to extract from the parameters map
     * @param defaultValue The default value to return if the parameter is not present
     * @return The double value from the parameters map if present, otherwise the default value
     * @throws IllegalArgumentException if the parameter is present but cannot be converted to a double
     */
    public static double parseDoubleWithDefault(final Map<String, Object> parameters, final String fieldName, final double defaultValue)
        throws IllegalArgumentException {
        if (!parameters.containsKey(fieldName)) {
            // all double parameters are optional
            return defaultValue;
        }
        return parseDouble(parameters, fieldName);
    }
}
