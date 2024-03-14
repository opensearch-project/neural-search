/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;

/**
 * Parse the parameter for text chunking processor and algorithms.
 * The parameter must be validated before parsing.
 */
public class ChunkerParameterParser {

    /**
     * Parse string type parameter
     */
    public static String parseStringParameter(final Map<String, Object> parameters, final String fieldName, final String defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            return defaultValue;
        }
        return parameters.get(fieldName).toString();
    }

    /**
     * Parse integer type parameter
     */
    public static int parseIntegerParameter(final Map<String, Object> parameters, final String fieldName, final int defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all chunking algorithm parameters are optional
            return defaultValue;
        }
        String fieldValueString = parameters.get(fieldName).toString();
        return NumberUtils.createInteger(fieldValueString);
    }

    /**
     * parse double type parameter
     */
    public static double parseDoubleParameter(final Map<String, Object> parameters, final String fieldName, final double defaultValue) {
        if (!parameters.containsKey(fieldName)) {
            // all chunking algorithm parameters are optional
            return defaultValue;
        }
        String fieldValueString = parameters.get(fieldName).toString();
        return NumberUtils.createDouble(fieldValueString);
    }
}
