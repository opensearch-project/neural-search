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
 * Validate and parse the parameter for chunking algorithms
 */
public class ChunkerParameterValidator {

    public static String validateStringParameters(
        Map<String, Object> parameters,
        String fieldName,
        String defaultValue,
        boolean allowEmpty
    ) {
        if (!parameters.containsKey(fieldName)) {
            // all parameters are optional
            return defaultValue;
        }
        Object fieldValue = parameters.get(fieldName);
        if (!(fieldValue instanceof String)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Chunking algorithm parameter [%s] cannot be cast to [%s]", fieldName, String.class.getName())
            );
        } else if (!allowEmpty && StringUtils.isEmpty(fieldValue.toString())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Chunking algorithm parameter [%s] should not be empty.", fieldName)
            );
        }
        return (String) fieldValue;
    }

    public static int validatePositiveIntegerParameter(Map<String, Object> parameters, String fieldName, int defaultValue) {
        // this method validate that parameter is a positive integer
        if (!parameters.containsKey(fieldName)) {
            // all parameters are optional
            return defaultValue;
        }
        String fieldValue = parameters.get(fieldName).toString();
        if (!(NumberUtils.isParsable(fieldValue))) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Chunking algorithm parameter [%s] cannot be cast to [%s]", fieldName, Number.class.getName())
            );
        }
        int fieldValueInt = NumberUtils.createInteger(fieldValue);
        if (fieldValueInt <= 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Chunking algorithm parameter [%s] must be positive.", fieldName)
            );
        }
        return fieldValueInt;
    }
}
