/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;

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
            throw new IllegalArgumentException("Chunker parameter [" + fieldName + "] cannot be cast to [" + String.class.getName() + "]");
        } else if (!allowEmpty && StringUtils.isEmpty(fieldValue.toString())) {
            throw new IllegalArgumentException("Chunker parameter: " + fieldName + " should not be empty.");
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
                "fixed length parameter [" + fieldName + "] cannot be cast to [" + Number.class.getName() + "]"
            );
        }
        if (NumberUtils.createInteger(fieldValue) <= 0) {
            throw new IllegalArgumentException("fixed length parameter [" + fieldName + "] must be positive");
        }
        return Integer.valueOf(fieldValue);
    }
}
