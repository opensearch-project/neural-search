/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

/**
 * The implementation {@link Chunker} for delimiter algorithm
 */
public class DelimiterChunker implements Chunker {

    public DelimiterChunker(Map<String, Object> parameters) {
        validateParameters(parameters);
    }

    public static final String DELIMITER_FIELD = "delimiter";

    public static final String DEFAULT_DELIMITER = ".";

    private String delimiter = DEFAULT_DELIMITER;

    /**
     * Validate the chunked passages for delimiter algorithm
     *
     * @param parameters a map containing parameters, containing the following parameters
     * 1. A string as the paragraph split indicator
     * @throws IllegalArgumentException If delimiter is not a string or empty
     */
    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(DELIMITER_FIELD)) {
            if (!(parameters.get(DELIMITER_FIELD) instanceof String)) {
                throw new IllegalArgumentException(
                        "delimiter parameter [" + DELIMITER_FIELD + "] cannot be cast to [" + String.class.getName() + "]"
                );
            }
            this.delimiter = parameters.get(DELIMITER_FIELD).toString();
            if (StringUtils.isBlank(delimiter)) {
                throw new IllegalArgumentException("delimiter parameter [" + DELIMITER_FIELD + "] should not be empty.");
            }
        }
    }

    /**
     * Return the chunked passages for delimiter algorithm
     *
     * @param content input string
     */
    @Override
    public List<String> chunk(String content) {
        if (parameters.containsKey(DELIMITER_FIELD)) {
            Object delimiterObject = parameters.get(DELIMITER_FIELD);
            delimiter = delimiterObject.toString();
        }

        List<String> chunkResult = new ArrayList<>();
        int start = 0, end;
        int nextDelimiterPosition = content.indexOf(delimiter);

        while (nextDelimiterPosition != -1) {
            end = nextDelimiterPosition + delimiter.length();
            chunkResult.add(content.substring(start, end));
            start = end;
            nextDelimiterPosition = content.indexOf(delimiter, start);
        }

        if (start < content.length()) {
            chunkResult.add(content.substring(start));
        }

        return chunkResult;
    }
}
