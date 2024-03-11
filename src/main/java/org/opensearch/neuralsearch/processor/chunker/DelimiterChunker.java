/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterValidator.validateStringParameters;

/**
 * The implementation {@link Chunker} for delimiter algorithm
 */
public class DelimiterChunker implements Chunker {

    public DelimiterChunker(Map<String, Object> parameters) {
        validateParameters(parameters);
    }

    public static final String DELIMITER_FIELD = "delimiter";

    public static final String DEFAULT_DELIMITER = ".";

    private String delimiter;

    /**
     * Validate the chunked passages for delimiter algorithm
     *
     * @param parameters a map containing parameters, containing the following parameters
     * 1. A string as the paragraph split indicator
     * @throws IllegalArgumentException If delimiter is not a string or empty
     */
    @Override
    public void validateParameters(Map<String, Object> parameters) {
        this.delimiter = validateStringParameters(parameters, DELIMITER_FIELD, DEFAULT_DELIMITER, false);
    }

    @Override
    public List<String> chunk(String content) {
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
