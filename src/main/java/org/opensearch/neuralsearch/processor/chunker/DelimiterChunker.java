/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseStringParameter;

/**
 * The implementation {@link Chunker} for delimiter algorithm
 */
public class DelimiterChunker implements Chunker {

    public static final String ALGORITHM_NAME = "delimiter";
    public static final String DELIMITER_FIELD = "delimiter";
    public static final String DEFAULT_DELIMITER = "\n\n";

    private String delimiter;

    public DelimiterChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
    }

    /**
     * Parse the parameters for delimiter algorithm.
     * Throw IllegalArgumentException if delimiter is not a string or empty.
     *
     * @param parameters a map containing parameters, containing the following parameters
     * 1. delimiter A string as the paragraph split indicator
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.delimiter = parseStringParameter(parameters, DELIMITER_FIELD, DEFAULT_DELIMITER);
    }

    /**
     * Return the chunked passages for fixed token length algorithm
     *
     * @param content input string
     * @param runtimeParameters a map for runtime parameters, but not needed by delimiter algorithm
     */
    @Override
    public List<String> chunk(final String content, final Map<String, Object> runtimeParameters) {
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
