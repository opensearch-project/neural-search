/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;
import java.util.Locale;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseIntegerParameter;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseStringParameter;

/**
 * The implementation {@link Chunker} for delimiter algorithm
 */
public final class DelimiterChunker implements Chunker {

    public static final String ALGORITHM_NAME = "delimiter";

    public static final String DELIMITER_FIELD = "delimiter";

    public static final String DEFAULT_DELIMITER = "\n\n";

    private String delimiter;
    private int maxChunkLimit;

    public DelimiterChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
    }

    /**
     * Parse the parameters for delimiter algorithm.
     * Throw IllegalArgumentException if delimiter is not a string or an empty string.
     *
     * @param parameters a map with non-runtime parameters as the following:
     * 1. delimiter A string as the paragraph split indicator
     * 2. max_chunk_limit processor level max chunk level
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.delimiter = parseStringParameter(parameters, DELIMITER_FIELD, DEFAULT_DELIMITER);
        this.maxChunkLimit = parseIntegerParameter(parameters, MAX_CHUNK_LIMIT_FIELD, DEFAULT_MAX_CHUNK_LIMIT);
    }

    /**
     * Return the chunked passages for delimiter algorithm
     *
     * @param content input string
     * @param runtimeParameters a map for runtime parameters, containing the following runtime parameters:
     * 1. max_chunk_level content level max chunk limit
     */
    @Override
    public List<String> chunk(final String content, final Map<String, Object> runtimeParameters) {
        int runtimeMaxChunkLimit = parseIntegerParameter(runtimeParameters, MAX_CHUNK_LIMIT_FIELD, maxChunkLimit);

        List<String> chunkResult = new ArrayList<>();
        int start = 0, end;
        int nextDelimiterPosition = content.indexOf(delimiter);

        while (nextDelimiterPosition != -1) {
            checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit);
            end = nextDelimiterPosition + delimiter.length();
            chunkResult.add(content.substring(start, end));
            start = end;
            nextDelimiterPosition = content.indexOf(delimiter, start);
        }

        if (start < content.length()) {
            checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit);
            chunkResult.add(content.substring(start));
        }

        return chunkResult;
    }

    private void checkRunTimeMaxChunkLimit(int chunkResultLength, int runtimeMaxChunkLimit) {
        if (chunkResultLength == runtimeMaxChunkLimit) {
            // need processorMaxChunkLimit to keep exception message consistent
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "The number of chunks produced by %s processor has exceeded the allowed maximum of [%s]. This limit can be set by changing the [%s] parameter.",
                    TYPE,
                    maxChunkLimit,
                    MAX_CHUNK_LIMIT_FIELD
                )
            );
        }
    }
}
