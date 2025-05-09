/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseStringWithDefault;

/**
 * The implementation {@link Chunker} for delimiter algorithm
 */
public final class DelimiterChunker implements Chunker {

    /** The identifier for the delimiter chunking algorithm. */
    public static final String ALGORITHM_NAME = "delimiter";

    /** The parameter field name for specifying the delimiter. */
    public static final String DELIMITER_FIELD = "delimiter";

    /** The default delimiter value used when none is specified. Uses two consecutive newline characters to split on paragraph boundaries. */
    public static final String DEFAULT_DELIMITER = "\n\n";

    /** The delimiter string used for text chunking. */
    private String delimiter;

    /**
     * Constructor that initializes the delimiter chunker with the specified parameters.
     * @param parameters a map with non-runtime parameters to be parsed
     */
    public DelimiterChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
    }

    /**
     * Parse the parameters for delimiter algorithm.
     * Throw IllegalArgumentException if delimiter is not a string or an empty string.
     *
     * @param parameters a map with non-runtime parameters as the following:
     * 1. delimiter A string as the paragraph split indicator
     * 2. max_chunk_limit processor level max chunk limit
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.delimiter = parseStringWithDefault(parameters, DELIMITER_FIELD, DEFAULT_DELIMITER);
    }

    /**
     * Return the chunked passages for delimiter algorithm
     *
     * @param content input string
     * @param runtimeParameters a map for runtime parameters, containing the following runtime parameters:
     * 1. max_chunk_limit field level max chunk limit
     * 2. chunk_string_count number of non-empty strings (including itself) which need to be chunked later
     */
    @Override
    public List<String> chunk(final String content, final Map<String, Object> runtimeParameters) {
        int runtimeMaxChunkLimit = parseInteger(runtimeParameters, MAX_CHUNK_LIMIT_FIELD);
        int chunkStringCount = parseInteger(runtimeParameters, CHUNK_STRING_COUNT_FIELD);

        List<String> chunkResult = new ArrayList<>();
        int start = 0, end;
        int nextDelimiterPosition = content.indexOf(delimiter);

        while (nextDelimiterPosition != -1) {
            if (Chunker.checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit, chunkStringCount)) {
                break;
            }
            end = nextDelimiterPosition + delimiter.length();
            chunkResult.add(content.substring(start, end));
            start = end;
            nextDelimiterPosition = content.indexOf(delimiter, start);
        }

        // add the rest content into the chunk result
        if (start < content.length()) {
            chunkResult.add(content.substring(start));
        }

        return chunkResult;
    }

    @Override
    public String getAlgorithmName() {
        return ALGORITHM_NAME;
    }
}
