/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseIntegerWithDefault;
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

    /** The parameter field name for specifying the chunk size. */
    public static final String CHUNK_SIZE_FIELD = "chunk_size";

    /** The default chunk size value. */
    public static final int DEFAULT_CHUNK_SIZE = Integer.MAX_VALUE;

    /** The delimiter string used for text chunking. */
    private String delimiter;

    /** The minimum chunk size. If the total text length is smaller than this, it will not be split. */
    private int chunkSize;

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
     * 1. delimiter A string used as the split indicator (e.g., paragraph separator).
     * 2. chunk_size An integer representing the maximum allowed length for each chunk;
     *    if a segment split by delimiter exceeds this length, it will be further split.
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.delimiter = parseStringWithDefault(parameters, DELIMITER_FIELD, DEFAULT_DELIMITER);
        this.chunkSize = parseIntegerWithDefault(parameters, CHUNK_SIZE_FIELD, DEFAULT_CHUNK_SIZE);
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

        // Skip splitting to save memory if content is short and has no delimiter.
        if (content.length() <= this.chunkSize && !content.contains(delimiter)) {
            return Collections.singletonList(content);
        }

        List<String> chunkResult = new ArrayList<>();
        int start = 0;

        while (start < content.length()) {
            if (Chunker.checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit, chunkStringCount)) {
                chunkResult.add(content.substring(start));
                break;
            }

            int nextDelimiterPosition = content.indexOf(delimiter, start);
            int end = (nextDelimiterPosition == -1) ? content.length() : nextDelimiterPosition + delimiter.length();

            String chunk = content.substring(start, end);

            // For here 'chunk' represents a portion of content sliced by the delimiter.
            // If chunk is too large, split it further by chunkSize
            if (chunk.length() > this.chunkSize) {
                List<String> splitChunks = splitByMaxLengthIncludingDelimiter(chunk, this.chunkSize);
                int segmentOffsetWithinContent = 0;

                for (String splitChunk : splitChunks) {
                    // If adding this chunk exceeds the max chunk limit for chunkResult,
                    // merge all remaining parts into the last chunk
                    if (Chunker.checkRunTimeMaxChunkLimit(chunkResult.size(), runtimeMaxChunkLimit, chunkStringCount)) {
                        chunkResult.add(content.substring(start + segmentOffsetWithinContent));
                        return chunkResult;
                    }

                    chunkResult.add(splitChunk);
                    segmentOffsetWithinContent += splitChunk.length();
                }
            } else {
                chunkResult.add(chunk);
            }

            start = end;
        }

        return chunkResult;
    }

    private List<String> splitByMaxLengthIncludingDelimiter(final String text, final int maxLength) {
        List<String> result = new ArrayList<>();
        int start = 0;

        while (start < text.length()) {
            int end = Math.min(start + maxLength, text.length());

            // If a delimiter exists within the chunk and doesn't end at the boundary,
            // adjust to include the full delimiter
            if (!text.substring(start, end).endsWith(delimiter) && text.indexOf(delimiter, start) < end) {
                end = text.indexOf(delimiter, start) + delimiter.length();
            }

            result.add(text.substring(start, end));
            start = end;
        }

        return result;
    }
}
