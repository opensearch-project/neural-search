/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import lombok.Getter;

import java.util.Locale;
import java.util.Map;
import java.util.List;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseIntegerWithDefault;

/**
 * The interface for all chunking algorithms.
 * All algorithms need to parse parameters and chunk the content.
 */
public abstract class Chunker implements Validator, Parser {

    /** Field name for specifying the maximum chunk limit in the configuration. */
    public static String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";

    /** Field name for tracking the count of chunked strings. */
    public static String CHUNK_STRING_COUNT_FIELD = "chunk_string_count";

    /** Default maximum number of chunks allowed (100). */
    public static int DEFAULT_MAX_CHUNK_LIMIT = 100;

    /** Special value (-1) indicating that chunk limiting is disabled. */
    public static int DISABLED_MAX_CHUNK_LIMIT = -1;

    @Getter
    private int maxChunkLimit;

    /**
     * Parse the common parameters for the chunking algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map containing non-runtime parameters for chunking algorithms
     */
    @Override
    public void parse(Map<String, Object> parameters) {
        final int maxChunkLimit = parseIntegerWithDefault(parameters, MAX_CHUNK_LIMIT_FIELD, DEFAULT_MAX_CHUNK_LIMIT);
        validateMaxChunkLimit(maxChunkLimit);
        this.maxChunkLimit = maxChunkLimit;
    }

    /**
     * Chunk the input string according to parameters and return chunked passages
     *
     * @param content input string
     * @param runtimeParameters a map containing runtime parameters for chunking algorithms
     * @return chunked passages
     */
    public abstract List<String> chunk(String content, Map<String, Object> runtimeParameters);

    /**
     * Checks whether the chunking results would exceed the max chunk limit after adding a passage
     * If exceeds, then return true
     *
     * @param chunkResultSize the size of chunking result
     * @param runtimeMaxChunkLimit runtime max_chunk_limit, used to check with chunkResultSize
     * @param chunkStringCount runtime chunk_string_count, used to check with chunkResultSize
     * @return true if adding the new chunks would exceed the limit, false otherwise
     */
    static boolean checkRunTimeMaxChunkLimit(int chunkResultSize, int runtimeMaxChunkLimit, int chunkStringCount) {
        return runtimeMaxChunkLimit != DISABLED_MAX_CHUNK_LIMIT && chunkResultSize + chunkStringCount >= runtimeMaxChunkLimit;
    }

    public abstract String getAlgorithmName();

    /**
     * Validate the common parameters for a chunker
     * @param parameters parameters for a chunker
     */
    @Override
    public void validate(Map<String, Object> parameters) {
        final int maxChunkLimit = parseIntegerWithDefault(parameters, MAX_CHUNK_LIMIT_FIELD, DEFAULT_MAX_CHUNK_LIMIT);
        validateMaxChunkLimit(maxChunkLimit);
    }

    void validateMaxChunkLimit(int maxChunkLimit) {
        if (maxChunkLimit <= 0 && maxChunkLimit != DISABLED_MAX_CHUNK_LIMIT) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Parameter [%s] must be positive or %s to disable this parameter",
                    MAX_CHUNK_LIMIT_FIELD,
                    DISABLED_MAX_CHUNK_LIMIT
                )
            );
        }
    }
}
