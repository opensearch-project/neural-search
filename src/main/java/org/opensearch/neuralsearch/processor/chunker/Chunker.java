/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Map;
import java.util.List;

/**
 * The interface for all chunking algorithms.
 * All algorithms need to parse parameters and chunk the content.
 */
public interface Chunker {

    /** Field name for specifying the maximum chunk limit in the configuration. */
    String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";

    /** Field name for tracking the count of chunked strings. */
    String CHUNK_STRING_COUNT_FIELD = "chunk_string_count";

    /** Default maximum number of chunks allowed (100). */
    int DEFAULT_MAX_CHUNK_LIMIT = 100;

    /** Special value (-1) indicating that chunk limiting is disabled. */
    int DISABLED_MAX_CHUNK_LIMIT = -1;

    /**
     * Parse the parameters for chunking algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map containing non-runtime parameters for chunking algorithms
     */
    void parseParameters(Map<String, Object> parameters);

    /**
     * Chunk the input string according to parameters and return chunked passages
     *
     * @param content input string
     * @param runtimeParameters a map containing runtime parameters for chunking algorithms
     * @return chunked passages
     */
    List<String> chunk(String content, Map<String, Object> runtimeParameters);

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

    String getAlgorithmName();
}
