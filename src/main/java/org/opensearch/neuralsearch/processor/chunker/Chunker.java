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

    String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";
    String STRING_TOBE_CHUNKED_FIELD = "string_tobe_chunked_count";
    int DEFAULT_MAX_CHUNK_LIMIT = 100;
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
     * @param stringTobeChunkedCount runtime string_tobe_chunked_count, used to check with chunkResultSize
     */
    static boolean checkRunTimeMaxChunkLimit(int chunkResultSize, int runtimeMaxChunkLimit, int stringTobeChunkedCount) {
        return runtimeMaxChunkLimit != DISABLED_MAX_CHUNK_LIMIT && chunkResultSize + stringTobeChunkedCount >= runtimeMaxChunkLimit;
    }
}
