/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.DISABLED_MAX_CHUNK_LIMIT;

/**
 * A util class used by chunking algorithms.
 */
public class ChunkerUtil {

    private ChunkerUtil() {} // no instance of this util class

    /**
     * Checks whether the chunking results would exceed the max chunk limit.
     * If exceeds, then return true
     *
     * @param chunkResultSize the size of chunking result
     * @param runtimeMaxChunkLimit runtime max_chunk_limit, used to check with chunkResultSize
     * @param stringTobeChunkedCount runtime string_tobe_chunked_count, used to check with chunkResultSize
     */
    public static boolean checkRunTimeMaxChunkLimit(int chunkResultSize, int runtimeMaxChunkLimit, int stringTobeChunkedCount) {
        return runtimeMaxChunkLimit != DISABLED_MAX_CHUNK_LIMIT && chunkResultSize + stringTobeChunkedCount >= runtimeMaxChunkLimit;
    }
}
