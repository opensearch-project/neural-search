/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.Locale;

import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;

/**
 * A util class used by chunking algorithms.
 */
public class ChunkerUtil {

    private ChunkerUtil() {} // no instance of this util class

    /**
     * Checks whether the chunking results would exceed the max chunk limit.
     * If exceeds, then Throw IllegalStateException
     *
     * @param chunkResultSize the size of chunking result
     * @param runtimeMaxChunkLimit runtime max_chunk_limit, used to check with chunkResultSize
     * @param nonRuntimeMaxChunkLimit non-runtime max_chunk_limit, used to keep exception message consistent
     */
    public static void checkRunTimeMaxChunkLimit(int chunkResultSize, int runtimeMaxChunkLimit, int nonRuntimeMaxChunkLimit) {
        if (chunkResultSize == runtimeMaxChunkLimit) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The number of chunks produced by %s processor has exceeded the allowed maximum of [%s]. This limit can be set by changing the [%s] parameter.",
                    TYPE,
                    nonRuntimeMaxChunkLimit,
                    MAX_CHUNK_LIMIT_FIELD
                )
            );
        }
    }
}
