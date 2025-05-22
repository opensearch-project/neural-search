/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.List;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;

/**
 * The interface for all chunking algorithms.
 * All algorithms need to parse parameters and chunk the content.
 */
public abstract class Chunker {

    /** Field name for specifying the maximum chunk limit in the configuration. */
    public static String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";

    /** Field name for tracking the count of chunked strings. */
    public static String CHUNK_STRING_COUNT_FIELD = "chunk_string_count";

    /** Default maximum number of chunks allowed (100). */
    public static int DEFAULT_MAX_CHUNK_LIMIT = 100;

    /** Special value (-1) indicating that chunk limiting is disabled. */
    public static int DISABLED_MAX_CHUNK_LIMIT = -1;

    /**
     * Parse the parameters for chunking algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map containing non-runtime parameters for chunking algorithms
     */
    abstract void parseParameters(Map<String, Object> parameters);

    /**
     * Chunk the input string according to parameters and return chunked passages
     *
     * @param content input string
     * @param runtimeParameters a map containing runtime parameters for chunking algorithms
     * @return chunked passages
     */
    abstract List<String> chunk(String content, Map<String, Object> runtimeParameters);

    /**
     * Chunk a string and also update the runTimeParameters properly. At the end return the chunked results.
     * @param content The string content to chunk
     * @param runTimeParameters a map containing runtime parameters for chunking algorithms
     * @return chunked passages
     */
    public List<String> chunkString(final String content, final Map<String, Object> runTimeParameters) {
        // return an empty list for empty string
        if (StringUtils.isEmpty(content)) {
            return List.of();
        }
        List<String> contentResult = this.chunk(content, runTimeParameters);
        // update chunk_string_count for each string
        int chunkStringCount = parseInteger(runTimeParameters, CHUNK_STRING_COUNT_FIELD);
        runTimeParameters.put(CHUNK_STRING_COUNT_FIELD, chunkStringCount - 1);
        // update runtime max_chunk_limit if not disabled
        int runtimeMaxChunkLimit = parseInteger(runTimeParameters, MAX_CHUNK_LIMIT_FIELD);
        if (runtimeMaxChunkLimit != DISABLED_MAX_CHUNK_LIMIT) {
            runTimeParameters.put(MAX_CHUNK_LIMIT_FIELD, runtimeMaxChunkLimit - contentResult.size());
        }
        return contentResult;
    }

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
}
