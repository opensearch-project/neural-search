/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import reactor.util.annotation.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.DISABLED_MAX_CHUNK_LIMIT;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;

/**
 * Utility class for text chunking. It holds reusable chunking related helper functions.
 */
public class ChunkUtils {
    /**
     * Chunk a string and also update the runTimeParameters properly. At the end return the chunked results.
     * @param chunker The chunker to do the actual chunking work
     * @param content The string content to chunk
     * @param runTimeParameters a map containing runtime parameters for chunking algorithms
     * @return chunked passages
     */
    public static List<String> chunkString(
        @NonNull final Chunker chunker,
        final String content,
        final Map<String, Object> runTimeParameters
    ) {
        // return an empty list for empty string
        if (StringUtils.isEmpty(content)) {
            return List.of();
        }
        List<String> contentResult = chunker.chunk(content, runTimeParameters);
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
     * Chunk a list of strings and also update the runTimeParameters properly. At the end return the chunked results.
     * @param chunker The chunker to do the actual chunking work
     * @param contentList A list of strings to chunk
     * @param runTimeParameters a map containing runtime parameters for chunking algorithms
     * @return chunked passages
     */
    public static List<String> chunkList(
        @NonNull final Chunker chunker,
        final List<String> contentList,
        final Map<String, Object> runTimeParameters
    ) {
        // flatten original output format from List<List<String>> to List<String>
        List<String> result = new ArrayList<>();
        for (String content : contentList) {
            result.addAll(chunkString(chunker, content, runTimeParameters));
        }
        return result;
    }
}
