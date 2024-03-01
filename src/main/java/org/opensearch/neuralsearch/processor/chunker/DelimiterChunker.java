/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DelimiterChunker implements IFieldChunker {

    public DelimiterChunker() {}

    public static String DELIMITER_FIELD = "delimiter";

    public static String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";

    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(DELIMITER_FIELD)) {
            Object delimiter = parameters.get(DELIMITER_FIELD);
            if (!(delimiter instanceof String)) {
                throw new IllegalArgumentException("delimiter parameters: " + delimiter + " must be string.");
            } else if (((String) delimiter).length() == 0) {
                throw new IllegalArgumentException("delimiter parameters should not be empty.");
            }
        } else {
            throw new IllegalArgumentException("You must contain field: " + DELIMITER_FIELD + " in your parameter.");
        }
        if (parameters.containsKey(MAX_CHUNK_LIMIT_FIELD)) {
            Object maxChunkLimit = parameters.get(MAX_CHUNK_LIMIT_FIELD);
            if (!(maxChunkLimit instanceof Integer)) {
                throw new IllegalArgumentException("Parameter max_chunk_limit:" + maxChunkLimit.toString() + " should be integer.");
            } else if ((int) maxChunkLimit <= 0) {
                throw new IllegalArgumentException("Parameter max_chunk_limit:" + maxChunkLimit + " is not greater than 0.");
            }
        }
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> parameters) {
        String delimiter = (String) parameters.get(DELIMITER_FIELD);
        int maxChunkingNumber = (int) parameters.getOrDefault(MAX_CHUNK_LIMIT_FIELD, -1);
        List<String> chunkResult = new ArrayList<>();
        int start = 0;
        int end = content.indexOf(delimiter);

        while (end != -1) {
            addChunkResult(chunkResult, maxChunkingNumber, content.substring(start, end + delimiter.length()));
            start = end + delimiter.length();
            end = content.indexOf(delimiter, start);

        }

        if (start < content.length()) {
            addChunkResult(chunkResult, maxChunkingNumber, content.substring(start));
        }
        return chunkResult;

    }

    private void addChunkResult(List<String> chunkResult, int maxChunkingNumber, String candidate) {
        if (chunkResult.size() >= maxChunkingNumber && maxChunkingNumber > 0) {
            throw new IllegalArgumentException("Exceed max chunk number: " + maxChunkingNumber);
        }
        chunkResult.add(candidate);
    }
}
