/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DelimiterChunker implements FieldChunker {

    public DelimiterChunker() {}

    public static String DELIMITER_FIELD = "delimiter";

    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(DELIMITER_FIELD)) {
            Object delimiter = parameters.get(DELIMITER_FIELD);
            if (!(delimiter instanceof String)) {
                throw new IllegalArgumentException("delimiter parameters: " + delimiter + " must be string.");
            } else if (((String) delimiter).isEmpty()) {
                throw new IllegalArgumentException("delimiter parameters should not be empty.");
            }
        }
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> parameters) {
        String delimiter = (String) parameters.getOrDefault(DELIMITER_FIELD, ".");
        List<String> chunkResult = new ArrayList<>();
        int start = 0;
        int end = content.indexOf(delimiter);

        while (end != -1) {
            chunkResult.add(content.substring(start, end + delimiter.length()));
            start = end + delimiter.length();
            end = content.indexOf(delimiter, start);
        }

        if (start < content.length()) {
            chunkResult.add(content.substring(start));
        }
        return chunkResult;

    }
}
