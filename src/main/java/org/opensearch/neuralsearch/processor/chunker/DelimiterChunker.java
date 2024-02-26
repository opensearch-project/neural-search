/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class DelimiterChunker implements IFieldChunker {

    public DelimiterChunker() {}

    public static String DELIMITER_FIELD = "delimiter";


    @Override
    public void validateParameters(Map<String, Object> parameters) {
        if (parameters.containsKey(DELIMITER_FIELD))
        {
            Object delimiter = parameters.get(DELIMITER_FIELD);
            if (!(delimiter instanceof String)){
                throw new IllegalArgumentException("delimiter parameters " + delimiter + " must be string");
            }
        }
        else {
            throw new IllegalArgumentException("You must contain field:" + DELIMITER_FIELD + " in your parameter");
        }
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> parameters) {
        List<String > chunkingResult = new ArrayList<>();
        String delimiter = (String) parameters.get(DELIMITER_FIELD);
        Scanner scanner = new Scanner(content);
        scanner.useDelimiter(delimiter);
        while (scanner.hasNext()) {
            String nextChunk = scanner.next();
            chunkingResult.add(nextChunk);
        }
        return chunkingResult;
    }
}
