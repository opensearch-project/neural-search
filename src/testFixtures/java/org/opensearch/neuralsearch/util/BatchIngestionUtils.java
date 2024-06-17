/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A helper class to build docs for bulk request which is used by batch ingestion tests.
 */
public class BatchIngestionUtils {
    private static final List<String> TEXTS = Arrays.asList(
        "hello",
        "world",
        "an apple",
        "find me",
        "birdy",
        "flying piggy",
        "newspaper",
        "dynamic programming",
        "random text",
        "finally"
    );

    public static List<Map<String, String>> prepareDataForBulkIngestion(int startId, int count) {
        List<Map<String, String>> docs = new ArrayList<>();
        for (int i = startId; i < startId + count; ++i) {
            Map<String, String> params = new HashMap<>();
            params.put("id", Integer.toString(i));
            params.put("text", TEXTS.get(i % TEXTS.size()));
            docs.add(params);
        }
        return docs;
    }
}
