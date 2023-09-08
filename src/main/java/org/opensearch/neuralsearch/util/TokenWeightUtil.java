/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TokenWeightUtil {
    /**
     * Converts a Map result to Map<String, Float>
     *
     * @param mapResult {@link Map} of String and ?
     * @return Map of String and Float
     */
    public static String RESPONSE_KEY = "response";

    public static List<Map<String, Float>> fetchQueryTokensList(Map<String, ?> mapResult) {
        assert mapResult.get(RESPONSE_KEY) instanceof List;
        List<Map<String, ?>> responseList = (List) mapResult.get(RESPONSE_KEY);
        return responseList.stream().map(TokenWeightUtil::buildQueryTokensMap).collect(Collectors.toList());
    }

    private static Map<String, Float> buildQueryTokensMap(Map<String, ?> mapResult) {
        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<String, ?> entry: mapResult.entrySet()) {
            assert entry.getValue() instanceof Number;
            result.put(entry.getKey(), ((Number) entry.getValue()).floatValue());
        }
        return result;
    }
}
