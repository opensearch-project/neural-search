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
    //todo: change comments, add validation (throw exception)
    /**
     * Converts a Map result to Map<String, Float>
     *
     * @param mapResult {@link Map} of String and ?
     * @return Map of String and Float
     */
    public static String RESPONSE_KEY = "response";

    public static List<Map<String, Float>> fetchQueryTokensList(List<Map<String, ?>> mapResultList) {
        return mapResultList.stream().map(TokenWeightUtil::buildQueryTokensMap).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Float> buildQueryTokensMap(Map<String, ?> mapResult) {
        Object response = mapResult.get(RESPONSE_KEY);
        Map<String, Float> result = new HashMap<>();
        if (response instanceof Map) {
            for (Map.Entry<String, ?> entry: ((Map<String, ?>) response).entrySet()) {
                assert entry.getValue() instanceof Number;
                result.put(entry.getKey(), ((Number) entry.getValue()).floatValue());
            }
            return result;
        } else {
            assert response instanceof List;
            for (Map.Entry<String, ?> entry: ((Map<String, ?>) ((List<?>) response).get(0)).entrySet()) {
                assert entry.getValue() instanceof Number;
                result.put(entry.getKey(), ((Number) entry.getValue()).floatValue());
            }
            return result;
        }
    }
}
