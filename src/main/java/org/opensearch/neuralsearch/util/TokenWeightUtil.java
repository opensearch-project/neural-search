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
        if (response instanceof Map) {
            Map<String, Float> result = new HashMap<>();
            for (Map.Entry<String, ?> entry: ((Map<String, ?>) response).entrySet()) {
                assert entry.getValue() instanceof Number;
                result.put(entry.getKey(), ((Number) entry.getValue()).floatValue());
            }
            return result;
        } else {
            throw new IllegalArgumentException("wrong type");
        }
    }
}
