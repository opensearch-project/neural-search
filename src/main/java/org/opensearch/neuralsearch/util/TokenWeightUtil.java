/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.neuralsearch.processor.pruning.PruneUtils;
import org.opensearch.neuralsearch.processor.pruning.PruningType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for working with sparse_encoding queries and ingest processor.
 * Used to fetch the (token, weight) Map from the response returned by {@link org.opensearch.neuralsearch.ml.MLCommonsClientAccessor}
 *
 */

public class TokenWeightUtil {
    public static String RESPONSE_KEY = "response";

    /**
     * possible input data format
     * case remote inference:
     * [{
     *     "response":{
     *         [
     *         { TOKEN_WEIGHT_MAP},
     *         { TOKEN_WEIGHT_MAP}
     *         ]
     *     }
     * }]
     * case local deploy:
     * [{"response":{
     *         [
     *         { TOKEN_WEIGHT_MAP}
     *         ]
     *     }
     * },{"response":{
     *         [
     *         { TOKEN_WEIGHT_MAP}
     *         ]
     *     }]
     *
     * @param mapResultList {@link Map} which is the response from {@link org.opensearch.neuralsearch.ml.MLCommonsClientAccessor}
     */
    public static List<Map<String, Float>> fetchListOfTokenWeightMap(
        List<Map<String, ?>> mapResultList,
        PruningType pruningType,
        float pruneRatio
    ) {
        if (null == mapResultList || mapResultList.isEmpty()) {
            throw new IllegalArgumentException("The inference result can not be null or empty.");
        }
        List<Object> results = new ArrayList<>();
        for (Map<String, ?> map : mapResultList) {
            if (!map.containsKey(RESPONSE_KEY)) {
                throw new IllegalArgumentException("The inference result should be associated with the field [" + RESPONSE_KEY + "].");
            }
            if (!List.class.isAssignableFrom(map.get(RESPONSE_KEY).getClass())) {
                throw new IllegalArgumentException("The data object associated with field [" + RESPONSE_KEY + "] should be a list.");
            }
            results.addAll((List<?>) map.get("response"));
        }
        return results.stream()
            .map(uncastedMap -> TokenWeightUtil.buildTokenWeightMap(uncastedMap, pruningType, pruneRatio))
            .collect(Collectors.toList());
    }

    public static List<Map<String, Float>> fetchListOfTokenWeightMap(List<Map<String, ?>> mapResultList) {
        return TokenWeightUtil.fetchListOfTokenWeightMap(mapResultList, PruningType.NONE, 0f);
    }

    private static Map<String, Float> buildTokenWeightMap(Object uncastedMap, PruningType pruningType, float pruneRatio) {
        if (!Map.class.isAssignableFrom(uncastedMap.getClass())) {
            throw new IllegalArgumentException("The expected inference result is a Map with String keys and Float values.");
        }
        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) uncastedMap).entrySet()) {
            if (!String.class.isAssignableFrom(entry.getKey().getClass()) || !Number.class.isAssignableFrom(entry.getValue().getClass())) {
                throw new IllegalArgumentException("The expected inference result is a Map with String keys and Float values.");
            }
            result.put((String) entry.getKey(), ((Number) entry.getValue()).floatValue());
        }
        return PruneUtils.pruningSparseVector(pruningType, pruneRatio, result);
    }
}
