/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.util.List;
import java.util.Map;

/**
 * Util class for routines associated with aggregations testing
 */
public class AggregationsTestUtils {

    public static List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    public static Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }

    public static Map<String, Object> getAggregations(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> aggsMap = (Map<String, Object>) searchResponseAsMap.get("aggregations");
        return aggsMap;
    }

    public static <T> T getAggregationValue(final Map<String, Object> aggsMap, final String aggName) {
        Map<String, Object> aggValues = (Map<String, Object>) aggsMap.get(aggName);
        return (T) aggValues.get("value");
    }

    public static <T> T getAggregationBuckets(final Map<String, Object> aggsMap, final String aggName) {
        Map<String, Object> aggValues = (Map<String, Object>) aggsMap.get(aggName);
        return (T) aggValues.get("buckets");
    }

    public static <T> T getAggregationValues(final Map<String, Object> aggsMap, final String aggName) {
        return (T) aggsMap.get(aggName);
    }
}
