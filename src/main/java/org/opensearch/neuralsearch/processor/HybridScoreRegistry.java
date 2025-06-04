/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry to store hybrid score for each search context
 */
public class HybridScoreRegistry {
    private static final Map<SearchContext, Map<Integer, float[]>> contextToHolder = new HashMap<>();

    /**
     * Store hybrid score for each search context
     * @param context
     * @param scoreMap
     */
    public static void store(SearchContext context, Map<Integer, float[]> scoreMap) {
        contextToHolder.put(context, scoreMap);
    }

    /**
     * Get hybrid score for each search context
     * @param context
     * @return map of hybrid score
     */
    public static Map<Integer, float[]> get(SearchContext context) {
        return contextToHolder.get(context);
    }

    /**
     * Remove hybrid score for each search context
     * @param context
     */
    public static void remove(SearchContext context) {
        contextToHolder.remove(context);
    }
}
