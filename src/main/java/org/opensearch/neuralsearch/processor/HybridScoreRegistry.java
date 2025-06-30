/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.SearchPhaseContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry to store hybrid score for each search context
 */
public class HybridScoreRegistry {
    private static final Map<SearchPhaseContext, Map<String, float[]>> contextToHolder = new HashMap<>();

    /**
     * Store hybrid score for each search context
     * @param context
     * @param scoreMap
     */
    public static void store(SearchPhaseContext context, Map<String, float[]> scoreMap) {
        contextToHolder.put(context, scoreMap);
    }

    /**
     * Get hybrid score for each search context
     * @param context
     * @return map of hybrid score
     */
    public static Map<String, float[]> get(SearchPhaseContext context) {
        return contextToHolder.get(context);
    }

    /**
     * Remove hybrid score for each search context
     * @param context
     */
    public static void remove(SearchPhaseContext context) {
        contextToHolder.remove(context);
    }
}
