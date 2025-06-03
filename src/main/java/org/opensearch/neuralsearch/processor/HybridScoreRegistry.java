/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.search.internal.SearchContext;

import java.util.HashMap;
import java.util.Map;

public class HybridScoreRegistry {
    private static final Map<SearchContext, Map<Integer, float[]>> contextToHolder = new HashMap<>();

    public static void store(SearchContext context, Map<Integer, float[]> scoreMap) {
        contextToHolder.put(context, scoreMap);
    }

    public static Map<Integer, float[]> get(SearchContext context) {
        return contextToHolder.get(context);
    }

    public static void remove(SearchContext context) {
        contextToHolder.remove(context);
    }
}
