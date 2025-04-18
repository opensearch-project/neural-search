/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import java.util.Map;

public class FeatureFlagUtil {
    public static final String SEMANTIC_FIELD_ENABLED = "semantic_field_enabled";
    private static final Map<String, Boolean> featureFlagMap = Map.of(SEMANTIC_FIELD_ENABLED, Boolean.FALSE);

    public static Boolean isEnabled(String name) {
        return featureFlagMap.getOrDefault(name, Boolean.FALSE);
    }
}
