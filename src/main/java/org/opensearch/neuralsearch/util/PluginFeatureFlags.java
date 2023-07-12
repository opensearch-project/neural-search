/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;
import org.opensearch.transport.TransportSettings;

/**
 * Abstracts feature flags operations specific to neural-search plugin
 */
public class PluginFeatureFlags {

    /**
     * Used to test feature flags whose values are expected to be booleans.
     * This method returns true if the value is "true" (case-insensitive),
     * and false otherwise.
     * Checks alternative flag names as they may be different for plugins
     */
    public static boolean isEnabled(final String featureFlagName) {
        return FeatureFlags.isEnabled(featureFlagName) || FeatureFlags.isEnabled(transportFeatureName(featureFlagName));
    }

    /**
     * Get the feature name that is used for transport specific features. It's used by core for all features
     * defined at plugin level (https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/plugins/PluginsService.java#L277)
     */
    private static String transportFeatureName(final String name) {
        if (Strings.isNullOrEmpty(name)) {
            return name;
        }
        return String.join(".", TransportSettings.FEATURE_PREFIX, name);
    }
}
