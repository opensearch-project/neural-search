/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportSettings;

public class PluginFeatureFlagsTests extends OpenSearchTestCase {

    public void testIsEnabled_whenNamePassed_thenSuccessful() {
        String settingName = "my.cool.setting";
        Settings settings = Settings.builder().put(settingName, true).build();
        FeatureFlags.initializeFeatureFlags(settings);
        assertTrue(PluginFeatureFlags.isEnabled(settingName));
    }

    public void testTransportFeaturePrefix_whenNamePassedWithPrefix_thenSuccessful() {
        String settingNameWithoutPrefix = "my.cool.setting";
        String settingNameWithPrefix = new StringBuilder().append(TransportSettings.FEATURE_PREFIX)
            .append('.')
            .append(settingNameWithoutPrefix)
            .toString();
        Settings settings = Settings.builder().put(settingNameWithPrefix, true).build();
        FeatureFlags.initializeFeatureFlags(settings);
        assertTrue(PluginFeatureFlags.isEnabled(settingNameWithoutPrefix));
    }

    public void testIsEnabled_whenNonExistentFeature_thenFail() {
        String settingName = "my.very_cool.setting";
        Settings settings = Settings.builder().put(settingName, true).build();
        FeatureFlags.initializeFeatureFlags(settings);
        assertFalse(PluginFeatureFlags.isEnabled("some_random_feature"));
    }

    public void testIsEnabled_whenFeatureIsNotBoolean_thenFail() {
        String settingName = "my.cool.setting";
        Settings settings = Settings.builder().put(settingName, 1234).build();
        FeatureFlags.initializeFeatureFlags(settings);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> PluginFeatureFlags.isEnabled(settingName));
        assertThat(
            exception.getMessage(),
            allOf(containsString("Failed to parse value"), containsString("only [true] or [false] are allowed"))
        );
    }
}
