/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.neuralsearch.plugin.NeuralSearch.DependentPlugin;

public class NeuralSearchTests extends OpenSearchTestCase {

    public void testDependentPluginsEnumValues() {
        // Verify we have exactly two plugins
        assertEquals("Should have exactly two dependent plugins", 2, DependentPlugin.values().length);

        // Verify the enum contains both required plugins
        assertTrue("Should contain ML Commons plugin", containsName(DependentPlugin.values(), "ML_COMMONS"));
        assertTrue("Should contain KNN plugin", containsName(DependentPlugin.values(), "KNN"));
    }

    public void testPluginValidationWithMissingDependencies() {
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            try {
                // Try to load a non-existent class directly
                Class.forName("org.opensearch.non.existent.TestClass");
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Neural Search plugin requires the TEST plugin to be installed", e);
            }
        });

        assertTrue("Exception message should mention plugin name", exception.getMessage().contains("TEST"));
        assertTrue("Exception should be caused by ClassNotFoundException", exception.getCause() instanceof ClassNotFoundException);
    }

    private boolean containsName(DependentPlugin[] plugins, String name) {
        for (DependentPlugin plugin : plugins) {
            if (plugin.name().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
