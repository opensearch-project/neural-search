/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class MetricStatNameTests extends OpenSearchTestCase {
    public static final EnumSet<MetricStatName> METRIC_STATS = EnumSet.allOf(MetricStatName.class);

    public void test_fromValid() {
        String validStatName = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getNameString();
        MetricStatName result = MetricStatName.from(validStatName);
        assertEquals(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE, result);
    }

    public void test_fromInvalid() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> { MetricStatName.from("non_existent_stat"); });
        assertTrue(exception.getMessage().contains("Metric stat not found"));
    }

    public void test_isValidName_returnsTrue() {
        assertTrue(MetricStatName.isValidName(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getNameString()));
    }

    public void test_isValidName_returnsFalse() {
        assertFalse(MetricStatName.isValidName("invalid_name"));
    }

    public void test_version() {
        Version version = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getVersion();
        assertEquals(Version.V_3_3_0, version);
    }

    public void test_uniquePaths() {
        Set<String> paths = new HashSet<>();

        // First pass to add all base paths (excluding stat names) to avoid colliding a stat name with a terminal path
        // e.g. if a.b is a stat, a.b.c cannot be a stat.
        for (MetricStatName statName : METRIC_STATS) {
            String path = statName.getPath().toLowerCase(Locale.ROOT);
            paths.add(path);
        }

        // Check possible path collisions
        // i.e. a full path is a terminal path that should not have any children
        for (MetricStatName statName : METRIC_STATS) {
            String path = statName.getFullPath().toLowerCase(Locale.ROOT);
            assertFalse(String.format(Locale.ROOT, "Checking full path uniqueness for %s", path), paths.contains(path));
            paths.add(path);
        }
    }
}
