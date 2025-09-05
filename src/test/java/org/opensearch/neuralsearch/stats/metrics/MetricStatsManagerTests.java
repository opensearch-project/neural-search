/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class MetricStatsManagerTests extends AbstractSparseTestBase {

    public void testGetStats_withAllStats() {
        MetricStatsManager metricStatsManager = MetricStatsManager.instance();
        Map<MetricStatName, MemoryStatSnapshot> stats = metricStatsManager.getStats(EnumSet.allOf(MetricStatName.class));
        Set<MetricStatName> allStatNames = EnumSet.allOf(MetricStatName.class);

        assertEquals(allStatNames, stats.keySet());
    }

    public void testGetStats_withFilteredStats() {
        MetricStatsManager metricStatsManager = MetricStatsManager.instance();
        Map<MetricStatName, MemoryStatSnapshot> stats = metricStatsManager.getStats(EnumSet.of(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE));

        assertEquals(1, stats.size());
        assertTrue(stats.containsKey(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE));
    }
}
