/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class MemoryStatTests extends AbstractSparseTestBase {

    public void testGetValue_withUsagePercentage() {
        MemoryStat memoryStat = new MemoryStat(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE);
        Double value = memoryStat.getValue();
        assertNotNull(value);

        double expectedValue = 0.0d;
        assertEquals(expectedValue, value, DELTA_FOR_ASSERTION);
    }

    public void testGetValue_withUsage() {
        MemoryStatSnapshot forwardIndex = new MemoryStat(MetricStatName.MEMORY_SPARSE_FORWARD_INDEX_USAGE).getStatSnapshot();
        MemoryStatSnapshot clusteredPosting = new MemoryStat(MetricStatName.MEMORY_SPARSE_CLUSTERED_POSTING_USAGE).getStatSnapshot();
        MemoryStatSnapshot total = new MemoryStat(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE).getStatSnapshot();

        double forwardIndexSize = forwardIndex.getValue();
        double clusteredPostingSize = clusteredPosting.getValue();
        double totalSize = total.getValue();

        assertEquals(totalSize, forwardIndexSize + clusteredPostingSize, 0.02d);
    }

    public void testGetStatSnapshot_withUsagePercentage() {
        MemoryStat memoryStat = new MemoryStat(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE);
        MemoryStatSnapshot memoryStatSnapshot = memoryStat.getStatSnapshot();

        assertNotNull(memoryStatSnapshot);
        assertEquals(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE, memoryStatSnapshot.getStatName());
        assertFalse(memoryStatSnapshot.isAggregationMetric());
    }

    public void testGetStatSnapshot_withUsage() {
        MemoryStat memoryStat = new MemoryStat(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE);
        MemoryStatSnapshot memoryStatSnapshot = memoryStat.getStatSnapshot();

        assertNotNull(memoryStatSnapshot);
        assertEquals(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE, memoryStatSnapshot.getStatName());
        assertTrue(memoryStatSnapshot.isAggregationMetric());
    }
}
