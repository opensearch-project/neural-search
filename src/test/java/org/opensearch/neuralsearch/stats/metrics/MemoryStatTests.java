/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class MemoryStatTests extends AbstractSparseTestBase {

    public void testGetValue_withUsagePercentage() {
        MemoryStat memoryStat = new MemoryStat(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE);
        String value = memoryStat.getValue();
        assertNotNull(value);
        assertTrue(value.endsWith("%"));
    }

    public void testGetValue_withUsage() {
        MemoryStatSnapshot forwardIndex = new MemoryStat(MetricStatName.MEMORY_SPARSE_FORWARD_INDEX_USAGE).getStatSnapshot();
        MemoryStatSnapshot clusteredPosting = new MemoryStat(MetricStatName.MEMORY_SPARSE_CLUSTERED_POSTING_USAGE).getStatSnapshot();
        MemoryStatSnapshot total = new MemoryStat(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE).getStatSnapshot();

        long forwardIndexSize = forwardIndex.getByteSize();
        long clusteredPostingSize = clusteredPosting.getByteSize();
        long totalSize = total.getByteSize();

        assertEquals(totalSize, forwardIndexSize + clusteredPostingSize);
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
