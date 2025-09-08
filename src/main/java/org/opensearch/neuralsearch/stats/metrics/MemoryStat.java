/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Locale;

import static org.opensearch.knn.common.KNNConstants.BYTES_PER_KILOBYTES;

/**
 * Memory stat information class which gets memory stat data
 */
public class MemoryStat implements MetricStat {

    public static final int BYTES_PER_KILOBYTES = 1024;
    private final MetricStatName statName;

    /**
     * Constructor
     * @param statName the associate stat name identifier
     */
    public MemoryStat(MetricStatName statName) {
        this.statName = statName;
    }

    /**
     * @return the current memory stat value
     */
    private long getByteSize() {
        switch (statName) {
            case MetricStatName.MEMORY_SPARSE_MEMORY_USAGE:
            case MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE:
                return ForwardIndexCache.getInstance().ramBytesUsed() + ClusteredPostingCache.getInstance().ramBytesUsed();
            case MetricStatName.MEMORY_SPARSE_FORWARD_INDEX_USAGE:
                return ForwardIndexCache.getInstance().ramBytesUsed();
            case MetricStatName.MEMORY_SPARSE_CLUSTERED_POSTING_USAGE:
                return ClusteredPostingCache.getInstance().ramBytesUsed();
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Metric stat not found: %s", statName));
        }
    }

    @Override
    public Double getValue() {
        long byteSize = getByteSize();
        if (statName == MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE) {
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            // memoryMXBean.getHeapMemoryUsage().getMax() might return -1 in special case
            long heapMaxBytes = Math.max(0, memoryMXBean.getHeapMemoryUsage().getMax());
            double percentage = (double) byteSize / heapMaxBytes * 100;
            return Math.round(percentage * 100.0) / 100.0;
        }
        double kbSize = (double) byteSize / BYTES_PER_KILOBYTES;
        return Math.round(kbSize * 100.0) / 100.0;
    }

    @Override
    public MemoryStatSnapshot getStatSnapshot() {
        // For sparse memory usage percentage, there is no use to perform
        // aggregation metric memory usage percentage is a node-specific metric
        return MemoryStatSnapshot.builder()
            .statName(statName)
            .value(getValue())
            .isAggregationMetric(statName != MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE)
            .build();
    }
}
