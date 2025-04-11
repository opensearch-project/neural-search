/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory stat information class which gets memory stat data
 */
public class MemoryStat implements MetricStat {

    private final MetricStatName statName;
    private final AtomicLong byteSize = new AtomicLong(0);

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
    public synchronized String getValue() {
        this.byteSize.set(getByteSize());
        if (statName == MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE) {
            MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
            long heapMaxBytes = memoryMXBean.getHeapMemoryUsage().getMax();
            return String.format(Locale.ROOT, "%.2f%%", (double) this.byteSize.get() / heapMaxBytes * 100);
        }
        return new ByteSizeValue(this.byteSize.get()).toString();
    }

    @Override
    public MemoryStatSnapshot getStatSnapshot() {
        // For sparse memory usage percentage, there is no use to perform aggregation metric memory usage percentage is a node-specific
        // metric
        return MemoryStatSnapshot.builder()
            .statName(statName)
            .value(getValue())
            .byteSize(this.byteSize.get())
            .isAggregationMetric(statName != MetricStatName.MEMORY_SPARSE_MEMORY_USAGE_PERCENTAGE)
            .build();
    }
}
