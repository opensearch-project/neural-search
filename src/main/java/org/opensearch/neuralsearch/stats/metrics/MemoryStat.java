/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.codec.InMemoryClusteredPosting;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;

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
            case MetricStatName.MEMORY_SPARSE_TOTAL_USAGE:
                return InMemorySparseVectorForwardIndex.memUsage() + InMemoryClusteredPosting.memUsage();
            case MetricStatName.MEMORY_SPARSE_FORWARD_INDEX_USAGE:
                return InMemorySparseVectorForwardIndex.memUsage();
            case MetricStatName.MEMORY_SPARSE_CLUSTERED_POSTING_USAGE:
                return InMemoryClusteredPosting.memUsage();
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Metric stat not found: %s", statName));
        }
    }

    @Override
    public synchronized String getValue() {
        this.byteSize.set(getByteSize());
        return RamUsageEstimator.humanReadableUnits(this.byteSize.get());
    }

    @Override
    public MemoryStatSnapshot getStatSnapshot() {
        return MemoryStatSnapshot.builder().statName(statName).value(getValue()).byteSize(this.byteSize.get()).build();
    }
}
