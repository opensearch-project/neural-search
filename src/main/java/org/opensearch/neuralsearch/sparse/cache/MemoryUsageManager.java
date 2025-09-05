/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.function.BiPredicate;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;

/**
 * Singleton class for managing memory usage in sparse vector operations.
 */
@Setter
@Getter
public class MemoryUsageManager {
    private static volatile MemoryUsageManager instance;
    private RamBytesRecorder memoryUsageTracker;

    protected MemoryUsageManager() {
        memoryUsageTracker = new RamBytesRecorder();
    }

    /**
     * Returns the singleton instance of MemoryUsageManager.
     *
     * @return the singleton instance
     */
    public static MemoryUsageManager getInstance() {
        if (instance == null) {
            synchronized (MemoryUsageManager.class) {
                if (instance == null) {
                    instance = new MemoryUsageManager();
                }
            }
        }
        return instance;
    }

    /**
     * Sets the memory limit for sparse vector operations.
     *
     * @param limit the maximum memory limit
     * @param overhead the overhead for the circuit breaker
     */
    public void setLimitAndOverhead(ByteSizeValue limit, double overhead) {
        memoryUsageTracker.setCanRecordIncrementChecker(getInternalAndCbPredicate(limit, overhead));
    }

    @VisibleForTesting
    BiPredicate<Long, Long> getInternalAndCbPredicate(ByteSizeValue limit, double overhead) {
        return (bytes, targetedTotalBytes) -> {
            // check if memory limit exceeds, then check if circuit breaker is triggered
            return (long) (targetedTotalBytes * overhead) <= limit.getBytes() && CircuitBreakerManager.addMemoryUsage(bytes, SEISMIC);
        };
    }
}
