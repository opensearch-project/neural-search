/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Setter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/**
 * Thread-safe recorder for tracking RAM usage in bytes with optional increment validation.
 */
public class RamBytesRecorder {
    private final AtomicLong totalBytes;
    @Setter
    private BiPredicate<Long, Long> canRecordIncrementChecker;

    /**
     * Default constructor
     */
    public RamBytesRecorder() {
        this.totalBytes = new AtomicLong(0L);
        this.canRecordIncrementChecker = null;
    }

    /**
     * Creates recorder with increment validation.
     * @param canRecordIncrementChecker predicate to validate increments (increment, newTotal)
     */
    public RamBytesRecorder(BiPredicate<Long, Long> canRecordIncrementChecker) {
        this.totalBytes = new AtomicLong(0L);
        this.canRecordIncrementChecker = canRecordIncrementChecker;
    }

    /**
     * Creates recorder with initial byte count.
     * @param initialBytes starting byte count
     */
    public RamBytesRecorder(long initialBytes) {
        this.totalBytes = new AtomicLong(initialBytes);
        this.canRecordIncrementChecker = null;
    }

    /**
     * Records byte increment with validation if checker is set.
     * @param bytes byte count to add (can be negative)
     * @return true if recorded, false if validation failed
     */
    public synchronized boolean record(long bytes) {
        if (canRecordIncrementChecker != null && bytes > 0) {
            if (!canRecordIncrementChecker.test(bytes, totalBytes.get() + bytes)) {
                return false;
            }
        }
        totalBytes.addAndGet(bytes);
        return true;
    }

    /**
     * Records bytes without validation and executes post-action.
     * @param bytes byte count to add
     * @param postAction action to execute after recording
     */
    public synchronized void safeRecord(long bytes, Consumer<Long> postAction) {
        totalBytes.addAndGet(bytes);
        if (postAction != null) {
            postAction.accept(bytes);
        }
    }

    /**
     * Returns current total byte count.
     * @return total bytes recorded
     */
    public long getBytes() {
        return totalBytes.get();
    }
}
