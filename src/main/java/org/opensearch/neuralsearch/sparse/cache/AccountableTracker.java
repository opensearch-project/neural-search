/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.util.Accountable;

/**
 * Abstract base class for tracking RAM usage of objects.
 * Provides functionality to record and report memory consumption.
 */
public abstract class AccountableTracker implements Accountable {
    private final RamBytesRecorder recorder = new RamBytesRecorder(0);

    /**
     * Records the number of bytes used by an object.
     *
     * @param bytes number of bytes to record
     */
    public void recordUsedBytes(long bytes) {
        recorder.record(bytes);
    }

    @Override
    public long ramBytesUsed() {
        return recorder.getBytes();
    }
}
