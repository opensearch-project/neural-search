/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InMemorySparseVectorForwardIndex is used to store/read sparse vector in memory
 */
@Log4j2
public class InMemorySparseVectorForwardIndex implements SparseVectorForwardIndex, Accountable {

    private static final Map<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> forwardIndexMap = new ConcurrentHashMap<>();

    public static long memUsage() {
        long mem = RamUsageEstimator.shallowSizeOf(forwardIndexMap);
        for (Map.Entry<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> entry : forwardIndexMap.entrySet()) {
            mem += RamUsageEstimator.shallowSizeOf(entry.getKey());
            mem += entry.getValue().ramBytesUsed();
        }
        return mem;
    }

    public static synchronized InMemorySparseVectorForwardIndex getOrCreate(InMemoryKey.IndexKey key, int docCount) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        return forwardIndexMap.computeIfAbsent(key, k -> new InMemorySparseVectorForwardIndex(docCount));
    }

    public static InMemorySparseVectorForwardIndex get(InMemoryKey.IndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        return forwardIndexMap.get(key);
    }

    public static void removeIndex(InMemoryKey.IndexKey key) {
        forwardIndexMap.remove(key);
    }

    private final SparseVector[] sparseVectors;
    private final AtomicLong usedRamBytes = new AtomicLong(0);
    private final SparseVectorReader reader = new InMemorySparseVectorReader();
    private final SparseVectorWriter writer = new InMemorySparseVectorWriter();

    public InMemorySparseVectorForwardIndex(int docCount) {
        sparseVectors = new SparseVector[docCount];
        // Account for the array itself in memory usage
        usedRamBytes.set(RamUsageEstimator.shallowSizeOf(sparseVectors));
    }

    @Override
    public SparseVectorReader getReader() {
        return reader;
    }

    @Override
    public SparseVectorWriter getWriter() {
        return writer;
    }

    @Override
    public long ramBytesUsed() {
        return usedRamBytes.get();
    }

    private class InMemorySparseVectorReader implements SparseVectorReader {
        @Override
        public SparseVector read(int docId) throws IOException {
            assert docId < sparseVectors.length : "docId " + docId + " is out of bounds";
            return sparseVectors[docId];
        }
    }

    private class InMemorySparseVectorWriter implements SparseVectorWriter {

        @Override
        public synchronized void write(int docId, SparseVector vector) {
            if (vector == null || docId >= sparseVectors.length) {
                return;
            }

            // Calculate memory impact of this operation
            SparseVector oldVector = sparseVectors[docId];
            long oldVectorSize = (oldVector != null) ? oldVector.ramBytesUsed() : 0;
            long newVectorSize = vector.ramBytesUsed();

            // Update the vector
            sparseVectors[docId] = vector;

            // Update memory usage tracking (subtract old size, add new size)
            usedRamBytes.addAndGet(newVectorSize - oldVectorSize);
        }

        @Override
        public void write(int docId, BytesRef doc) throws IOException {
            write(docId, new SparseVector(doc));
        }
    }
}
