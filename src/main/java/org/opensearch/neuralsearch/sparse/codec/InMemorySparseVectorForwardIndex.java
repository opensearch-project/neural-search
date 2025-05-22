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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * InMemorySparseVectorForwardIndex is used to store/read sparse vector in memory
 */
@Log4j2
public class InMemorySparseVectorForwardIndex implements SparseVectorForwardIndex, Accountable {

    private static final Map<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> forwardIndexMap = new ConcurrentHashMap<>();

    public static long memUsage() {
        long mem = 0;
        for (Map.Entry<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> entry : forwardIndexMap.entrySet()) {
            mem += RamUsageEstimator.shallowSizeOf(InMemoryKey.IndexKey.class);
            mem += entry.getValue().ramBytesUsed();
        }
        return mem;
    }

    public static InMemorySparseVectorForwardIndex getOrCreate(InMemoryKey.IndexKey key, int docCount) {
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

    // private final Map<Integer, SparseVector> sparseVectorMap = new ConcurrentHashMap<>();
    private final SparseVector[] sparseVectors;

    public InMemorySparseVectorForwardIndex(int docCount) {
        sparseVectors = new SparseVector[docCount];
    }

    @Override
    public SparseVectorForwardIndexReader getForwardIndexReader() {
        return new InMemorySparseVectorForwardIndexReader();
    }

    @Override
    public SparseVectorForwardIndexWriter getForwardIndexWriter() {
        return new InMemorySparseVectorForwardIndexWriter();
    }

    @Override
    public long ramBytesUsed() {
        long ramUsed = 0;
        for (SparseVector vector : sparseVectors) {
            if (vector == null) continue;
            ramUsed += RamUsageEstimator.shallowSizeOfInstance(SparseVector.class);
        }
        return ramUsed;
    }

    private class InMemorySparseVectorForwardIndexReader implements SparseVectorForwardIndexReader {

        @Override
        public SparseVector readSparseVector(int docId) {
            assert docId < sparseVectors.length : "docId " + docId + " is out of bounds";
            return sparseVectors[docId];
        }

        @Override
        public BytesRef read(int docId) {
            throw new UnsupportedOperationException();
        }
    }

    private class InMemorySparseVectorForwardIndexWriter implements SparseVectorForwardIndexWriter {

        @Override
        public void write(int docId, SparseVector vector) {
            if (vector == null || docId >= sparseVectors.length) return;
            sparseVectors[docId] = vector;
        }

        @Override
        public void write(int docId, BytesRef doc) throws IOException {
            write(docId, new SparseVector(doc));
        }

        @Override
        public void close() {}
    }
}
