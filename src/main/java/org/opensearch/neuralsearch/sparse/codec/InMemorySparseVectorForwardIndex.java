/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * InMemorySparseVectorForwardIndex is used to store/read sparse vector in memory
 */
public class InMemorySparseVectorForwardIndex implements SparseVectorForwardIndex {

    private static final Map<InMemoryKey.IndexKey, InMemorySparseVectorForwardIndex> forwardIndexMap = new HashMap<>();

    public static InMemorySparseVectorForwardIndex getOrCreate(InMemoryKey.IndexKey key) {
        if (key == null) {
            throw new IllegalArgumentException("Index key cannot be null");
        }
        synchronized (forwardIndexMap) {
            return forwardIndexMap.computeIfAbsent(key, k -> new InMemorySparseVectorForwardIndex());
        }
    }

    public static void removeIndex(InMemoryKey.IndexKey key) {
        synchronized (forwardIndexMap) {
            forwardIndexMap.remove(key);
        }
    }

    private final Map<Integer, SparseVector> sparseVectorMap = new HashMap<>();

    public InMemorySparseVectorForwardIndex() {}

    @Override
    public SparseVectorForwardIndexReader getForwardIndexReader() {
        return new InMemorySparseVectorForwardIndexReader();
    }

    @Override
    public SparseVectorForwardIndexWriter getForwardIndexWriter() {
        return new InMemorySparseVectorForwardIndexWriter();
    }

    private class InMemorySparseVectorForwardIndexReader implements SparseVectorForwardIndexReader {

        @Override
        public SparseVector readSparseVector(int docId) {
            synchronized (sparseVectorMap) {
                return sparseVectorMap.get(docId);
            }
        }

        @Override
        public BytesRef read(int docId) {
            throw new UnsupportedOperationException();
        }
    }

    private class InMemorySparseVectorForwardIndexWriter implements SparseVectorForwardIndexWriter {

        @Override
        public void write(int docId, SparseVector vector) {
            if (vector == null) return;
            synchronized (sparseVectorMap) {
                // Use putIfAbsent to make the operation atomic and more efficient
                SparseVector existing = sparseVectorMap.putIfAbsent(docId, vector);
                if (existing != null) {
                    throw new IllegalArgumentException("Document ID " + docId + " already exists");
                }
            }
        }

        @Override
        public void write(int docId, BytesRef doc) throws IOException {
            write(docId, new SparseVector(doc));
        }

        @Override
        public void close() {}
    }
}
