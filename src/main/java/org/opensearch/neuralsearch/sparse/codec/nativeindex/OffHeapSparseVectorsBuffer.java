/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import lombok.Getter;
import org.opensearch.neuralsearch.jni.NativeLibrary;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

/**
 * Buffers sparse vectors on-heap in CSR format using raw primitive arrays,
 * and flushes them to off-heap memory via {@link NativeLibrary#transferVectors}
 * either explicitly or when a predefined byte size limit is reached.
 */
public class OffHeapSparseVectorsBuffer implements Closeable {
    private static final int DEFAULT_CAPACITY = 1024;

    @Getter
    private final long[] memoryAddresses = new long[3];
    private final long byteSizeLimit;

    private int[] indices;
    private int indicesSize;

    private int[] tokens;
    private float[] values;
    private int nnzSize;

    private long currentByteSize;

    public OffHeapSparseVectorsBuffer(long byteSizeLimit) {
        this.byteSizeLimit = byteSizeLimit;
        this.indices = new int[DEFAULT_CAPACITY];
        this.indices[0] = 0;
        this.indicesSize = 1;
        this.tokens = new int[DEFAULT_CAPACITY];
        this.values = new float[DEFAULT_CAPACITY];
        this.nnzSize = 0;
        this.currentByteSize = 0;
    }

    public void addVector(List<Integer> vectorTokens, List<Float> vectorValues) {
        addVector(Ints.toArray(vectorTokens), Floats.toArray(vectorValues));
    }

    public void addVector(int[] vectorTokens, float[] vectorValues) {
        ensureNnzCapacity(nnzSize + vectorTokens.length);
        System.arraycopy(vectorTokens, 0, tokens, nnzSize, vectorTokens.length);
        System.arraycopy(vectorValues, 0, values, nnzSize, vectorValues.length);
        nnzSize += vectorTokens.length;

        ensureIndicesCapacity(indicesSize + 1);
        indices[indicesSize] = nnzSize;
        indicesSize++;

        currentByteSize += (long) vectorTokens.length * (Short.BYTES + Float.BYTES) + Integer.BYTES;
        if (currentByteSize >= byteSizeLimit) {
            flush();
        }
    }

    public void flush() {
        if (indicesSize <= 1) {
            return;
        }

        NativeLibrary.transferVectors(
            memoryAddresses,
            Arrays.copyOf(indices, indicesSize),
            Arrays.copyOf(tokens, nnzSize),
            Arrays.copyOf(values, nnzSize)
        );

        reset();
    }

    private void reset() {
        indicesSize = 1;
        indices[0] = 0;
        nnzSize = 0;
        currentByteSize = 0;
    }

    private void ensureNnzCapacity(int minCapacity) {
        if (minCapacity > tokens.length) {
            int newCapacity = Math.max(tokens.length * 2, minCapacity);
            tokens = Arrays.copyOf(tokens, newCapacity);
            values = Arrays.copyOf(values, newCapacity);
        }
    }

    private void ensureIndicesCapacity(int minCapacity) {
        if (minCapacity > indices.length) {
            int newCapacity = Math.max(indices.length * 2, minCapacity);
            indices = Arrays.copyOf(indices, newCapacity);
        }
    }

    /**
     * Hands the transferred vectors to the index, which takes ownership of them.
     *
     * The addresses are dropped first: {@code insertToIndex} adopts all three vectors before
     * anything that can throw and frees them on every path out, so once it has been entered this
     * buffer must never free them again. Pending on-heap vectors are not flushed -- the caller
     * flushes when it is done adding, because a flush after this call would allocate vectors that
     * the index has already been built without.
     */
    public void insertInto(long indexAddress, int[] docIds, int threadCount) {
        long[] consumed = memoryAddresses.clone();
        Arrays.fill(memoryAddresses, 0);
        NativeLibrary.insertToIndex(indexAddress, docIds, consumed[0], consumed[1], consumed[2], threadCount);
    }

    /**
     * Releases the off-heap vectors this buffer still owns, which is nothing at all once
     * {@link #insertInto} has handed them over. Pending on-heap vectors are dropped rather than
     * flushed: a caller that is closing has no way to reach the addresses a flush would allocate.
     */
    @Override
    public void close() {
        NativeLibrary.freeVectors(memoryAddresses);
    }
}
