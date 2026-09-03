/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.io;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

/**
 * Wraps a Lucene {@link IndexOutput} for JNI streaming writes.
 * <p>
 * The native side accumulates data in a native buffer, then on flush
 * calls {@link #writeBytes(byte[], int, int)} to write directly to the
 * underlying IndexOutput.
 */
public class IndexOutputWrapper implements Closeable {
    private final IndexOutput indexOutput;

    public IndexOutputWrapper(IndexOutput indexOutput) {
        this.indexOutput = indexOutput;
    }

    /**
     * Called from native code to write bytes directly to the IndexOutput.
     *
     * @param bytes  the byte array containing data
     * @param offset start offset in the array
     * @param length number of bytes to write
     */
    public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
        indexOutput.writeBytes(bytes, offset, length);
    }

    /**
     * Byte offset within the output file at which the native writer's first byte
     * will land.
     * <p>
     * Read from native code, which needs the absolute file offset rather than a
     * count of bytes it has written: nsparse pads each serialized array up to its
     * element alignment, and the mmap reader recomputes that padding from the
     * offset in the file. If the two disagree the mapped load fails with
     * "array is misaligned for its element type" — so a header written here before
     * the native payload has to be visible to the padding arithmetic.
     *
     * @return the current file pointer of the wrapped IndexOutput
     */
    public long getFilePointer() {
        return indexOutput.getFilePointer();
    }

    @Override
    public void close() throws IOException {
        indexOutput.close();
    }
}
