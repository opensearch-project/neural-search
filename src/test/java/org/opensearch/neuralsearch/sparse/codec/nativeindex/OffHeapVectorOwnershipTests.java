/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Who frees the off-heap CSR vectors that {@link OffHeapSparseVectorsBuffer} transfers.
 *
 * The buffer's {@code memoryAddresses} are the only Java-visible handle on that memory, and
 * {@code insertToIndex} is the only routine that frees it -- it adopts the three vectors into
 * {@code unique_ptr}s. So any path that transfers vectors without reaching {@code insertToIndex}
 * drops the last reference to memory nothing will ever release, and the addresses going back to
 * zero is what "released" looks like from Java.
 *
 * These run against the real nsparse library, like {@link NativeIndexRoundTripTests}: Mockito
 * cannot stub a native method, so there is no way to observe the transfer other than making it.
 *
 * Scoped to the buffer on purpose. {@link DefaultNativeIndexWriter#writeIndex} relies on these same
 * guarantees to survive doc values that throw mid-stream, but the only signal a test has for that is
 * the process's resident size, which nothing here controls -- see issue 2016.
 */
public class OffHeapVectorOwnershipTests extends AbstractSparseTestBase {

    private static final long[] NOTHING_ALLOCATED = new long[3];

    /**
     * A buffer whose vectors were never handed to {@code insertToIndex} still owns them, so closing
     * it has to free them. Nothing else can: Java holds no other copy of the addresses, and the
     * native side only frees on the insert path.
     */
    @SneakyThrows
    public void testCloseFreesVectorsThatWereNeverInserted() {
        // The one-byte limit makes the first addVector transfer, so the memory is really off-heap
        long[] addresses;
        try (OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(1L)) {
            buffer.addVector(List.of(7), List.of(1.0f));
            addresses = buffer.getMemoryAddresses();
            assertTrue("addVector past the limit should have transferred the vector off-heap", addresses[0] != 0);
        }

        assertArrayEquals("close() left off-heap vectors that nothing else can free", NOTHING_ALLOCATED, addresses);
    }

    /**
     * Closing must not transfer what it is about to abandon. Flushing on the way out allocates a
     * fresh off-heap vector whose address the caller never sees -- closing the buffer grows the
     * off-heap footprint instead of releasing it.
     */
    @SneakyThrows
    public void testCloseDoesNotTransferVectorsItWillNotFree() {
        // No limit, so nothing is transferred while adding: everything is still on-heap at close
        long[] addresses;
        try (OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(Long.MAX_VALUE)) {
            buffer.addVector(List.of(7), List.of(1.0f));
            addresses = buffer.getMemoryAddresses();
            assertArrayEquals("nothing should be off-heap before the limit is reached", NOTHING_ALLOCATED, addresses);
        }

        assertArrayEquals("close() transferred vectors off-heap and then abandoned them", NOTHING_ALLOCATED, addresses);
    }

    /**
     * The other half of the invariant: once {@code insertToIndex} has adopted the vectors, closing
     * the buffer must not free them a second time. A double free corrupts the allocator rather than
     * throwing, so the assertion is that the index built from those vectors still answers -- run
     * with {@code -PjniSanitizers=true} to have ASan fail on it outright.
     */
    @SneakyThrows
    public void testCloseAfterInsertDoesNotFreeTheVectorsTwice() {
        long indexAddress = NativeLibrary.initIndex(1, 4096, invertedIndexParameters());
        try {
            long[] addresses;
            try (OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(1L)) {
                buffer.addVector(List.of(7), List.of(2.0f));
                addresses = buffer.getMemoryAddresses();
                buffer.insertInto(indexAddress, new int[] { 0 }, 1);
                assertArrayEquals("the insert takes the vectors over, so the buffer owns nothing", NOTHING_ALLOCATED, addresses);
            }

            SparseQueryResult[] results = NativeLibrary.queryIndex(indexAddress, new int[] { 7 }, new float[] { 1.0f }, 1, new HashMap<>());
            assertEquals(1, results.length);
            assertEquals(0, results[0].getId());
            assertEquals(2.0f, results[0].getScore(), 0.001f);
        } finally {
            NativeLibrary.freeIndex(indexAddress);
        }
    }

    /**
     * A single flush's relative indptr is a plain int, so its running non-zero count must fit one.
     * The cumulative offset is widened to 64-bit on the native side, but overflowing an int here would
     * wrap the indptr negative and silently corrupt what nsparse maps -- so the buffer refuses it.
     * Checked directly rather than by adding INT_MAX non-zeros, which no test heap could stage.
     */
    public void testPerFlushNnzOverflowIsRejected() {
        // Well within an int: the common case, where the guard does nothing.
        OffHeapSparseVectorsBuffer.requirePerFlushNnzFitsInt(1_000_000, 200);

        // The boundary: a running count that a further batch pushes past Integer.MAX_VALUE.
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> OffHeapSparseVectorsBuffer.requirePerFlushNnzFitsInt(Integer.MAX_VALUE - 1, 2)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Integer.MAX_VALUE non-zeros"));
    }

    // ---- helpers ----

    private Map<String, Object> invertedIndexParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        parameters.put("index", "inverted");
        return parameters;
    }
}
