/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

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
 */
public class OffHeapVectorOwnershipTests extends AbstractSparseTestBase {

    private static final long[] NOTHING_ALLOCATED = new long[3];
    private static final String FIELD = "test_field";

    private Directory directory;

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        directory = FSDirectory.open(createTempDir());
    }

    @SneakyThrows
    @Override
    public void tearDown() {
        directory.close();
        SparseSettings.reset();
        super.tearDown();
    }

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
     * The reachable leak: {@link DefaultNativeIndexWriter#writeIndex} streams doc values into the
     * buffer, and the buffer transfers off-heap every time its batch limit is hit -- one attempt here
     * is ~24 MiB of vectors against a limit of 1% of the test JVM's heap, so it transfers many times.
     * If the doc
     * values then throw -- a corrupt value, an I/O error mid-merge -- the buffer is a local that was
     * never returned, so its addresses die with the frame and {@code insertToIndex} is never reached.
     *
     * Measured rather than asserted on a handle, because that is the point: after the throw there is
     * no handle left. Two identical batches of attempts, and the second one's growth is what is
     * asserted on: a leak grows by {@link #LEAKED_BYTES_PER_ATTEMPT} per attempt however many have
     * run already, while an allocator that keeps freed arenas rather than returning them to the OS
     * reaches its steady state during the first batch and stays there.
     */
    @SneakyThrows
    public void testFailedWriteDoesNotAbandonTransferredVectors() {
        assumeTrue("resident size is read from /proc", Files.exists(Path.of("/proc/self/statm")));
        // Defaults, so the writer's batch limit is derived from the heap rather than read from a
        // setting; nothing here needs a cluster service.
        SparseSettings.reset();

        runFailingWrites(0);
        long settled = residentBytes();
        runFailingWrites(ATTEMPTS_PER_BATCH);
        long growth = residentBytes() - settled;

        assertTrue(
            "a second batch of failed writes added " + growth / (1024 * 1024) + " MiB, so they are abandoning their vectors",
            growth < LEAKED_BYTES_PER_ATTEMPT
        );
    }

    /** {@link #ATTEMPTS_PER_BATCH} writes that stream every document off-heap and then fail. */
    private void runFailingWrites(int firstSegment) {
        for (int attempt = 0; attempt < ATTEMPTS_PER_BATCH; attempt++) {
            FieldInfo fieldInfo = fieldInfo();
            SegmentInfo segmentInfo = segmentInfo("_" + (firstSegment + attempt));
            DefaultNativeIndexWriter writer = new DefaultNativeIndexWriter(writeState(segmentInfo, fieldInfo), fieldInfo);
            expectThrows(IOException.class, () -> writer.writeIndex(failsAfterAllDocuments()));
        }
        // Resident size has to be read with the heap settled, or the JVM's own growth reads as leak
        System.gc();
    }

    // ---- helpers ----

    private static final int ATTEMPTS_PER_BATCH = 6;
    private static final int DOCS_PER_ATTEMPT = 20_000;
    private static final int TOKENS_PER_DOC = 200;
    /**
     * int32 token + float weight per non-zero, plus the CSR offset per document. One attempt's worth
     * is the threshold: a leaking batch adds ATTEMPTS_PER_BATCH times this, so the margin is 6x.
     */
    private static final long LEAKED_BYTES_PER_ATTEMPT = (long) DOCS_PER_ATTEMPT * (TOKENS_PER_DOC * 8L + 4L);

    @SneakyThrows
    private long residentBytes() {
        // statm field 2 is the resident set in pages; off-heap vectors are written to, so they are
        // resident, and freeing them returns the pages to the OS.
        String[] fields = Files.readString(Path.of("/proc/self/statm")).trim().split("\\s+");
        return Long.parseLong(fields[1]) * 4096L;
    }

    /**
     * Every document transfers cleanly and the iterator then fails, which is the worst case: the
     * whole segment is off-heap by the time the addresses are dropped.
     */
    private BinaryDocValues failsAfterAllDocuments() {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return vector(doc);
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                doc++;
                if (doc >= DOCS_PER_ATTEMPT) {
                    throw new IOException("simulated doc values failure");
                }
                return doc;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc;
            }

            @Override
            public long cost() {
                return DOCS_PER_ATTEMPT;
            }
        };
    }

    @SneakyThrows
    private BytesRef vector(int doc) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(TOKENS_PER_DOC * 8);
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            for (int token = 0; token < TOKENS_PER_DOC; token++) {
                dos.writeInt(token);
                dos.writeFloat(1.0f + doc);
            }
        }
        return new BytesRef(baos.toByteArray());
    }

    private SegmentWriteState writeState(SegmentInfo segmentInfo, FieldInfo fieldInfo) {
        return new SegmentWriteState(
            InfoStream.getDefault(),
            segmentInfo.dir,
            segmentInfo,
            new FieldInfos(new FieldInfo[] { fieldInfo }),
            null,
            IOContext.DEFAULT
        );
    }

    @SneakyThrows
    private SegmentInfo segmentInfo(String name) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            name,
            DOCS_PER_ATTEMPT,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
    }

    /** Below the approximate threshold, so no clustering runs: the failure is before the index anyway. */
    private FieldInfo fieldInfo() {
        FieldInfo fieldInfo = new FieldInfo(
            FIELD,
            0,
            false,
            false,
            false,
            IndexOptions.DOCS,
            DocValuesType.BINARY,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        fieldInfo.putAttribute(SPARSE_FIELD, "true");
        fieldInfo.putAttribute(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(Integer.MAX_VALUE));
        return fieldInfo;
    }

    private Map<String, Object> invertedIndexParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        parameters.put("index", "inverted");
        return parameters;
    }
}
