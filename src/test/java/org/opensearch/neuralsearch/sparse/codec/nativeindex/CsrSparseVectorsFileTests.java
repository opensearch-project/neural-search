/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * The bytes {@link CsrSparseVectorsFile} hands nsparse. Asserted here rather than only through a
 * round trip because the mapped reader reinterprets the arrays in place -- a wrong width, byte order,
 * or padding offset is not something a round trip would localize.
 *
 * {@link NativeIndexCsrRoundTripTests} covers the other half: that nsparse accepts these bytes.
 */
public class CsrSparseVectorsFileTests extends AbstractSparseTestBase {

    private static final String SEGMENT = "_0";
    /** Codes come out as {@code round(weight * 255 / ceiling)}, so this ceiling keeps them readable. */
    private static final float CEILING = 2.55f;

    private Directory directory;

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        // FSDirectory, not newDirectory(): the class refuses anything nsparse cannot map.
        directory = FSDirectory.open(createTempDir());
    }

    @SneakyThrows
    @Override
    public void tearDown() {
        directory.close();
        super.tearDown();
    }

    @SneakyThrows
    public void testCsrLayoutMatchesTheNativeCodesFormat() {
        byte[] contents;
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(3, 1), List.of(2.55f, 1.275f));
            csrFile.addVector(List.of(CsrSparseVectorsFile.MAX_TOKEN_ID), List.of(0.01f));
            csrFile.finish(65536, new int[] { 0, 7 });
            contents = readFile(csrFile.resolveCsrPath());
        }

        ByteBuffer buffer = ByteBuffer.wrap(contents).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("rows", 2L, buffer.getLong());
        assertEquals("cols", 65536L, buffer.getLong());
        assertEquals("nnz", 3L, buffer.getLong());

        assertEquals("indptr[0]", 0, buffer.getInt());
        assertEquals("indptr[1]", 2, buffer.getInt());
        assertEquals("indptr[2]", 3, buffer.getInt());

        // Tokens keep the order the doc values stored them in, and 65535 has to survive as uint16
        // rather than wrapping to -1.
        assertEquals("indices[0]", 3, buffer.getShort() & 0xFFFF);
        assertEquals("indices[1]", 1, buffer.getShort() & 0xFFFF);
        assertEquals("indices[2]", 65535, buffer.getShort() & 0xFFFF);

        // The pad is to alignof(float) whatever the value width, so three 2-byte terms are still
        // followed by two zero bytes even though the codes that follow need no alignment at all.
        assertEquals("padding before the values", 2, CsrSparseVectorsFile.paddingBytes(3));
        assertEquals("padding byte", 0, buffer.getShort());

        // One byte per value, not four: the quantized indexes borrow codes at their own code width
        assertEquals("values[0]", 255, buffer.get() & 0xFF);
        assertEquals("values[1]", 128, buffer.get() & 0xFF);
        assertEquals("values[2]", 1, buffer.get() & 0xFF);

        assertEquals("nsparse checks the file size against the layout exactly", 0, buffer.remaining());
    }

    /** An even nnz already lands the values on a 4-byte boundary, so nsparse expects no padding. */
    @SneakyThrows
    public void testEvenNonZeroCountNeedsNoPadding() {
        byte[] contents;
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1, 2), List.of(2.55f, 1.275f));
            csrFile.finish(3, new int[] { 4 });
            contents = readFile(csrFile.resolveCsrPath());
        }

        assertEquals("padding before the values", 0, CsrSparseVectorsFile.paddingBytes(2));
        assertEquals(
            "native_file_size(indptr_size = 2, nnz = 2, element_size = 1)",
            CsrSparseVectorsFile.HEADER_BYTES + 2 * Integer.BYTES + 2 * Short.BYTES + 2 * CsrSparseVectorsFile.QUANTIZED_VALUE_BYTES,
            contents.length
        );
        int valuesOffset = CsrSparseVectorsFile.HEADER_BYTES + 2 * Integer.BYTES + 2 * Short.BYTES;
        assertEquals(255, contents[valuesOffset] & 0xFF);
        assertEquals(128, contents[valuesOffset + 1] & 0xFF);
    }

    /**
     * The codes must be the ones the index's vmax decodes, which is the ingest ceiling: nothing
     * re-encodes them on the way in.
     */
    @SneakyThrows
    public void testWeightsAreQuantizedWithTheGivenCeiling() {
        byte[] contents;
        try (
            CsrSparseVectorsFile csrFile = CsrSparseVectorsFile.forQuantizedIndex(
                directory,
                IOContext.DEFAULT,
                SEGMENT,
                new ByteQuantizer(1.0f)
            )
        ) {
            // Above the ceiling clamps, below zero clamps, and the middle rounds
            csrFile.addVector(List.of(0, 1, 2), List.of(5.0f, -1.0f, 0.5f));
            csrFile.finish(3, new int[] { 0 });
            contents = readFile(csrFile.resolveCsrPath());
        }

        int valuesOffset = CsrSparseVectorsFile.HEADER_BYTES + 2 * Integer.BYTES + 3 * Short.BYTES + CsrSparseVectorsFile.paddingBytes(3);
        assertEquals("a weight above the ceiling clamps to 255", 255, contents[valuesOffset] & 0xFF);
        assertEquals("a negative weight clamps to 0", 0, contents[valuesOffset + 1] & 0xFF);
        assertEquals("0.5 of the ceiling rounds to 128", 128, contents[valuesOffset + 2] & 0xFF);
    }

    /** {@code [int64 count][int32 id x count]}, row-aligned with the CSR. */
    @SneakyThrows
    public void testIdFileLayout() {
        byte[] contents;
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            csrFile.addVector(List.of(2), List.of(1.0f));
            csrFile.finish(3, new int[] { 5, 11 });
            contents = readFile(csrFile.resolveIdPath());
        }

        ByteBuffer buffer = ByteBuffer.wrap(contents).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("count", 2L, buffer.getLong());
        assertEquals("ids[0]", 5, buffer.getInt());
        assertEquals("ids[1]", 11, buffer.getInt());
        assertEquals("nsparse checks the id file size against its count", 0, buffer.remaining());
        assertEquals(CsrSparseVectorsFile.ID_HEADER_BYTES + 2 * Integer.BYTES, contents.length);
    }

    /**
     * nsparse cross-checks the id count against the CSR row count, but only after mapping the CSR.
     * Catching it here names the caller's mistake instead.
     */
    @SneakyThrows
    public void testIdCountMustMatchTheRowCount() {
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> csrFile.finish(2, new int[] { 1, 2 }));
            assertTrue(e.getMessage(), e.getMessage().contains("2 doc ids for 1 CSR rows"));
        }
    }

    /**
     * The mapped reader skips the per-term range check on purpose, and the JNI insert path that does
     * check is not on this route, so a token nsparse's uint16 cannot hold has to be caught here --
     * narrowing it silently would alias two tokens onto one term.
     */
    @SneakyThrows
    public void testTokenBeyondUint16IsRejected() {
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> csrFile.addVector(List.of(65536), List.of(1.0f))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("65536"));
            expectThrows(IllegalArgumentException.class, () -> csrFile.addVector(List.of(-1), List.of(1.0f)));
        }
    }

    /** nsparse rejects a header with rows == 0, so an empty segment must not reach it as a file. */
    @SneakyThrows
    public void testFinishRefusesAnEmptyFile() {
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            expectThrows(IllegalStateException.class, () -> csrFile.finish(4, new int[0]));
        }
    }

    /**
     * Only the two assembled files may be left behind: the scratch files the nnz-sized arrays stream
     * into are an implementation detail, and one left in the directory would be tracked into
     * segmentInfo.files() as a file the segment can never open.
     */
    @SneakyThrows
    public void testFinishLeavesOnlyTheAssembledFiles() {
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            csrFile.finish(2, new int[] { 0 });
            assertEquals("scratch files outlived finish()", 2, stagedFiles().size());
            assertTrue(stagedFiles().toString(), stagedFiles().stream().noneMatch(name -> name.contains("csr_indices")));
            assertTrue(stagedFiles().toString(), stagedFiles().stream().noneMatch(name -> name.contains("csr_values")));
        }
    }

    /**
     * Closing is what frees the staging area, on the failure paths too: nothing else deletes these
     * files, and they are not part of the segment.
     */
    @SneakyThrows
    public void testCloseDeletesEverythingItCreated() {
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            csrFile.finish(2, new int[] { 0 });
        }
        assertEquals("close() left files behind", List.of(), stagedFiles());

        // And before finish(), when only the scratch files exist
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            assertEquals("the two scratch files should exist while streaming", 2, stagedFiles().size());
        }
        assertEquals("close() left scratch files behind", List.of(), stagedFiles());
    }

    /** The paths are what go to nsparse, so they cannot be asked for before they name a file. */
    @SneakyThrows
    public void testPathsAreUnavailableBeforeFinish() {
        try (CsrSparseVectorsFile csrFile = newCsrFile()) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            expectThrows(IllegalStateException.class, csrFile::resolveCsrPath);
            expectThrows(IllegalStateException.class, csrFile::resolveIdPath);
        }
    }

    /** nsparse maps the files itself, so a directory with no filesystem behind it cannot be staged. */
    @SneakyThrows
    public void testRefusesADirectoryItCannotResolveAPathIn() {
        try (Directory inMemory = new ByteBuffersDirectory()) {
            IOException e = expectThrows(
                IOException.class,
                () -> CsrSparseVectorsFile.forQuantizedIndex(inMemory, IOContext.DEFAULT, SEGMENT, new ByteQuantizer(CEILING))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("filesystem path"));
            // ByteBuffersDirectory is not on the mock filesystem, so listAll() really is only ours
            assertEquals("a rejected directory should be left untouched", 0, inMemory.listAll().length);
        }
    }

    /**
     * The directory's contents, minus whatever else is in there: the test framework's
     * {@code ExtrasFS} drops a stray file into every new directory on purpose, so an assertion on
     * {@code listAll()} itself would be asserting the mock filesystem's behaviour.
     */
    private List<String> stagedFiles() throws IOException {
        return Arrays.stream(directory.listAll()).filter(name -> name.startsWith(SEGMENT)).toList();
    }

    private CsrSparseVectorsFile newCsrFile() throws IOException {
        return CsrSparseVectorsFile.forQuantizedIndex(directory, IOContext.DEFAULT, SEGMENT, new ByteQuantizer(CEILING));
    }

    private byte[] readFile(String path) throws IOException {
        return Files.readAllBytes(Path.of(path));
    }
}
