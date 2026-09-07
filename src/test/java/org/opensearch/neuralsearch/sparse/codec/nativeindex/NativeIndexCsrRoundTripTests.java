/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * That nsparse accepts what {@link CsrSparseVectorsFile} writes, driven against the real library like
 * {@link NativeIndexRoundTripTests} -- {@code IDMapIndex::read_csr_and_ids} maps the files and
 * reinterprets their arrays in place, so only a real build proves the layout.
 *
 * These go through {@link NativeLibrary} rather than {@link CsrFileNativeIndexWriter} so the index
 * parameters are explicit: both quantized layouts are exercised here, whereas a writer test would
 * pick one from the field mapping.
 */
public class NativeIndexCsrRoundTripTests extends AbstractSparseTestBase {

    private static final String SEGMENT = "_0";
    private static final int DOC_COUNT = 24;
    private static final int DIMENSION = 32;
    /** Must equal the {@code vmax} the index is built with, since nothing re-encodes the codes. */
    private static final float CEILING = 32.0f;

    private Directory directory;
    private final List<Long> loadedIndexes = new ArrayList<>();

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        directory = FSDirectory.open(createTempDir());
    }

    @SneakyThrows
    @Override
    public void tearDown() {
        // nsparse maps the engine file, and Windows refuses to delete a mapped file, so the
        // framework's temp-dir cleanup fails unless every handle is freed -- including on the paths
        // where an assertion threw first.
        for (long address : loadedIndexes) {
            if (address != 0) {
                NativeLibrary.freeIndex(address);
            }
        }
        loadedIndexes.clear();
        directory.close();
        super.tearDown();
    }

    /** Weights rise by document, so the ranking is known whichever blocks the search picks. */
    @SneakyThrows
    public void testBuildFromCsrFilesRoundTrip() {
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            vectors.add(Map.of(7, 1.0f + doc));
        }

        long address = buildAndLoad(vectors, docIds(DOC_COUNT));
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 7 }, new float[] { 1.0f }, 3, searchParameters());

        assertEquals(3, results.length);
        assertEquals("highest weight first", DOC_COUNT - 1, results[0].getId());
        assertTrue("scores should descend", results[0].getScore() >= results[1].getScore());
        // 8-bit codes over [0, CEILING], decoded back with the same range, so the top score is the
        // dot product of the highest weight with 1.0 to within a quantization step
        assertEquals((float) DOC_COUNT, results[0].getScore(), CEILING / 255.0f);
    }

    /**
     * The id file is what makes a mapped CSR usable at all: rows are dense and consecutive, doc ids
     * are neither once a segment has deletions or documents missing the field.
     */
    @SneakyThrows
    public void testRowsAreMappedBackToTheirDocIds() {
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        int[] docIds = new int[] { 3, 17, 100 };
        for (int row = 0; row < docIds.length; row++) {
            vectors.add(Map.of(7, 1.0f + row));
        }

        long address = buildAndLoad(vectors, docIds);
        SparseQueryResult[] results = NativeLibrary.queryIndex(
            address,
            new int[] { 7 },
            new float[] { 1.0f },
            docIds.length,
            searchParameters()
        );

        assertEquals(docIds.length, results.length);
        // Row 2 carries the largest weight, and its doc id is 100 rather than its row number
        assertEquals("the top hit should be a doc id, not a row index", 100, results[0].getId());
        for (SparseQueryResult result : results) {
            assertTrue(
                "returned id " + result.getId() + " is not one of the staged doc ids",
                result.getId() == 3 || result.getId() == 17 || result.getId() == 100
            );
        }
    }

    /** An odd non-zero count is where the values need padding to stay 4-byte aligned for the map. */
    @SneakyThrows
    public void testOddNonZeroCountBuilds() {
        // Three non-zeros across two rows
        List<Map<Integer, Float>> vectors = List.of(Map.of(1, 2.0f, 2, 3.0f), Map.of(1, 5.0f));

        long address = buildAndLoad(vectors, new int[] { 0, 1 });
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 1 }, new float[] { 1.0f }, 2, searchParameters());

        assertEquals(2, results.length);
        assertEquals("doc 1 has the larger weight for token 1", 1, results[0].getId());
        assertEquals(5.0f, results[0].getScore(), CEILING / 255.0f);
    }

    /**
     * nsparse validates the id file before it lets the delegate touch the CSR, so a count that
     * disagrees with the rows is refused rather than silently mis-mapping ids.
     */
    @SneakyThrows
    public void testMismatchedIdCountIsRefused() {
        try (
            CsrSparseVectorsFile csrFile = CsrSparseVectorsFile.forQuantizedIndex(
                directory,
                IOContext.DEFAULT,
                SEGMENT,
                new ByteQuantizer(CEILING)
            )
        ) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            csrFile.finish(DIMENSION, new int[] { 0 });

            // A stale id file with a row count the CSR does not have
            String idPath = writeIdFile(new int[] { 0, 1 });

            long indexAddress = NativeLibrary.initIndex(DOC_COUNT, DIMENSION, indexParameters());
            try {
                Exception e = expectThrows(
                    Exception.class,
                    () -> NativeLibrary.readCsrAndIdsToIndex(indexAddress, csrFile.resolveCsrPath(), idPath, 1)
                );
                assertTrue(e.getMessage(), e.getMessage().contains("does not match"));
            } finally {
                NativeLibrary.freeIndex(indexAddress);
            }
        }
    }

    /** The row-to-doc-id map has nowhere to go without an idmap index, so the JNI refuses one. */
    @SneakyThrows
    public void testNonIdMapIndexIsRefused() {
        try (
            CsrSparseVectorsFile csrFile = CsrSparseVectorsFile.forQuantizedIndex(
                directory,
                IOContext.DEFAULT,
                SEGMENT,
                new ByteQuantizer(CEILING)
            )
        ) {
            csrFile.addVector(List.of(1), List.of(1.0f));
            csrFile.finish(DIMENSION, new int[] { 0 });

            Map<String, Object> parameters = indexParameters();
            parameters.put("idmap", false);
            long indexAddress = NativeLibrary.initIndex(DOC_COUNT, DIMENSION, parameters);
            try {
                Exception e = expectThrows(
                    Exception.class,
                    () -> NativeLibrary.readCsrAndIdsToIndex(indexAddress, csrFile.resolveCsrPath(), csrFile.resolveIdPath(), 1)
                );
                assertTrue(e.getMessage(), e.getMessage().contains("idmap"));
            } finally {
                NativeLibrary.freeIndex(indexAddress);
            }
        }
    }

    /**
     * A batched build over a mapped CSR. Batching the term space (nsparse #45) spills each window of
     * clustered lists and re-reads the vectors per window, so it is worth pinning that it works when
     * the vectors are borrowed from a mapping rather than owned -- and that it changes nothing about
     * the result, which is the whole point of a memory-bounding knob.
     */
    @SneakyThrows
    public void testBatchedBuildFromCsrFilesMatchesUnbatched() {
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            vectors.add(Map.of(7, 1.0f + doc, 9, 0.5f));
        }

        SparseQueryResult[] unbatched = queryTopHits(buildAndLoad(vectors, docIds(DOC_COUNT), indexParameters()));

        Map<String, Object> batched = indexParameters();
        batched.put("inverted_list_batch_size", 4);
        batched.put("batch_file_output_path", CodecUtils.resolveDirectoryPath(directory).toString());
        SparseQueryResult[] fromBatched = queryTopHits(buildAndLoad(vectors, docIds(DOC_COUNT), batched, "batched"));

        assertEquals(unbatched.length, fromBatched.length);
        for (int i = 0; i < unbatched.length; i++) {
            assertEquals("hit " + i + " doc id", unbatched[i].getId(), fromBatched[i].getId());
            assertEquals("hit " + i + " score", unbatched[i].getScore(), fromBatched[i].getScore(), 0.0f);
        }
    }

    private SparseQueryResult[] queryTopHits(long address) {
        return NativeLibrary.queryIndex(address, new int[] { 7 }, new float[] { 1.0f }, 5, searchParameters());
    }

    // ---- helpers ----

    /**
     * Stages the vectors, builds the index from the files, serializes it the way the writer does, and
     * loads the result back.
     */
    @SneakyThrows
    private long buildAndLoad(List<Map<Integer, Float>> vectors, int[] docIds) {
        return buildAndLoad(vectors, docIds, indexParameters(), "default");
    }

    @SneakyThrows
    private long buildAndLoad(List<Map<Integer, Float>> vectors, int[] docIds, Map<String, Object> parameters) {
        return buildAndLoad(vectors, docIds, parameters, "default");
    }

    @SneakyThrows
    private long buildAndLoad(List<Map<Integer, Float>> vectors, int[] docIds, Map<String, Object> parameters, String label) {
        String engineFileName = SEGMENT + "_csr_roundtrip_" + label + ".nsparse";
        try (
            IndexOutput output = directory.createOutput(engineFileName, IOContext.DEFAULT);
            CsrSparseVectorsFile csrFile = CsrSparseVectorsFile.forQuantizedIndex(
                directory,
                IOContext.DEFAULT,
                SEGMENT,
                new ByteQuantizer(CEILING)
            )
        ) {
            for (Map<Integer, Float> vector : vectors) {
                List<Integer> tokens = new ArrayList<>(vector.keySet());
                List<Float> weights = new ArrayList<>();
                for (int token : tokens) {
                    weights.add(vector.get(token));
                }
                csrFile.addVector(tokens, weights);
            }
            csrFile.finish(DIMENSION, docIds);

            long indexAddress = NativeLibrary.initIndex(DOC_COUNT, DIMENSION, parameters);
            boolean ownershipTransferred = false;
            try {
                NativeLibrary.readCsrAndIdsToIndex(indexAddress, csrFile.resolveCsrPath(), csrFile.resolveIdPath(), 1);
                ownershipTransferred = true;
                NativeLibrary.writeIndex(indexAddress, new IndexOutputWrapper(output));
            } finally {
                if (ownershipTransferred == false) {
                    NativeLibrary.freeIndex(indexAddress);
                }
            }
        }

        long address = NativeLibrary.loadIndex(CodecUtils.resolveFilePath(directory, engineFileName));
        loadedIndexes.add(address);
        return address;
    }

    /** An id file that {@link CsrSparseVectorsFile} did not write, for the mismatch case. */
    private String writeIdFile(int[] docIds) throws IOException {
        String name;
        try (IndexOutput output = directory.createTempOutput(SEGMENT, "stale_ids", IOContext.DEFAULT)) {
            name = output.getName();
            output.writeLong(docIds.length);
            for (int docId : docIds) {
                output.writeInt(docId);
            }
        }
        return CodecUtils.resolveFilePath(directory, name);
    }

    private static int[] docIds(int count) {
        int[] docIds = new int[count];
        for (int i = 0; i < count; i++) {
            docIds[i] = i;
        }
        return docIds;
    }

    /** What NativeIndexParameters builds for a field over the seismic threshold. */
    private Map<String, Object> indexParameters() {
        return indexParameters("seismic_sq");
    }

    private Map<String, Object> indexParameters(String indexType) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        parameters.put("index", indexType);
        parameters.put("quantizer", "8bit");
        parameters.put("vmin", 0.0f);
        parameters.put("vmax", CEILING);
        parameters.put("lambda", 10);
        parameters.put("beta", 5.0f);
        parameters.put("alpha", 1.0f);
        return parameters;
    }

    private Map<String, Object> searchParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("cut", 10);
        parameters.put("heap_factor", 1.0f);
        // The range the query is encoded with and the scores decoded by; a quantized index reads it
        // off the search parameters rather than reusing its build-time one.
        parameters.put("vmin", 0.0f);
        parameters.put("vmax", CEILING);
        return parameters;
    }
}
