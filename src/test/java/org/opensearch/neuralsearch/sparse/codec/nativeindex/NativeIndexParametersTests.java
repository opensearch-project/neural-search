/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTERING_BATCH_SIZE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;

/**
 * That the field's mapping arrives as the nsparse {@code index_factory} keys, since a dropped or
 * misnamed key is silently ignored by the factory and leaves the segment built to defaults.
 */
public class NativeIndexParametersTests extends AbstractSparseTestBase {

    private static final String BATCH_SIZE_KEY = "inverted_list_batch_size";
    private static final String SPILL_PATH_KEY = "batch_file_output_path";

    public void testSubThresholdSegmentGetsAnInvertedIndexAndNoClusterParameters() {
        Map<String, Object> parameters = build(attributes(Map.of(APPROXIMATE_THRESHOLD_FIELD, "1000")), new ByteBuffersDirectory());

        assertEquals("inverted", parameters.get("index"));
        assertNull(parameters.get("lambda"));
        assertNull(parameters.get(BATCH_SIZE_KEY));
    }

    public void testDefaultBatchSizeSendsNeitherBatchKey() {
        Map<String, Object> parameters = build(attributes(Map.of()), new ByteBuffersDirectory());

        assertEquals("seismic_sq", parameters.get("index"));
        assertNull(parameters.get(BATCH_SIZE_KEY));
        assertNull(parameters.get(SPILL_PATH_KEY));
    }

    @SneakyThrows
    public void testBatchSizeSendsBothBatchKeysWithTheSegmentDirectoryToSpillInto() {
        try (Directory directory = FSDirectory.open(createTempDir())) {
            Map<String, Object> parameters = build(attributes(Map.of(CLUSTERING_BATCH_SIZE_FIELD, "8")), directory);

            assertEquals(8, parameters.get(BATCH_SIZE_KEY));
            assertEquals(CodecUtils.resolveDirectoryPath(directory).toString(), parameters.get(SPILL_PATH_KEY));
        }
    }

    /**
     * nsparse maps the spill itself, so a directory it cannot open is nothing to fail the segment
     * over: the build falls back to a single window, which is what it did before the parameter.
     */
    public void testBatchSizeIsDroppedWhenTheDirectoryHasNoFilesystemPath() {
        Map<String, Object> parameters = build(attributes(Map.of(CLUSTERING_BATCH_SIZE_FIELD, "8")), new ByteBuffersDirectory());

        assertEquals("seismic_sq", parameters.get("index"));
        assertNull(parameters.get(BATCH_SIZE_KEY));
        assertNull(parameters.get(SPILL_PATH_KEY));
    }

    /** The forward index picks the layout, and batching applies to either. */
    public void testPerBlockForwardIndexIsBatchedToo() {
        Map<String, Object> parameters = build(
            attributes(Map.of(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName(), CLUSTERING_BATCH_SIZE_FIELD, "4")),
            new ByteBuffersDirectory()
        );

        assertEquals("disk_seismic_sq", parameters.get("index"));
    }

    private Map<String, Object> build(Map<String, String> attributes, Directory directory) {
        FieldInfo fieldInfo = fieldInfo(attributes);
        SegmentWriteState state = TestsPrepareUtils.prepareSegmentWriteState(directory, new FieldInfos(new FieldInfo[] { fieldInfo }));
        return NativeIndexParameters.build(state, fieldInfo);
    }

    /** The attributes {@code SparseVectorFieldMapper} writes for a seismic field, with overrides. */
    private Map<String, String> attributes(Map<String, String> overrides) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(N_POSTINGS_FIELD, "100");
        attributes.put(CLUSTER_RATIO_FIELD, "0.1");
        attributes.put(SUMMARY_PRUNE_RATIO_FIELD, "0.4");
        attributes.put(QUANTIZATION_CEILING_INGEST_FIELD, "3.0");
        // The prepared segment holds 10 documents, so this is over the threshold
        attributes.put(APPROXIMATE_THRESHOLD_FIELD, "1");
        attributes.put(FORWARD_INDEX_FIELD, SparseForwardIndex.DEFAULT.getName());
        attributes.putAll(overrides);
        return attributes;
    }

    private FieldInfo fieldInfo(Map<String, String> attributes) {
        return new FieldInfo(
            "test_field",
            0,
            false,
            false,
            false,
            IndexOptions.DOCS,
            DocValuesType.BINARY,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(attributes),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
