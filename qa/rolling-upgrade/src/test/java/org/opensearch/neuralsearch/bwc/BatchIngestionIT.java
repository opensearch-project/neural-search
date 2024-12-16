/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import org.opensearch.neuralsearch.util.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.BatchIngestionUtils.prepareDataForBulkIngestion;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;

public class BatchIngestionIT extends AbstractRollingUpgradeTestCase {
    private static final String SPARSE_PIPELINE = "BatchIngestionIT_sparse_pipeline_rolling";
    private static final String TEXT_FIELD_NAME = "passage_text";
    private static final String EMBEDDING_FIELD_NAME = "passage_embedding";

    public void testBatchIngestion_SparseEncodingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        String sparseModelId = null;
        switch (getClusterType()) {
            case OLD:
                sparseModelId = uploadSparseEncodingModel();
                loadModel(sparseModelId);
                createPipelineForSparseEncodingProcessor(sparseModelId, SPARSE_PIPELINE, 2);
                createIndexWithConfiguration(
                    indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_PIPELINE
                );
                List<Map<String, String>> docs = prepareDataForBulkIngestion(0, 5);
                bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docs);
                validateDocCountAndInfo(indexName, 5, () -> getDocById(indexName, "4"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            case MIXED:
                sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                loadModel(sparseModelId);
                List<Map<String, String>> docsForMixed = prepareDataForBulkIngestion(5, 5);
                bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForMixed);
                validateDocCountAndInfo(indexName, 10, () -> getDocById(indexName, "9"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            case UPGRADED:
                try {
                    sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                    loadModel(sparseModelId);
                    List<Map<String, String>> docsForUpgraded = prepareDataForBulkIngestion(10, 5);
                    bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForUpgraded);
                    validateDocCountAndInfo(indexName, 15, () -> getDocById(indexName, "14"), EMBEDDING_FIELD_NAME, Map.class);
                } finally {
                    wipeOfTestResources(indexName, SPARSE_PIPELINE, sparseModelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
