/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

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
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        super.ingestPipelineName = SPARSE_PIPELINE;

        switch (getClusterType()) {
            case OLD:
                super.modelId = uploadSparseEncodingModel();
                loadModel(super.modelId);
                createPipelineForSparseEncodingProcessor(super.modelId, SPARSE_PIPELINE, 2);
                createIndexWithConfiguration(
                    super.indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_PIPELINE
                );
                List<Map<String, String>> docs = prepareDataForBulkIngestion(0, 5);
                bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docs);
                validateDocCountAndInfo(super.indexName, 5, () -> getDocById(super.indexName, "4"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            case MIXED:
                super.modelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                loadModel(super.modelId);
                List<Map<String, String>> docsForMixed = prepareDataForBulkIngestion(5, 5);
                bulkAddDocuments(super.indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForMixed);
                validateDocCountAndInfo(super.indexName, 10, () -> getDocById(super.indexName, "9"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            case UPGRADED:
                super.modelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                loadModel(super.modelId);
                List<Map<String, String>> docsForUpgraded = prepareDataForBulkIngestion(10, 5);
                bulkAddDocuments(super.indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForUpgraded);
                validateDocCountAndInfo(super.indexName, 15, () -> getDocById(super.indexName, "14"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
