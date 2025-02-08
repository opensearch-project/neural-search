/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import org.opensearch.neuralsearch.util.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.BatchIngestionUtils.prepareDataForBulkIngestion;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;

public class BatchIngestionIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "pipeline-BatchIngestionIT";
    private static final String TEXT_FIELD_NAME = "passage_text";
    private static final String EMBEDDING_FIELD_NAME = "passage_embedding";
    private static final int batchSize = 3;

    public void testBatchIngestionWithNeuralSparseProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        super.ingestPipelineName = PIPELINE_NAME;

        if (isRunningAgainstOldCluster()) {
            super.modelId = uploadSparseEncodingModel();
            loadModel(super.modelId);
            createPipelineForSparseEncodingProcessor(super.modelId, PIPELINE_NAME, batchSize);
            createIndexWithConfiguration(
                super.indexName,
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                PIPELINE_NAME
            );
            List<Map<String, String>> docs = prepareDataForBulkIngestion(0, 5);
            bulkAddDocuments(super.indexName, TEXT_FIELD_NAME, PIPELINE_NAME, docs);
            validateDocCountAndInfo(super.indexName, 5, () -> getDocById(super.indexName, "4"), EMBEDDING_FIELD_NAME, Map.class);
        } else {
            super.modelId = TestUtils.getModelId(getIngestionPipeline(PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
            loadModel(super.modelId);
            List<Map<String, String>> docs = prepareDataForBulkIngestion(5, 5);
            bulkAddDocuments(super.indexName, TEXT_FIELD_NAME, PIPELINE_NAME, docs);
            validateDocCountAndInfo(super.indexName, 10, () -> getDocById(super.indexName, "9"), EMBEDDING_FIELD_NAME, Map.class);
        }
    }

}
