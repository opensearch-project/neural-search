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

import static org.opensearch.neuralsearch.util.BatchIngestionUtils.prepareDocsForBulk;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.SPARSE_ENCODING_PROCESSOR;

public class BatchIngestionIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "pipeline-BatchIngestionIT";
    private static final String TEXT_FIELD_NAME = "passage_text";
    private static final String EMBEDDING_FIELD_NAME = "passage_embedding";

    public void testBatchIngestionWithNeuralSparseProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        String indexName = getIndexNameForTest();
        final int batchSize = 3;
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadSparseEncodingModel();
            loadModel(modelId);
            createPipelineForSparseEncodingProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                indexName,
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                PIPELINE_NAME
            );
            List<Map<String, String>> docs = prepareDocsForBulk(0, 5);
            addDocsThroughBulk(indexName, TEXT_FIELD_NAME, PIPELINE_NAME, docs, batchSize);
            validateDocCountAndEmbedding(indexName, 5, "4", EMBEDDING_FIELD_NAME);
        } else {
            String modelId = null;
            modelId = TestUtils.getModelId(getIngestionPipeline(PIPELINE_NAME), SPARSE_ENCODING_PROCESSOR);
            loadModel(modelId);
            try {
                List<Map<String, String>> docs = prepareDocsForBulk(5, 5);
                addDocsThroughBulk(indexName, TEXT_FIELD_NAME, PIPELINE_NAME, docs, batchSize);
                validateDocCountAndEmbedding(indexName, 10, "9", EMBEDDING_FIELD_NAME);
            } finally {
                wipeOfTestResources(indexName, PIPELINE_NAME, modelId, null);
            }
        }
    }

}
