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
                createPipelineForSparseEncodingProcessor(sparseModelId, SPARSE_PIPELINE);
                createIndexWithConfiguration(
                    indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_PIPELINE
                );
                List<Map<String, String>> docs = prepareDocsForBulk(0, 5);
                addDocsThroughBulk(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docs, 2);
                validateDocCountAndEmbedding(indexName, 5, "4", EMBEDDING_FIELD_NAME);
                break;
            case MIXED:
                sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                loadModel(sparseModelId);
                List<Map<String, String>> docsForMixed = prepareDocsForBulk(5, 5);
                addDocsThroughBulk(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForMixed, 3);
                validateDocCountAndEmbedding(indexName, 10, "9", EMBEDDING_FIELD_NAME);
                break;
            case UPGRADED:
                try {
                    sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                    loadModel(sparseModelId);
                    List<Map<String, String>> docsForUpgraded = prepareDocsForBulk(10, 5);
                    addDocsThroughBulk(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForUpgraded, 2);
                    validateDocCountAndEmbedding(indexName, 15, "14", EMBEDDING_FIELD_NAME);
                } finally {
                    wipeOfTestResources(indexName, SPARSE_PIPELINE, sparseModelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }
}
