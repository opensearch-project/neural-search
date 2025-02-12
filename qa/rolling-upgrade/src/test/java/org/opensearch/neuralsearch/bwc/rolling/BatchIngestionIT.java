/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.ml.common.model.MLModelState;

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
        String indexName = getIndexNameForTest();
        String sparseModelId = null;
        switch (getClusterType()) {
            case OLD:
                sparseModelId = uploadSparseEncodingModel();
                loadModel(sparseModelId);
                MLModelState oldModelState = getModelState(sparseModelId);
                logger.info("Model state in OLD phase: {}", oldModelState);
                if (oldModelState != MLModelState.LOADED) {
                    logger.error("Model {} is not in LOADED state in OLD phase. Current state: {}", sparseModelId, oldModelState);
                    waitForModelToLoad(sparseModelId);
                }
                createPipelineForSparseEncodingProcessor(sparseModelId, SPARSE_PIPELINE, 2);
                logger.info("Pipeline state in OLD phase: {}", getIngestionPipeline(SPARSE_PIPELINE));
                createIndexWithConfiguration(
                    indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                    SPARSE_PIPELINE
                );
                List<Map<String, String>> docs = prepareDataForBulkIngestion(0, 5);
                bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docs);
                logger.info("Document count after OLD phase ingestion: {}", getDocCount(indexName));
                validateDocCountAndInfo(indexName, 5, () -> getDocById(indexName, "4"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            case MIXED:
                sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                loadModel(sparseModelId);
                MLModelState mixedModelState = getModelState(sparseModelId);
                logger.info("Model state in MIXED phase: {}", mixedModelState);
                if (mixedModelState != MLModelState.LOADED) {
                    logger.error("Model {} is not in LOADED state in MIXED phase. Current state: {}", sparseModelId, mixedModelState);
                    waitForModelToLoad(sparseModelId);
                }
                logger.info("Pipeline state in MIXED phase: {}", getIngestionPipeline(SPARSE_PIPELINE));
                List<Map<String, String>> docsForMixed = prepareDataForBulkIngestion(5, 5);
                logger.info("Document count before MIXED phase ingestion: {}", getDocCount(indexName));
                bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForMixed);
                logger.info("Document count after MIXED phase ingestion: {}", getDocCount(indexName));
                validateDocCountAndInfo(indexName, 10, () -> getDocById(indexName, "9"), EMBEDDING_FIELD_NAME, Map.class);
                break;
            case UPGRADED:
                try {
                    sparseModelId = TestUtils.getModelId(getIngestionPipeline(SPARSE_PIPELINE), SPARSE_ENCODING_PROCESSOR);
                    loadModel(sparseModelId);
                    MLModelState upgradedModelState = getModelState(sparseModelId);
                    logger.info("Model state in UPGRADED phase: {}", upgradedModelState);
                    if (upgradedModelState != MLModelState.LOADED) {
                        logger.error(
                            "Model {} is not in LOADED state in UPGRADED phase. Current state: {}",
                            sparseModelId,
                            upgradedModelState
                        );
                        waitForModelToLoad(sparseModelId);
                    }
                    logger.info("Pipeline state in UPGRADED phase: {}", getIngestionPipeline(SPARSE_PIPELINE));
                    List<Map<String, String>> docsForUpgraded = prepareDataForBulkIngestion(10, 5);
                    logger.info("Document count before UPGRADED phase ingestion: {}", getDocCount(indexName));
                    bulkAddDocuments(indexName, TEXT_FIELD_NAME, SPARSE_PIPELINE, docsForUpgraded);
                    logger.info("Document count after UPGRADED phase ingestion: {}", getDocCount(indexName));
                    validateDocCountAndInfo(indexName, 15, () -> getDocById(indexName, "14"), EMBEDDING_FIELD_NAME, Map.class);
                } finally {
                    wipeOfTestResources(indexName, SPARSE_PIPELINE, sparseModelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void waitForModelToLoad(String modelId) throws Exception {
        int maxAttempts = 30;  // Maximum number of attempts
        int waitTimeInSeconds = 2;  // Time to wait between attempts

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            MLModelState state = getModelState(modelId);
            if (state == MLModelState.LOADED) {
                logger.info("Model {} is now loaded after {} attempts", modelId, attempt + 1);
                return;
            }
            logger.info("Waiting for model {} to load. Current state: {}. Attempt {}/{}", modelId, state, attempt + 1, maxAttempts);
            Thread.sleep(waitTimeInSeconds * 1000);
        }
        throw new RuntimeException("Model " + modelId + " failed to load after " + maxAttempts + " attempts");
    }
}
