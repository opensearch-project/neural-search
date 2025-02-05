/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class SemanticSearchIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline-text-embedding";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final int NUM_DOCS_PER_ROUND = 1;
    private static String modelId = "";

    // Test rolling-upgrade Semantic Search
    // Create Text Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in rolling-upgrade scenario
    public void testSemanticSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        switch (getClusterType()) {
            case OLD:
                modelId = getOrUploadTextEmbeddingModel(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadModel(modelId);
                createPipelineProcessor(modelId, PIPELINE_NAME, TEXT_EMBEDDING_PROCESSOR);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
                break;
            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, TEXT);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_MIXED, null, null);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, modelId, TEXT_MIXED);
                }
                break;
            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                    loadModel(modelId);
                    addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_UPGRADED, null, null);
                    validateTestIndexOnUpgrade(totalDocsCountUpgraded, modelId, TEXT_UPGRADED);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }

    }

    private void validateTestIndexOnUpgrade(final int numberOfDocs, final String modelId, final String text) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        loadModel(modelId);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .modelId(modelId)
            .queryText(text)
            .k(1)
            .build();
        Map<String, Object> response = search(getIndexNameForTest(), neuralQueryBuilder, 1);
        assertNotNull(response);
    }
}
