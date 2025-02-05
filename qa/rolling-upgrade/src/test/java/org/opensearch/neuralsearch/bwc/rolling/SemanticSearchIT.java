/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class SemanticSearchIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final int NUM_DOCS_PER_ROUND = 1;

    // Test rolling-upgrade Semantic Search
    // Create Text Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in rolling-upgrade scenario
    public void testSemanticSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        switch (getClusterType()) {
            case OLD:
                loadModel(textEmbeddingModelId);
                createPipelineProcessor(textEmbeddingModelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
                break;
            case MIXED:
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, textEmbeddingModelId, TEXT);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_MIXED, null, null);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, textEmbeddingModelId, TEXT_MIXED);
                }
                break;
            case UPGRADED:
                try {
                    int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                    loadModel(textEmbeddingModelId);
                    addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_UPGRADED, null, null);
                    validateTestIndexOnUpgrade(totalDocsCountUpgraded, textEmbeddingModelId, TEXT_UPGRADED);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, null, null);
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
