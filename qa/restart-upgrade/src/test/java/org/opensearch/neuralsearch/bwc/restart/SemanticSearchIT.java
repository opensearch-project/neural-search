/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.util.TextEmbeddingModel;

public class SemanticSearchIT extends AbstractRestartUpgradeRestTestCase {

    private static final String PIPELINE_NAME = "nlp-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";

    // Test restart-upgrade Semantic Search
    // Create Text Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testTextEmbeddingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, null, null);
        } else {
            String modelId = null;
            try {
                modelId = TextEmbeddingModel.getInstance().getModelId();
                loadModel(modelId);
                addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_1, null, null);
                validateTestIndex(modelId);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, null, null);
            }
        }
    }

    private void validateTestIndex(final String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(2, docCount);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(TEXT)
            .modelId(modelId)
            .k(1)
            .build();
        Map<String, Object> response = search(getIndexNameForTest(), neuralQueryBuilder, 1);
        assertNotNull(response);
    }
}
