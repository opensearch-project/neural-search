/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_IMAGE_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class MultiModalSearchIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEST_IMAGE_FIELD = "passage_image";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_IMAGE_TEXT_1 = "/9j/4AAQSkZJRgbdwoeicfhoid";

    // Test restart-upgrade test image embedding processor
    // Create Text Image Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in restart-upgrade scenario
    public void testTextImageEmbeddingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            String modelId = uploadTextEmbeddingModel();
            createPipelineForTextImageProcessor(modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                getIndexNameForTest(),
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT);
        } else {
            String modelId = null;
            try {
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_IMAGE_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_1, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_1);
                validateTestIndex(modelId);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
            }
        }
    }

    private void validateTestIndex(final String modelId) throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(2, docCount);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(TEXT)
            .queryImage(TEST_IMAGE_TEXT)
            .modelId(modelId)
            .k(1)
            .build();
        Map<String, Object> response = search(getIndexNameForTest(), neuralQueryBuilder, 1);
        assertNotNull(response);
    }

}
