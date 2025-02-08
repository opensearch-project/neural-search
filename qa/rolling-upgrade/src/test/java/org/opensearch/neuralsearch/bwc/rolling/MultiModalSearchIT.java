/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_IMAGE_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class MultiModalSearchIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "nlp-ingest-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEST_IMAGE_FIELD = "passage_image";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_IMAGE_TEXT_MIXED = "/9j/4AAQSkZJRgbdwoeicfhoid";
    private static final String TEST_IMAGE_TEXT_UPGRADED = "/9j/4AAQSkZJR8eydhgfwceocvlk";

    private static final int NUM_DOCS_PER_ROUND = 1;

    // Test rolling-upgrade test image embedding processor
    // Create Text Image Embedding Processor, Ingestion Pipeline and add document
    // Validate process , pipeline and document count in rolling-upgrade scenario
    public void testTextImageEmbeddingProcessor_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);
        super.ingestPipelineName = PIPELINE_NAME;

        switch (getClusterType()) {
            case OLD:
                super.modelId = uploadTextImageEmbeddingModel();
                loadModel(super.modelId);
                createPipelineForTextImageProcessor(super.modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    super.indexName,
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(super.indexName, "0", TEST_FIELD, TEXT, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT);
                break;
            case MIXED:
                super.modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_IMAGE_EMBEDDING_PROCESSOR);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, super.modelId, TEXT, TEST_IMAGE_TEXT);
                    addDocument(super.indexName, "1", TEST_FIELD, TEXT_MIXED, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_MIXED);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateTestIndexOnUpgrade(totalDocsCountMixed, super.modelId, TEXT_MIXED, TEST_IMAGE_TEXT_MIXED);
                }
                break;
            case UPGRADED:
                super.modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_IMAGE_EMBEDDING_PROCESSOR);
                int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                loadModel(super.modelId);
                addDocument(super.indexName, "2", TEST_FIELD, TEXT_UPGRADED, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_UPGRADED);
                validateTestIndexOnUpgrade(totalDocsCountUpgraded, super.modelId, TEXT_UPGRADED, TEST_IMAGE_TEXT_UPGRADED);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateTestIndexOnUpgrade(final int numberOfDocs, final String modelId, final String text, final String imageText)
        throws Exception {
        int docCount = getDocCount(super.indexName);
        assertEquals(numberOfDocs, docCount);
        loadModel(modelId);
        NeuralQueryBuilder neuralQueryBuilderWithKQuery = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(text)
            .queryImage(imageText)
            .modelId(modelId)
            .k(1)
            .build();

        Map<String, Object> responseWithKQuery = search(super.indexName, neuralQueryBuilderWithKQuery, 1);
        assertNotNull(responseWithKQuery);
    }
}
