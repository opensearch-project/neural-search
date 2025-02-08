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

public class KnnRadialSearchIT extends AbstractRestartUpgradeRestTestCase {
    private static final String PIPELINE_NAME = "radial-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEST_IMAGE_FIELD = "passage_image";
    private static final String TEXT = "Hello world";
    private static final String TEXT_1 = "Hello world a";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_IMAGE_TEXT_1 = "/9j/4AAQSkZJRgbdwoeicfhoid";

    // Test rolling-upgrade with kNN radial search
    // Create Text Image Embedding Processor, Ingestion Pipeline and add document
    // Validate radial query, pipeline and document count in restart-upgrade scenario
    public void testKnnRadialSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        super.ingestPipelineName = PIPELINE_NAME;

        if (isRunningAgainstOldCluster()) {
            super.modelId = uploadTextEmbeddingModel();
            loadModel(super.modelId);
            createPipelineForTextImageProcessor(super.modelId, PIPELINE_NAME);
            createIndexWithConfiguration(
                super.indexName,
                Files.readString(Path.of(classLoader.getResource("processor/IndexMappingMultipleShard.json").toURI())),
                PIPELINE_NAME
            );
            addDocument(super.indexName, "0", TEST_FIELD, TEXT, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT);
        } else {
            super.modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_IMAGE_EMBEDDING_PROCESSOR);

            loadModel(super.modelId);
            addDocument(super.indexName, "1", TEST_FIELD, TEXT_1, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_1);
            validateIndexQuery(super.modelId);
        }
    }

    private void validateIndexQuery(final String modelId) {
        NeuralQueryBuilder neuralQueryBuilderWithMinScoreQuery = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(TEXT)
            .queryImage(TEST_IMAGE_TEXT)
            .modelId(modelId)
            .minScore(0.01f)
            .build();

        Map<String, Object> responseWithMinScoreQuery = search(super.indexName, neuralQueryBuilderWithMinScoreQuery, 1);
        assertNotNull(responseWithMinScoreQuery);

        NeuralQueryBuilder neuralQueryBuilderWithMaxDistanceQuery = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(TEXT)
            .queryImage(TEST_IMAGE_TEXT)
            .modelId(modelId)
            .maxDistance(100000f)
            .build();
        Map<String, Object> responseWithMaxDistanceQuery = search(super.indexName, neuralQueryBuilderWithMaxDistanceQuery, 1);
        assertNotNull(responseWithMaxDistanceQuery);
    }
}
