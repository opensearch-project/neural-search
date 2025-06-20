/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_IMAGE_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class KnnRadialSearchIT extends AbstractRollingUpgradeTestCase {
    private static final String PIPELINE_NAME = "radial-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String TEST_IMAGE_FIELD = "passage_image";
    private static final String TEXT = "Hello world";
    private static final String TEXT_MIXED = "Hello world mixed";
    private static final String TEXT_UPGRADED = "Hello world upgraded";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_IMAGE_TEXT_MIXED = "/9j/4AAQSkZJRgbdwoeicfhoid";
    private static final String TEST_IMAGE_TEXT_UPGRADED = "/9j/4AAQSkZJR8eydhgfwceocvlk";

    private static final int NUM_DOCS_PER_ROUND = 1;
    private static String modelId = "";

    // Test rolling-upgrade with kNN radial search
    // Create Text Image Embedding Processor, Ingestion Pipeline and add document
    // Validate radial query, pipeline and document count in rolling-upgrade scenario
    public void testKnnRadialSearch_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextImageEmbeddingModel();
                loadModel(modelId);
                createPipelineForTextImageProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(
                    getIndexNameForTest(),
                    Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
                    PIPELINE_NAME
                );
                addDocument(getIndexNameForTest(), "0", TEST_FIELD, TEXT, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT);
                break;
            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_IMAGE_EMBEDDING_PROCESSOR);
                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateIndexQueryOnUpgrade(totalDocsCountMixed, modelId, TEXT, TEST_IMAGE_TEXT);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, TEXT_MIXED, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_MIXED);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateIndexQueryOnUpgrade(totalDocsCountMixed, modelId, TEXT_MIXED, TEST_IMAGE_TEXT_MIXED);
                }
                break;
            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_IMAGE_EMBEDDING_PROCESSOR);
                    int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                    loadModel(modelId);
                    addDocument(getIndexNameForTest(), "2", TEST_FIELD, TEXT_UPGRADED, TEST_IMAGE_FIELD, TEST_IMAGE_TEXT_UPGRADED);
                    validateIndexQueryOnUpgrade(totalDocsCountUpgraded, modelId, TEXT_UPGRADED, TEST_IMAGE_TEXT_UPGRADED);
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, null);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    private void validateIndexQueryOnUpgrade(final int numberOfDocs, final String modelId, final String text, final String imageText)
        throws Exception {
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(numberOfDocs, docCount);
        loadModel(modelId);

        // Test 1: Neural query with k parameter (standard k-NN search)
        NeuralQueryBuilder neuralQueryBuilderWithK = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(text)
            .queryImage(imageText)
            .modelId(modelId)
            .k(5)
            .build();

        Map<String, Object> responseWithK = search(getIndexNameForTest(), neuralQueryBuilderWithK, 10);
        validateSearchResponse(responseWithK, "k-NN search", numberOfDocs);

        // Test 2: Radial search with minScore
        NeuralQueryBuilder neuralQueryBuilderWithMinScoreQuery = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(text)
            .queryImage(imageText)
            .modelId(modelId)
            .minScore(0.01f)
            .build();

        Map<String, Object> responseWithMinScore = search(getIndexNameForTest(), neuralQueryBuilderWithMinScoreQuery, 10);
        validateSearchResponse(responseWithMinScore, "radial search with minScore", numberOfDocs);

        // Test 3: Radial search with maxDistance
        NeuralQueryBuilder neuralQueryBuilderWithMaxDistanceQuery = NeuralQueryBuilder.builder()
            .fieldName("passage_embedding")
            .queryText(text)
            .queryImage(imageText)
            .modelId(modelId)
            .maxDistance(100000f)
            .build();

        Map<String, Object> responseWithMaxDistance = search(getIndexNameForTest(), neuralQueryBuilderWithMaxDistanceQuery, 10);
        validateSearchResponse(responseWithMaxDistance, "radial search with maxDistance", numberOfDocs);
    }

    private void validateSearchResponse(Map<String, Object> response, String queryType, int expectedMinDocs) {
        assertNotNull("Response should not be null for " + queryType, response);

        // Check for errors
        assertFalse("Response should not contain errors for " + queryType, response.containsKey("error"));

        // Verify shard information - critical for multi-node BWC tests
        Map<String, Object> shards = (Map<String, Object>) response.get("_shards");
        assertNotNull("Shards info should not be null for " + queryType, shards);

        int totalShards = (int) shards.get("total");
        int successfulShards = (int) shards.get("successful");
        int failedShards = (int) shards.get("failed");

        assertEquals(
            String.format(
                Locale.ROOT,
                "All shards should be successful for %s. Total: %d, Successful: %d, Failed: %d",
                queryType,
                totalShards,
                successfulShards,
                failedShards
            ),
            totalShards,
            successfulShards
        );
        assertEquals(String.format(Locale.ROOT, "No shards should fail for %s. Failed: %d", queryType, failedShards), 0, failedShards);

        // Verify hits structure
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertNotNull("Hits should not be null for " + queryType, hits);

        // Check total hits - handle both old and new response formats
        Object totalObj = hits.get("total");
        int totalHits;
        if (totalObj instanceof Map) {
            // New format: {"value": N, "relation": "eq"}
            Map<String, Object> total = (Map<String, Object>) totalObj;
            totalHits = (int) total.get("value");
        } else {
            // Old format: just the number
            totalHits = (int) totalObj;
        }

        assertTrue(
            String.format(Locale.ROOT, "Should find at least one document for %s, but found %d", queryType, totalHits),
            totalHits > 0
        );

        // Verify actual documents returned
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        assertNotNull("Hits list should not be null for " + queryType, hitsList);
        assertFalse("Should return actual documents for " + queryType, hitsList.isEmpty());

        // Log for debugging
        logger.info(
            "Validated {} - Shards: {}/{} successful, {} failed - Found {} total hits, returned {} documents",
            queryType,
            successfulShards,
            totalShards,
            failedShards,
            totalHits,
            hitsList.size()
        );
    }
}
