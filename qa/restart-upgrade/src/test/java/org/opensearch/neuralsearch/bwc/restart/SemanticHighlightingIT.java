/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

/**
 * BWC test for semantic highlighting with local models during restart upgrade.
 * Tests backward compatibility of single inference mode (default) from OpenSearch 3.0.0+
 */
public class SemanticHighlightingIT extends AbstractRestartUpgradeRestTestCase {

    private static final String PIPELINE_NAME = "semantic-highlight-pipeline";
    private static final String TEST_FIELD = "text";
    private static final String TEST_KNN_VECTOR_FIELD = "text_knn";
    private static final String HIGHLIGHT_MODEL_NAME = "sentence_highlighting_qa_model";
    private static final String INDEX_MAPPING_PATH = "processor/SemanticHighlightingIndexMapping.json";

    private static final String DOC_OLD = "Parkinson disease is a progressive neurodegenerative disorder.";
    private static final String DOC_NEW = "Alzheimer disease is a progressive neurodegenerative disorder.";

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    /**
     * Test semantic highlighting through restart upgrade.
     * Follows the same pattern as SemanticSearchIT.
     */
    public void testSemanticHighlighting_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            // Deploy models and create resources in old cluster
            String highlightModelId = prepareSemanticHighlightingLocalModel();
            String embeddingModelId = uploadTextEmbeddingModel();
            createPipelineProcessor(embeddingModelId, PIPELINE_NAME);

            URL indexMappingURL = classLoader.getResource(INDEX_MAPPING_PATH);
            Objects.requireNonNull(indexMappingURL, "Index mapping file not found: " + INDEX_MAPPING_PATH);
            String indexMapping = Files.readString(Path.of(indexMappingURL.toURI()));
            createIndexWithConfiguration(getIndexNameForTest(), indexMapping, PIPELINE_NAME);

            addDocument(getIndexNameForTest(), "0", TEST_FIELD, DOC_OLD, null, null);
        } else {
            // Validate in new cluster after restart
            String embeddingModelId = null;
            String highlightModelId = null;
            try {
                embeddingModelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                highlightModelId = findModelIdByName(HIGHLIGHT_MODEL_NAME);

                loadAndWaitForModelToBeReady(embeddingModelId);
                loadAndWaitForModelToBeReady(highlightModelId);

                addDocument(getIndexNameForTest(), "1", TEST_FIELD, DOC_NEW, null, null);
                validateSemanticHighlighting(highlightModelId, embeddingModelId);
            } finally {
                wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, embeddingModelId, null);
                if (highlightModelId != null) {
                    try {
                        deleteModel(highlightModelId);
                    } catch (Exception e) {
                        logger.warn("Error deleting highlight model: {}", e.getMessage());
                    }
                }
            }
        }
    }

    private void validateSemanticHighlighting(final String highlightModelId, final String embeddingModelId) throws Exception {
        // Verify document count
        int docCount = getDocCount(getIndexNameForTest());
        logger.info("=== Validation Start (New Cluster) ===");
        logger.info("Document count in index {}: {}", getIndexNameForTest(), docCount);
        logger.info("Expected doc count: 2");
        logger.info("Highlight model ID: {}", highlightModelId);
        logger.info("Embedding model ID: {}", embeddingModelId);
        assertEquals(2, docCount);

        // Refresh index to ensure all documents are searchable
        refreshAllIndices();
        logger.info("Index refreshed");

        // Check if documents have KNN vectors by fetching a sample document
        try {
            Map<String, Object> sampleDoc = search(getIndexNameForTest(), new MatchQueryBuilder(TEST_FIELD, "disease"), 1);
            logger.info("Sample document for KNN vector check: {}", sampleDoc);
        } catch (Exception e) {
            logger.warn("Failed to fetch sample document: {}", e.getMessage());
        }

        // Test with match query and semantic highlighting
        logger.info("=== Testing Match Query with Semantic Highlighting ===");
        QueryBuilder matchQuery = new MatchQueryBuilder(TEST_FIELD, "neurodegenerative disorder");
        Map<String, Object> matchResponse = searchWithSemanticHighlighter(
            getIndexNameForTest(),
            matchQuery,
            2,
            TEST_FIELD,
            highlightModelId
        );
        logger.info("Match query response: {}", matchResponse);
        assertNotNull(matchResponse);
        assertHighlightsPresent(matchResponse);
        logger.info("Match query validation PASSED");

        // Refresh again before neural query
        refreshAllIndices();
        logger.info("Index refreshed before neural query");

        // Test with neural query and semantic highlighting
        logger.info("=== Testing Neural Query with Semantic Highlighting ===");
        logger.info("Neural query field: {}", TEST_KNN_VECTOR_FIELD);
        logger.info("Neural query text: progressive disease treatment");
        logger.info("Neural query k: 2");

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("progressive disease treatment")
            .modelId(embeddingModelId)
            .k(2)
            .build();

        Map<String, Object> neuralResponse = searchWithSemanticHighlighter(
            getIndexNameForTest(),
            neuralQuery,
            2,
            TEST_FIELD,
            highlightModelId
        );
        logger.info("Neural query response: {}", neuralResponse);
        assertNotNull(neuralResponse);
        assertHighlightsPresent(neuralResponse);
        logger.info("Neural query validation PASSED");
        logger.info("=== Validation Complete ===");
    }

    @SuppressWarnings("unchecked")
    private void assertHighlightsPresent(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertNotNull("Hits should not be null", hits);
        // log the hits for debugging
        logger.info("Search hits: {}", hits);

        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        assertNotNull("Hits list should not be null", hitsList);
        assertTrue("Should have at least one hit", !hitsList.isEmpty());

        // At least one document should have highlights with tags
        boolean foundHighlightWithTags = false;
        for (Map<String, Object> hit : hitsList) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(TEST_FIELD)) {
                List<String> fragments = (List<String>) highlight.get(TEST_FIELD);
                if (fragments != null && !fragments.isEmpty()) {
                    // Verify that at least one fragment contains highlight tags
                    for (String fragment : fragments) {
                        if (fragment.contains("<em>") && fragment.contains("</em>")) {
                            foundHighlightWithTags = true;
                            break;
                        }
                    }
                    if (foundHighlightWithTags) {
                        break;
                    }
                }
            }
        }
        assertTrue("At least one document should have highlights with <em> tags", foundHighlightWithTags);
    }
}
