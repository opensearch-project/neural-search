/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

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
 * BWC test for semantic highlighting with local models during rolling upgrade.
 * Tests backward compatibility of single inference mode (default) from OpenSearch 3.0.0+
 */
public class SemanticHighlightingIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "semantic-highlight-pipeline-rolling";
    private static final String TEST_FIELD = "text";
    private static final String TEST_KNN_VECTOR_FIELD = "text_knn";
    private static final String HIGHLIGHT_MODEL_NAME = "sentence_highlighting_qa_model";
    private static final String INDEX_MAPPING_PATH = "processor/SemanticHighlightingIndexMapping.json";

    private static final String DOC_OLD = "Parkinson disease is a progressive neurodegenerative disorder.";
    private static final String DOC_MIXED = "ALS disease is a progressive neurodegenerative disorder.";
    private static final String DOC_UPGRADED = "Alzheimer disease is a progressive neurodegenerative disorder.";

    private static final int NUM_DOCS_PER_ROUND = 1;
    private static String embeddingModelId = "";
    private static String highlightModelId = "";

    private final ClassLoader classLoader = this.getClass().getClassLoader();

    /**
     * Test semantic highlighting through rolling upgrade.
     * Follows the same pattern as SemanticSearchIT.
     */
    public void testSemanticHighlighting_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER, 90);

        switch (getClusterType()) {
            case OLD:
                // Deploy models and create resources in old cluster
                highlightModelId = prepareSemanticHighlightingLocalModel();
                embeddingModelId = uploadTextEmbeddingModel();
                createPipelineProcessor(embeddingModelId, PIPELINE_NAME);

                URL indexMappingURL = classLoader.getResource(INDEX_MAPPING_PATH);
                Objects.requireNonNull(indexMappingURL, "Index mapping file not found: " + INDEX_MAPPING_PATH);
                String indexMapping = Files.readString(Path.of(indexMappingURL.toURI()));
                createIndexWithConfiguration(getIndexNameForTest(), indexMapping, PIPELINE_NAME);

                addDocument(getIndexNameForTest(), "0", TEST_FIELD, DOC_OLD, null, null);
                break;

            case MIXED:
                // Retrieve model IDs and validate in mixed cluster
                embeddingModelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                highlightModelId = findModelIdByName(HIGHLIGHT_MODEL_NAME);

                loadAndWaitForModelToBeReady(embeddingModelId);
                loadAndWaitForModelToBeReady(highlightModelId);

                int totalDocsCountMixed;
                if (isFirstMixedRound()) {
                    totalDocsCountMixed = NUM_DOCS_PER_ROUND;
                    validateSemanticHighlighting(totalDocsCountMixed, highlightModelId, embeddingModelId, DOC_OLD);
                    addDocument(getIndexNameForTest(), "1", TEST_FIELD, DOC_MIXED, null, null);
                } else {
                    totalDocsCountMixed = 2 * NUM_DOCS_PER_ROUND;
                    validateSemanticHighlighting(totalDocsCountMixed, highlightModelId, embeddingModelId, DOC_MIXED);
                }
                break;

            case UPGRADED:
                try {
                    // Validate and cleanup in upgraded cluster
                    embeddingModelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    highlightModelId = findModelIdByName(HIGHLIGHT_MODEL_NAME);

                    int totalDocsCountUpgraded = 3 * NUM_DOCS_PER_ROUND;
                    loadAndWaitForModelToBeReady(embeddingModelId);
                    loadAndWaitForModelToBeReady(highlightModelId);

                    addDocument(getIndexNameForTest(), "2", TEST_FIELD, DOC_UPGRADED, null, null);
                    validateSemanticHighlighting(totalDocsCountUpgraded, highlightModelId, embeddingModelId, DOC_UPGRADED);
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
                break;

            default:
                throw new IllegalStateException("Unexpected cluster type: " + getClusterType());
        }
    }

    private void validateSemanticHighlighting(
        final int expectedDocCount,
        final String highlightModelId,
        final String embeddingModelId,
        final String searchText
    ) throws Exception {
        // Verify document count
        int docCount = getDocCount(getIndexNameForTest());
        assertEquals(expectedDocCount, docCount);

        // Test with match query and semantic highlighting
        QueryBuilder matchQuery = new MatchQueryBuilder(TEST_FIELD, "neurodegenerative disorder");
        Map<String, Object> matchResponse = searchWithSemanticHighlighter(
            getIndexNameForTest(),
            matchQuery,
            expectedDocCount,
            TEST_FIELD,
            highlightModelId
        );
        assertNotNull(matchResponse);
        assertHighlightsPresent(matchResponse);

        // Test with neural query and semantic highlighting
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("progressive disease treatment")
            .modelId(embeddingModelId)
            .k(expectedDocCount)
            .build();

        Map<String, Object> neuralResponse = searchWithSemanticHighlighter(
            getIndexNameForTest(),
            neuralQuery,
            expectedDocCount,
            TEST_FIELD,
            highlightModelId
        );
        assertNotNull(neuralResponse);
        assertHighlightsPresent(neuralResponse);
    }

    @SuppressWarnings("unchecked")
    private void assertHighlightsPresent(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertNotNull("Hits should not be null", hits);

        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        assertNotNull("Hits list should not be null", hitsList);
        assertTrue("Should have at least one hit", !hitsList.isEmpty());

        // At least one document should have highlights
        boolean foundHighlight = false;
        for (Map<String, Object> hit : hitsList) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(TEST_FIELD)) {
                List<String> fragments = (List<String>) highlight.get(TEST_FIELD);
                if (fragments != null && !fragments.isEmpty()) {
                    foundHighlight = true;
                    break;
                }
            }
        }
        assertTrue("At least one document should have highlights", foundHighlight);
    }
}
