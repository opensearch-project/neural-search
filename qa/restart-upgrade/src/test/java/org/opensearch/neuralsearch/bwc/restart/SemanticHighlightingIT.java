/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.util.List;
import java.util.Map;

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

    private static final String TEST_INDEX = "semantic-highlight-bwc-index";
    private static final String TEST_FIELD = "content";
    private static final String TEST_KNN_VECTOR_FIELD = "content_embedding";
    private static final String EMBEDDING_PIPELINE = "semantic-highlight-pipeline";
    private static final String HIGHLIGHT_MODEL_NAME = "sentence_highlighting_qa_model";

    // Test documents - medical research documents about neurodegenerative diseases
    private static final String DOC_1 =
        "Parkinson disease is a progressive neurodegenerative disorder characterized by synaptic loss and associated pathological changes. Clinical presentation typically includes cognitive decline and progressive functional decline. Current therapeutic approaches focus on cholinesterase inhibitors and supportive care interventions. Recent clinical trials have investigated novel treatments targeting underlying disease mechanisms, including anti-inflammatory agents, antioxidants, and disease-modifying therapies. Early intervention with cholinesterase inhibitors has shown promise in slowing disease progression and improving quality of life. Biomarker development and precision medicine approaches are advancing personalized treatment strategies. Multidisciplinary care teams provide comprehensive management including neurological assessment, rehabilitation services, and psychosocial support. Emerging therapies target specific molecular pathways involved in neurodegeneration, offering hope for more effective treatments in the future.";
    private static final String DOC_2 =
        "ALS disease is a progressive neurodegenerative disorder characterized by glial activation and associated pathological changes. Clinical presentation typically includes cognitive decline and progressive functional decline. Current therapeutic approaches focus on cognitive rehabilitation and supportive care interventions. Recent clinical trials have investigated novel treatments targeting underlying disease mechanisms, including anti-inflammatory agents, antioxidants, and disease-modifying therapies. Early intervention with cognitive rehabilitation has shown promise in slowing disease progression and improving quality of life. Biomarker development and precision medicine approaches are advancing personalized treatment strategies. Multidisciplinary care teams provide comprehensive management including neurological assessment, rehabilitation services, and psychosocial support. Emerging therapies target specific molecular pathways involved in neurodegeneration, offering hope for more effective treatments in the future.";
    private static final String DOC_3 =
        "Alzheimer disease is a progressive neurodegenerative disorder characterized by alpha-synuclein aggregation and associated pathological changes. Clinical presentation typically includes motor dysfunction and progressive functional decline. Current therapeutic approaches focus on gene therapy and supportive care interventions. Recent clinical trials have investigated novel treatments targeting underlying disease mechanisms, including anti-inflammatory agents, antioxidants, and disease-modifying therapies. Early intervention with gene therapy has shown promise in slowing disease progression and improving quality of life. Biomarker development and precision medicine approaches are advancing personalized treatment strategies. Multidisciplinary care teams provide comprehensive management including neurological assessment, rehabilitation services, and psychosocial support. Emerging therapies target specific molecular pathways involved in neurodegeneration, offering hope for more effective treatments in the future.";

    /**
     * Test semantic highlighting single inference mode (default) through restart upgrade.
     * This tests the feature that has existed since 3.0.0.
     */
    public void testSemanticHighlighting_SingleInferenceMode_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);

        if (isRunningAgainstOldCluster()) {
            setupOldCluster();
        } else {
            verifyNewCluster();
        }
    }

    private void setupOldCluster() throws Exception {
        // Deploy models using methods that exist in 3.0.0
        String highlightModelId = prepareSemanticHighlightingLocalModel();
        assertNotNull("Highlight model deployment failed", highlightModelId);

        String embeddingModelId = uploadTextEmbeddingModel();
        assertNotNull("Embedding model deployment failed", embeddingModelId);

        // Create pipeline and index
        createPipelineProcessor(embeddingModelId, EMBEDDING_PIPELINE);
        createIndexWithVectorMapping();
        indexDocuments();

        // Test single inference mode with match query
        verifySemanticHighlightingWithMatchQuery(highlightModelId);
    }

    private void verifyNewCluster() throws Exception {
        logger.info("=== Starting verifyNewCluster ===");

        // Retrieve model IDs from persisted cluster state
        logger.info("Retrieving embedding model ID from pipeline: {}", EMBEDDING_PIPELINE);
        String embeddingModelId = getModelId(getIngestionPipeline(EMBEDDING_PIPELINE), TEXT_EMBEDDING_PROCESSOR);
        logger.info("Retrieved embedding model ID: {}", embeddingModelId);

        logger.info("Retrieving highlight model ID by name: {}", HIGHLIGHT_MODEL_NAME);
        String highlightModelId = findModelIdByName(HIGHLIGHT_MODEL_NAME);
        logger.info("Retrieved highlight model ID: {}", highlightModelId);

        assertNotNull("Highlight model ID should be available from old cluster", highlightModelId);
        assertNotNull("Embedding model ID should be available from old cluster", embeddingModelId);

        try {
            // Ensure models are loaded
            loadAndWaitForModelToBeReady(highlightModelId);
            loadAndWaitForModelToBeReady(embeddingModelId);

            // Ensure index is refreshed
            refreshAllIndices();

            // Verify document count
            int docCount = getDocCount(TEST_INDEX);
            assertEquals("Document count mismatch after restart", 3, docCount);

            // Verify single inference mode still works (backward compatibility)
            verifySemanticHighlightingWithMatchQuery(highlightModelId);

            // Refresh before neural query to ensure KNN vectors are available
            refreshAllIndices();
            verifySemanticHighlightingWithNeuralQuery(highlightModelId, embeddingModelId);

            // Add new document and verify highlighting continues to work
            addDocument(
                TEST_INDEX,
                "4",
                TEST_FIELD,
                "Huntington disease is a progressive neurodegenerative disorder characterized by neuroinflammation and associated pathological changes. Clinical presentation typically includes rigidity and progressive functional decline. Current therapeutic approaches focus on cognitive rehabilitation and supportive care interventions.",
                null,
                null
            );

            // Refresh before querying new document
            refreshAllIndices();

            // Verify new document can be highlighted
            verifyHighlightingWithNewDocument(highlightModelId);

        } finally {
            // Clean up test resources - no search pipeline in this test
            try {
                wipeOfTestResources(TEST_INDEX, EMBEDDING_PIPELINE, embeddingModelId, null);
            } catch (Exception e) {
                logger.warn("Error during cleanup, continuing: {}", e.getMessage());
            }
            // Manually delete highlight model since wipeOfTestResources only handles one model
            if (highlightModelId != null) {
                try {
                    deleteModel(highlightModelId);
                } catch (Exception e) {
                    logger.warn("Error deleting highlight model, continuing: {}", e.getMessage());
                }
            }
        }
    }

    private void createIndexWithVectorMapping() throws Exception {
        // Create index with both text and KNN vector fields
        String mapping = String.format(LOCALE, """
            {
                "settings": {
                    "index.knn": true,
                    "default_pipeline": "%s"
                },
                "mappings": {
                    "properties": {
                        "%s": {"type": "text"},
                        "%s": {
                            "type": "knn_vector",
                            "dimension": 768,
                            "method": {
                                "name": "hnsw",
                                "engine": "lucene"
                            }
                        }
                    }
                }
            }
            """, EMBEDDING_PIPELINE, TEST_FIELD, TEST_KNN_VECTOR_FIELD);

        createIndexWithConfiguration(TEST_INDEX, mapping, null);
    }

    private void indexDocuments() throws Exception {
        addDocument(TEST_INDEX, "1", TEST_FIELD, DOC_1, null, null);
        addDocument(TEST_INDEX, "2", TEST_FIELD, DOC_2, null, null);
        addDocument(TEST_INDEX, "3", TEST_FIELD, DOC_3, null, null);

        // Refresh index to ensure documents are searchable immediately
        refreshAllIndices();
    }

    private void verifySemanticHighlightingWithMatchQuery(String modelId) throws Exception {
        QueryBuilder query = new MatchQueryBuilder(TEST_FIELD, "neurodegenerative disorder treatment");

        // Use single inference mode (default - no batch_inference flag)
        Map<String, Object> response = searchWithSemanticHighlighter(TEST_INDEX, query, 2, TEST_FIELD, modelId);

        assertNotNull("Search response should not be null", response);
        assertHighlightingPresent(response, TEST_FIELD);
    }

    private void verifySemanticHighlightingWithNeuralQuery(String highlightModelId, String embeddingModelId) throws Exception {

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("neurodegenerative disease therapeutic approaches")
            .modelId(embeddingModelId)
            .k(2)
            .build();

        // Use single inference mode (default - no batch_inference flag)
        Map<String, Object> response = searchWithSemanticHighlighter(TEST_INDEX, neuralQuery, 2, TEST_FIELD, highlightModelId);

        assertNotNull("Neural search response should not be null", response);
        assertHighlightingPresent(response, TEST_FIELD);
    }

    private void verifyHighlightingWithNewDocument(String modelId) throws Exception {
        QueryBuilder query = new MatchQueryBuilder(TEST_FIELD, "cognitive rehabilitation");

        Map<String, Object> response = searchWithSemanticHighlighter(TEST_INDEX, query, 2, TEST_FIELD, modelId);

        assertNotNull("Search response for new document should not be null", response);
        assertHighlightingPresent(response, TEST_FIELD);
    }

    @SuppressWarnings("unchecked")
    private void assertHighlightingPresent(Map<String, Object> response, String field) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        assertNotNull("Hits should not be null", hits);

        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        assertNotNull("Hits list should not be null", hitsList);
        assertTrue("Should have at least one hit", hitsList.size() > 0);

        boolean foundHighlight = false;
        for (Map<String, Object> hit : hitsList) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(field)) {
                List<String> fragments = (List<String>) highlight.get(field);
                if (fragments != null && !fragments.isEmpty()) {
                    // Verify highlight structure (check for HTML tags)
                    for (String fragment : fragments) {
                        // Check that highlight tags are present
                        assertTrue("Fragment should contain opening tag '<em>' in: " + fragment, fragment.contains("<em>"));
                        assertTrue("Fragment should contain closing tag '</em>' in: " + fragment, fragment.contains("</em>"));
                    }
                    foundHighlight = true;
                    break;
                }
            }
        }

        assertTrue("Semantic highlighting should produce highlights with HTML tags for field: " + field, foundHighlight);
    }
}
