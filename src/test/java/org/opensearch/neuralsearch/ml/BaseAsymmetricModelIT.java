/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;

/**
 * Base class for Asymmetric text embedding model integration tests.
 * Provides common setup, teardown, and utility methods for both local and remote model testing.
 */
@Log4j2
public abstract class BaseAsymmetricModelIT extends BaseNeuralSearchIT {

    protected static final String TEST_FIELD = "content";
    protected static final String TEST_KNN_VECTOR_FIELD = "content_embedding";
    protected static final int TEST_DIMENSION_LOCAL = 768;  // Local model dimension
    protected static final int TEST_DIMENSION_REMOTE = 384; // Remote model dimension
    protected static final String TEXT_EMBEDDING_PIPELINE = "test-asymmetric-embedding-pipeline";

    protected String asymmetricModelId;
    protected int testDimension = TEST_DIMENSION_LOCAL; // Default to local model dimension

    @Before
    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        updateMLCommonsSettings();
    }

    @After
    @SneakyThrows
    @Override
    public void tearDown() {
        // Delete text embedding pipeline
        try {
            Request request = new Request("DELETE", "/_ingest/pipeline/" + TEXT_EMBEDDING_PIPELINE);
            client().performRequest(request);
        } catch (Exception e) {
            log.debug("Failed to delete pipeline: {}", e.getMessage());
        }

        // Cleanup asymmetric model
        try {
            if (asymmetricModelId != null) {
                deleteModel(asymmetricModelId);
            }
        } catch (Exception e) {
            log.debug("Failed to delete asymmetric model: {}", e.getMessage());
        }

        super.tearDown();
    }

    /**
     * Update ML Commons settings for asymmetric model tests
     */
    @SneakyThrows
    protected void updateMLCommonsSettings() {
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        updateClusterSettings("plugins.ml_commons.connector.private_ip_enabled", true);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
        updateClusterSettings(
            "plugins.ml_commons.trusted_connector_endpoints_regex",
            List.of(
                "^https://runtime\\.sagemaker\\..*[a-z0-9-]\\.amazonaws\\.com/.*$",
                "^http://localhost:.*",
                "^http://127\\.0\\.0\\.1:.*",
                "^http://torchserve:.*"
            )
        );
    }

    /**
     * Prepare a KNN index for asymmetric model tests
     */
    @SneakyThrows
    protected void prepareAsymmetricIndex(String indexName) {
        prepareKnnIndex(indexName, Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD, testDimension, TEST_SPACE_TYPE)));
    }

    /**
     * Create text embedding pipeline for indexing documents with asymmetric model
     */
    @SneakyThrows
    protected void createAsymmetricEmbeddingPipeline() {
        if (asymmetricModelId == null) {
            return;
        }

        XContentBuilder pipelineBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("processors")
            .startObject()
            .startObject("text_embedding")
            .field("model_id", asymmetricModelId)
            .startObject("field_map")
            .field(TEST_FIELD, TEST_KNN_VECTOR_FIELD)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        Request createPipelineRequest = new Request("PUT", "/_ingest/pipeline/" + TEXT_EMBEDDING_PIPELINE);
        createPipelineRequest.setJsonEntity(pipelineBuilder.toString());
        Response pipelineResponse = client().performRequest(createPipelineRequest);
        assertEquals(200, pipelineResponse.getStatusLine().getStatusCode());
    }

    /**
     * Index a document with the asymmetric embedding pipeline
     */
    @SneakyThrows
    protected void addKnnDocWithAsymmetricPipeline(String indexName, String docId, String fieldName, String content, String pipeline) {
        Request request = new Request("PUT", "/" + indexName + "/_doc/" + docId + "?pipeline=" + pipeline + "&refresh=true");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field(fieldName, content).endObject();
        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    /**
     * Index a document with the asymmetric embedding pipeline using the default pipeline name
     */
    @SneakyThrows
    protected void addKnnDocWithAsymmetricPipeline(String indexName, String docId, String content) {
        addKnnDocWithAsymmetricPipeline(indexName, docId, TEST_FIELD, content, TEXT_EMBEDDING_PIPELINE);
    }

    /**
     * Index test documents for asymmetric model tests
     * Uses diverse content to test asymmetric query vs passage embeddings
     */
    @SneakyThrows
    protected void indexAsymmetricTestDocuments(String indexName) {
        if (asymmetricModelId != null) {
            // Index documents with pipeline for neural search support
            createAsymmetricEmbeddingPipeline();

            addKnnDocWithAsymmetricPipeline(
                indexName,
                "1",
                "Machine learning algorithms are computational methods that enable computers to learn patterns from data without being explicitly programmed. These algorithms form the foundation of artificial intelligence systems and are used in various applications including natural language processing, computer vision, and predictive analytics."
            );
            addKnnDocWithAsymmetricPipeline(
                indexName,
                "2",
                "Deep learning is a subset of machine learning that uses neural networks with multiple layers to model and understand complex patterns in data. Deep neural networks have revolutionized fields like image recognition, speech processing, and language understanding by automatically learning hierarchical representations."
            );
            addKnnDocWithAsymmetricPipeline(
                indexName,
                "3",
                "Natural language processing (NLP) combines computational linguistics with machine learning to help computers understand, interpret, and generate human language. NLP techniques are essential for applications like chatbots, translation systems, sentiment analysis, and text summarization."
            );
            addKnnDocWithAsymmetricPipeline(
                indexName,
                "4",
                "Computer vision enables machines to interpret and understand visual information from the world. Using deep learning models, computer vision systems can perform tasks like object detection, facial recognition, medical image analysis, and autonomous vehicle navigation."
            );
            addKnnDocWithAsymmetricPipeline(
                indexName,
                "5",
                "Artificial intelligence encompasses various technologies that enable machines to perform tasks that typically require human intelligence. AI systems combine machine learning, natural language processing, computer vision, and robotics to solve complex problems across industries."
            );
        } else {
            // Index documents without pipeline for basic match query tests
            addKnnDoc(
                indexName,
                "1",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Machine learning algorithms are computational methods that enable computers to learn patterns from data without being explicitly programmed. These algorithms form the foundation of artificial intelligence systems and are used in various applications including natural language processing, computer vision, and predictive analytics."
                )
            );
            addKnnDoc(
                indexName,
                "2",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Deep learning is a subset of machine learning that uses neural networks with multiple layers to model and understand complex patterns in data. Deep neural networks have revolutionized fields like image recognition, speech processing, and language understanding by automatically learning hierarchical representations."
                )
            );
            addKnnDoc(
                indexName,
                "3",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Natural language processing (NLP) combines computational linguistics with machine learning to help computers understand, interpret, and generate human language. NLP techniques are essential for applications like chatbots, translation systems, sentiment analysis, and text summarization."
                )
            );
            addKnnDoc(
                indexName,
                "4",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Computer vision enables machines to interpret and understand visual information from the world. Using deep learning models, computer vision systems can perform tasks like object detection, facial recognition, medical image analysis, and autonomous vehicle navigation."
                )
            );
            addKnnDoc(
                indexName,
                "5",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Artificial intelligence encompasses various technologies that enable machines to perform tasks that typically require human intelligence. AI systems combine machine learning, natural language processing, computer vision, and robotics to solve complex problems across industries."
                )
            );
        }
    }

    @SneakyThrows
    protected String prepareAsymmetricModel() {
        String requestBody = Files.readString(
            Path.of(Objects.requireNonNull(classLoader.getResource("asymmetric/LocalAsymmetricModel.json")).toURI())
        );
        String modelId = registerModelGroupAndUploadModel(requestBody);
        loadModel(modelId);
        return modelId;
    }

    /**
     * Assert neural search results contain expected documents
     */
    protected void assertNeuralSearchResults(Map<String, Object> responseMap, String expectedContent) {
        assertNotNull("Response should not be null", responseMap);

        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        assertNotNull("Response should contain hits", hits);

        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        assertNotNull("Hits should not be null", hitsList);
        assertFalse("Should have at least one hit", hitsList.isEmpty());

        // Check if any hit contains the expected content
        boolean foundExpectedContent = false;
        for (Map<String, Object> hit : hitsList) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            if (source != null && source.containsKey(TEST_FIELD)) {
                String content = (String) source.get(TEST_FIELD);
                if (content != null && content.toLowerCase(Locale.ROOT).contains(expectedContent.toLowerCase(Locale.ROOT))) {
                    foundExpectedContent = true;
                    break;
                }
            }
        }

        assertTrue("Should find document containing: " + expectedContent, foundExpectedContent);
    }

    /**
     * Assert that neural search with query content type returns relevant results
     */
    protected void assertAsymmetricQueryResults(Map<String, Object> responseMap, String queryText) {
        assertNeuralSearchResults(responseMap, "machine learning");

        // Verify the response structure for asymmetric queries
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");

        // For asymmetric models, query should find semantically related content
        // even if exact terms don't match
        assertTrue("Query should return relevant results", hitsList.size() > 0);

        // Check that results have reasonable scores (not all zeros)
        for (Map<String, Object> hit : hitsList) {
            Float score = ((Number) hit.get("_score")).floatValue();
            assertTrue("Score should be positive: " + score, score > 0);
        }
    }
}
