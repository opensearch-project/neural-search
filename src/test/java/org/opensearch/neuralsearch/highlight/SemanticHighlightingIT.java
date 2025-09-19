/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.util.TestUtils;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;

/**
 * Integration tests for Semantic Highlighting functionality with local models
 */
@Log4j2
public class SemanticHighlightingIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-semantic-highlight-index";
    private static final String TEST_FIELD = "content";
    private static final String TEST_KNN_VECTOR_FIELD = "content_embedding";
    private static final int TEST_DIMENSION = 768;

    private String textEmbeddingModelId;  // For neural queries
    private String localHighlightModelId;  // For local model tests

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        updateMLCommonsSettings();

        // Prepare models for local tests
        try {
            textEmbeddingModelId = prepareModel();
            log.info("Prepared text embedding model, model ID: {}", textEmbeddingModelId);
        } catch (Exception e) {
            log.warn("Failed to prepare text embedding model: {}", e.getMessage());
        }

        try {
            localHighlightModelId = prepareSentenceHighlightingModel();
            log.info("Prepared local highlighting model, model ID: {}", localHighlightModelId);
        } catch (Exception e) {
            log.warn("Failed to prepare local highlighting model: {}", e.getMessage());
        }

        // Create index for tests (supports both text and neural searches)
        prepareKnnIndex(TEST_INDEX, Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)));
        indexTestDocuments();
    }

    @After
    @SneakyThrows
    public void tearDown() {
        // Cleanup indexes
        try {
            deleteIndex(TEST_INDEX);
        } catch (Exception e) {
            log.debug("Failed to delete index: {}", e.getMessage());
        }

        // Delete text embedding pipeline
        try {
            Request request = new Request("DELETE", "/_ingest/pipeline/test-text-embedding-pipeline");
            client().performRequest(request);
        } catch (Exception e) {
            log.debug("Failed to delete pipeline: {}", e.getMessage());
        }

        // Cleanup local models
        try {
            if (textEmbeddingModelId != null) {
                deleteModel(textEmbeddingModelId);
            }
        } catch (Exception e) {
            log.debug("Failed to delete text embedding model: {}", e.getMessage());
        }

        try {
            if (localHighlightModelId != null) {
                deleteModel(localHighlightModelId);
            }
        } catch (Exception e) {
            log.debug("Failed to delete local highlight model: {}", e.getMessage());
        }

        super.tearDown();
    }

    @SneakyThrows
    private void updateMLCommonsSettings() {
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        updateClusterSettings("plugins.ml_commons.connector.private_ip_enabled", true);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
        updateClusterSettings(
            "plugins.ml_commons.trusted_connector_endpoints_regex",
            List.of(
                "^https://runtime\\.sagemaker\\..*[a-z0-9-]\\.amazonaws\\.com/.*$",
                "^http://localhost:.*",
                "^http://127\\.0\\.0\\.1:.*"
            )
        );
    }

    @SneakyThrows
    private void indexTestDocuments() {
        // Create text embedding pipeline if we have the model
        if (textEmbeddingModelId != null) {
            String pipeline = "test-text-embedding-pipeline";
            XContentBuilder pipelineBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startArray("processors")
                .startObject()
                .startObject("text_embedding")
                .field("model_id", textEmbeddingModelId)
                .startObject("field_map")
                .field(TEST_FIELD, TEST_KNN_VECTOR_FIELD)
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject();

            Request createPipelineRequest = new Request("PUT", "/_ingest/pipeline/" + pipeline);
            createPipelineRequest.setJsonEntity(pipelineBuilder.toString());
            Response pipelineResponse = client().performRequest(createPipelineRequest);
            assertEquals(200, pipelineResponse.getStatusLine().getStatusCode());

            // Index documents with pipeline for neural search support
            addKnnDocWithPipeline(
                TEST_INDEX,
                "1",
                TEST_FIELD,
                "OpenSearch is a scalable, flexible, and extensible open-source software suite for search, analytics, and observability applications. It is licensed under Apache 2.0.",
                pipeline
            );
            addKnnDocWithPipeline(
                TEST_INDEX,
                "2",
                TEST_FIELD,
                "Machine learning is a method of data analysis that automates analytical model building. It is a branch of artificial intelligence based on the idea that systems can learn from data.",
                pipeline
            );
            addKnnDocWithPipeline(
                TEST_INDEX,
                "3",
                TEST_FIELD,
                "Natural language processing enables computers to understand, interpret, and generate human language. It combines computational linguistics with machine learning and deep learning models.",
                pipeline
            );
        } else {
            // Index documents without pipeline for basic match query tests
            addKnnDoc(
                TEST_INDEX,
                "1",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "OpenSearch is a scalable, flexible, and extensible open-source software suite for search, analytics, and observability applications. It is licensed under Apache 2.0."
                )
            );
            addKnnDoc(
                TEST_INDEX,
                "2",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEST_FIELD),
                Collections.singletonList(
                    "Machine learning is a method of data analysis that automates analytical model building. It is a branch of artificial intelligence based on the idea that systems can learn from data."
                )
            );
        }

        // Documents are automatically refreshed by addKnnDoc
    }

    @SneakyThrows
    private void addKnnDocWithPipeline(String indexName, String docId, String fieldName, String content, String pipeline) {
        Request request = new Request("PUT", "/" + indexName + "/_doc/" + docId + "?pipeline=" + pipeline + "&refresh=true");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field(fieldName, content).endObject();
        request.setJsonEntity(builder.toString());
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    /**
     * Test semantic highlighting with local TORCH_SCRIPT model using QUESTION_ANSWERING function
     * This tests backward compatibility with OpenSearch 3.1 local models
     */
    public void testSemanticHighlightingWithQueryMatchWithBatchDisabledWithLocalModel() throws Exception {
        // Use the already prepared local model from setUp()
        log.info("Using pre-prepared local model with ID: {}", localHighlightModelId);

        // Create query with semantic highlighting and model_id in options
        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("match")
            .field(TEST_FIELD, "What is OpenSearch used for?")
            .endObject()
            .endObject()
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "semantic")
            .endObject()
            .endObject()
            .startObject("options")
            .field("model_id", localHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        log.info("Sending search request with semantic highlighting: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        log.info("Got search response: {}", responseBody);
        Map<String, Object> searchResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify semantic highlighting worked
        TestUtils.assertSemanticHighlighting(searchResponse, TEST_FIELD, "OpenSearch");

        log.info("Local model test completed successfully");
    }

    /**
     * Test semantic highlighting with Neural query using batch inference disabled with local model
     */
    public void testSemanticHighlightingWithNeuralQueryWithLocalModel() throws Exception {
        // Create neural query
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What is natural language processing?")
            .modelId(textEmbeddingModelId)
            .k(2)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .field("query")
            .value(neuralQuery)
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "semantic")
            .endObject()
            .endObject()
            .startObject("options")
            .field("model_id", localHighlightModelId)  // Use local model
            .endObject()
            .endObject()
            .endObject();

        log.info("Testing neural query with local model (batch disabled): {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify semantic highlighting worked
        TestUtils.assertSemanticHighlighting(responseMap, TEST_FIELD, "natural language processing");
    }

    public void testSemanticHighlightingDisabledWhenFactoryNotEnabled() throws Exception {
        updateClusterSettings("cluster.search.enabled_system_generated_factories", Collections.emptyList());
        try {
            XContentBuilder searchBody = XContentFactory.jsonBuilder()
                .startObject()
                .field("size", 1)
                .startObject("query")
                .startObject("match")
                .field(TEST_FIELD, "What is OpenSearch used for?")
                .endObject()
                .endObject()
                .startObject("highlight")
                .startObject("fields")
                .startObject(TEST_FIELD)
                .field("type", "semantic")
                .endObject()
                .endObject()
                .startObject("options")
                .field("model_id", localHighlightModelId)
                .endObject()
                .endObject()
                .endObject();

            Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
            request.setJsonEntity(searchBody.toString());
            Response response = client().performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            Map<String, Object> searchResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
            List<Map<String, Object>> hits = TestUtils.getNestedHits(searchResponse);
            assertFalse("Expected at least one hit", hits.isEmpty());
            Map<String, Object> highlight = (Map<String, Object>) hits.get(0).get("highlight");
            assertTrue(
                "Semantic highlighting should be absent when factory is disabled",
                highlight == null || highlight.containsKey(TEST_FIELD) == false
            );
        } finally {
            updateClusterSettings("cluster.search.enabled_system_generated_factories", null);
        }
    }
}
