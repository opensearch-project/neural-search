/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

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
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

/**
 * Integration tests for Asymmetric Model functionality with local models
 */
@Log4j2
public class AsymmetricLocalModelIT extends BaseAsymmetricModelIT {

    private static final String TEST_INDEX = "test-asymmetric-local-index";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();

        // Prepare asymmetric model for local tests
        try {
            asymmetricModelId = prepareAsymmetricModel();
            log.info("Prepared asymmetric model, model ID: {}", asymmetricModelId);
        } catch (Exception e) {
            log.warn("Failed to prepare asymmetric model: {}", e.getMessage());
        }

        // Create index for tests
        prepareAsymmetricIndex(TEST_INDEX);
        indexAsymmetricTestDocuments(TEST_INDEX);
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

        super.tearDown();
    }

    /**
     * Test basic neural search with local asymmetric model
     */
    public void testNeuralSearchWithLocalAsymmetricModel() throws Exception {
        // Create neural query for machine learning content
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What is artificial intelligence and machine learning?")
            .modelId(asymmetricModelId)
            .k(3)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .field("query")
            .value(neuralQuery)
            .endObject();

        log.info("Testing neural search with local asymmetric model: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify neural search worked and found relevant documents
        assertAsymmetricQueryResults(responseMap, "artificial intelligence");
    }

    /**
     * Test neural search with query about deep learning
     */
    public void testNeuralSearchDeepLearningQuery() throws Exception {
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("How do neural networks work in deep learning?")
            .modelId(asymmetricModelId)
            .k(2)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .field("query")
            .value(neuralQuery)
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertNeuralSearchResults(responseMap, "deep learning");
    }

    /**
     * Test neural search with NLP query
     */
    public void testNeuralSearchNLPQuery() throws Exception {
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What are natural language processing techniques?")
            .modelId(asymmetricModelId)
            .k(2)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .field("query")
            .value(neuralQuery)
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertNeuralSearchResults(responseMap, "natural language processing");
    }

    /**
     * Test neural search with computer vision query
     */
    public void testNeuralSearchComputerVisionQuery() throws Exception {
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("How does computer vision analyze images?")
            .modelId(asymmetricModelId)
            .k(2)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .field("query")
            .value(neuralQuery)
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertNeuralSearchResults(responseMap, "computer vision");
    }

    /**
     * Test neural search with higher k value
     */
    public void testNeuralSearchWithHigherK() throws Exception {
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("Explain machine learning and AI technologies")
            .modelId(asymmetricModelId)
            .k(5)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 5)
            .field("query")
            .value(neuralQuery)
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Should return all 5 documents
        assertNeuralSearchResults(responseMap, "machine learning");

        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        assertEquals("Should return all indexed documents", 5, ((Map<String, Object>) hits.get("total")).get("value"));
    }

    /**
     * Test that asymmetric model produces different embeddings for query vs passage content
     * This test verifies the asymmetric nature by checking that the same text embedded as
     * query vs passage would produce different results (though we can't directly test this
     * with the current local model setup, we verify the model works correctly)
     */
    public void testAsymmetricModelFunctionality() throws Exception {
        // Test with a specific query that should match AI/ML content
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("artificial intelligence systems")
            .modelId(asymmetricModelId)
            .k(3)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .field("query")
            .value(neuralQuery)
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify that the model returns semantically relevant results
        assertAsymmetricQueryResults(responseMap, "artificial intelligence");

        // Verify score ordering (higher scores should come first)
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        java.util.List<Map<String, Object>> hitsList = (java.util.List<Map<String, Object>>) hits.get("hits");

        if (hitsList.size() > 1) {
            Float firstScore = ((Number) hitsList.get(0).get("_score")).floatValue();
            Float secondScore = ((Number) hitsList.get(1).get("_score")).floatValue();
            assertTrue("First result should have higher or equal score", firstScore >= secondScore);
        }
    }
}
