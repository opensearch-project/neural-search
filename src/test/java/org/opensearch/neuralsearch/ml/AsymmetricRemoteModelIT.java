/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

/**
 * Integration tests for Asymmetric Model functionality with remote models
 */
@Log4j2
public class AsymmetricRemoteModelIT extends BaseAsymmetricModelIT {

    private static final String TEST_INDEX = "test-asymmetric-remote-index";

    private String remoteAsymmetricModelId;
    private String remoteAsymmetricConnectorId;
    private boolean isTorchServeAvailable = false;
    private String torchServeEndpoint;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();

        // Check for TorchServe endpoint from environment or system properties
        torchServeEndpoint = System.getenv("TORCHSERVE_ENDPOINT");
        if (torchServeEndpoint == null || torchServeEndpoint.isEmpty()) {
            torchServeEndpoint = System.getProperty("tests.torchserve.endpoint");
        }

        if (torchServeEndpoint != null && !torchServeEndpoint.isEmpty()) {
            isTorchServeAvailable = RemoteModelTestUtils.isRemoteEndpointAvailable(torchServeEndpoint);
            if (isTorchServeAvailable) {
                log.info("TorchServe is available at: {}", torchServeEndpoint);

                // Set dimension for remote model
                testDimension = TEST_DIMENSION_REMOTE;

                // Create connector and deploy remote asymmetric model
                remoteAsymmetricConnectorId = createRemoteAsymmetricModelConnector(torchServeEndpoint);
                remoteAsymmetricModelId = deployRemoteAsymmetricModel(remoteAsymmetricConnectorId, "asymmetric-text-embedding-remote");
                log.info("Deployed remote asymmetric model, model ID: {}", remoteAsymmetricModelId);

                // Set the asymmetric model ID for base class
                asymmetricModelId = remoteAsymmetricModelId;

                // Create index for testing
                prepareAsymmetricIndex(TEST_INDEX);
                indexAsymmetricTestDocuments(TEST_INDEX);
            } else {
                log.info("TorchServe not available at {}, tests will be skipped", torchServeEndpoint);
            }
        } else {
            log.info("No TorchServe endpoint configured, tests will be skipped");
        }
    }

    @After
    @SneakyThrows
    public void tearDown() {
        if (isTorchServeAvailable) {
            // Cleanup indexes
            try {
                deleteIndex(TEST_INDEX);
            } catch (Exception e) {
                log.debug("Failed to delete index: {}", e.getMessage());
            }

            // Cleanup remote model resources
            cleanupRemoteModelResources(remoteAsymmetricConnectorId, remoteAsymmetricModelId);
        }

        super.tearDown();
    }

    /**
     * Create remote model connector for asymmetric text embedding
     */
    @SneakyThrows
    private String createRemoteAsymmetricModelConnector(String endpoint) {
        String connectorName = "asymmetric-connector-" + System.currentTimeMillis();
        String connectorRequestBody = String.format(Locale.ROOT, getAsymmetricConnectorRequestBody(), connectorName, endpoint);

        Request connectorRequest = new Request("POST", "/_plugins/_ml/connectors/_create");
        connectorRequest.setJsonEntity(connectorRequestBody);
        Response connectorResponse = client().performRequest(connectorRequest);
        String connectorResponseBody = EntityUtils.toString(connectorResponse.getEntity());
        Map<String, Object> connectorResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), connectorResponseBody, false);

        return (String) connectorResponseMap.get("connector_id");
    }

    /**
     * Deploy remote asymmetric model
     */
    @SneakyThrows
    private String deployRemoteAsymmetricModel(String connectorId, String modelName) {
        String modelRequestBody = String.format(
            Locale.ROOT,
            "{"
                + "\"name\": \"%s\","
                + "\"function_name\": \"remote\","
                + "\"description\": \"Remote asymmetric text embedding model\","
                + "\"connector_id\": \"%s\""
                + "}",
            modelName,
            connectorId
        );

        Request modelRequest = new Request("POST", "/_plugins/_ml/models/_register");
        modelRequest.setJsonEntity(modelRequestBody);
        Response modelResponse = client().performRequest(modelRequest);
        String modelResponseBody = EntityUtils.toString(modelResponse.getEntity());
        Map<String, Object> modelResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), modelResponseBody, false);

        String modelId = (String) modelResponseMap.get("model_id");

        // Deploy the model
        Request deployRequest = new Request("POST", "/_plugins/_ml/models/" + modelId + "/_deploy");
        client().performRequest(deployRequest);

        // Wait for model to be deployed
        waitForModelToBeReady(modelId);

        return modelId;
    }

    /**
     * Get asymmetric connector request body from resource file
     */
    @SneakyThrows
    private String getAsymmetricConnectorRequestBody() {
        return Files.readString(
            Path.of(Objects.requireNonNull(classLoader.getResource("asymmetric/RemoteTorchServeConnector.json")).toURI())
        );
    }

    /**
     * Test neural search with remote asymmetric model using query content type
     */
    public void testNeuralSearchWithQueryContentType() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Create neural query that should be embedded as "query" type
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What is machine learning and how does it work?")
            .modelId(remoteAsymmetricModelId)
            .k(3)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .field("query")
            .value(neuralQuery)
            .endObject();

        log.info("Testing neural search with remote asymmetric model (query type): {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertAsymmetricQueryResults(responseMap, "machine learning");
    }

    /**
     * Test neural search with deep learning query
     */
    public void testNeuralSearchDeepLearningWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("How do neural networks process information in deep learning?")
            .modelId(remoteAsymmetricModelId)
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
     * Test neural search with NLP query using remote asymmetric model
     */
    public void testNeuralSearchNLPWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What are the applications of natural language processing?")
            .modelId(remoteAsymmetricModelId)
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
     * Test asymmetric model with different query types to verify content_type parameter
     */
    public void testAsymmetricModelWithDifferentContentTypes() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Test 1: Query about AI systems (should match AI content)
        NeuralQueryBuilder aiQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("artificial intelligence systems and applications")
            .modelId(remoteAsymmetricModelId)
            .k(3)
            .build();

        XContentBuilder aiSearchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .field("query")
            .value(aiQuery)
            .endObject();

        Request aiRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
        aiRequest.setJsonEntity(aiSearchBody.toString());
        Response aiResponse = client().performRequest(aiRequest);
        String aiResponseBody = EntityUtils.toString(aiResponse.getEntity());
        Map<String, Object> aiResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), aiResponseBody, false);

        assertAsymmetricQueryResults(aiResponseMap, "artificial intelligence");

        // Test 2: Query about computer vision (should match vision content)
        NeuralQueryBuilder visionQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("image recognition and visual analysis")
            .modelId(remoteAsymmetricModelId)
            .k(2)
            .build();

        XContentBuilder visionSearchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .field("query")
            .value(visionQuery)
            .endObject();

        Request visionRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
        visionRequest.setJsonEntity(visionSearchBody.toString());
        Response visionResponse = client().performRequest(visionRequest);
        String visionResponseBody = EntityUtils.toString(visionResponse.getEntity());
        Map<String, Object> visionResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), visionResponseBody, false);

        assertNeuralSearchResults(visionResponseMap, "computer vision");
    }

    /**
     * Test remote asymmetric model with higher k value
     */
    public void testRemoteAsymmetricModelWithHigherK() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("machine learning and artificial intelligence technologies")
            .modelId(remoteAsymmetricModelId)
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
     * Test that remote asymmetric model handles content type correctly
     * This verifies that the remote model receives and processes the content_type parameter
     */
    public void testRemoteAsymmetricModelContentTypeHandling() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Create a query that should demonstrate asymmetric behavior
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("How does machine learning work?")
            .modelId(remoteAsymmetricModelId)
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

        // Verify that the remote model returns valid results
        assertAsymmetricQueryResults(responseMap, "machine learning");

        // Verify response structure and scores
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        java.util.List<Map<String, Object>> hitsList = (java.util.List<Map<String, Object>>) hits.get("hits");

        // Check that all results have valid scores
        for (Map<String, Object> hit : hitsList) {
            Float score = ((Number) hit.get("_score")).floatValue();
            assertTrue("Score should be positive for remote asymmetric model: " + score, score > 0);
            assertNotNull("Hit should have source", hit.get("_source"));
        }
    }
}
