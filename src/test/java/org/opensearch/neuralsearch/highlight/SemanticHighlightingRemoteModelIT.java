/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.util.List;
import java.util.Map;

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
import org.opensearch.neuralsearch.util.AggregationsTestUtils;
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Integration tests for Semantic Highlighting functionality with remote models
 */
@Log4j2
public class SemanticHighlightingRemoteModelIT extends BaseSemanticHighlightingIT {

    private static final String TEST_INDEX = "test-semantic-highlight-remote-index";

    private String remoteHighlightModelId;
    private String remoteHighlightConnectorId;
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

                // Create connector and deploy remote models
                remoteHighlightConnectorId = createRemoteModelConnector(torchServeEndpoint);
                remoteHighlightModelId = deployRemoteModel(remoteHighlightConnectorId, "semantic-highlighter-remote");
                log.info("Deployed remote semantic highlighting model, model ID: {}", remoteHighlightModelId);

                // Prepare text embedding model for neural queries
                textEmbeddingModelId = prepareModel();
                log.info("Prepared text embedding model, model ID: {}", textEmbeddingModelId);

                // Create index for testing (supports both text and neural searches)
                prepareHighlightingIndex(TEST_INDEX);
                indexTestDocuments(TEST_INDEX);
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
            cleanupRemoteModelResources(remoteHighlightConnectorId, remoteHighlightModelId);
        }

        super.tearDown();
    }

    /**
     * Test semantic highlighting with match query using batch inference disabled with remote model
     */
    public void testSemanticHighlightingWithQueryMatchWithBatchDisabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("match")
            .field(TEST_FIELD, "treatments for neurodegenerative diseases")
            .endObject()
            .endObject()
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "semantic")
            .endObject()
            .endObject()
            .startObject("options")
            .field("model_id", remoteHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "treatments");
    }

    /**
     * Test semantic highlighting with match query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithQueryMatchWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("match")
            .field(TEST_FIELD, "clinical trials for therapies")
            .endObject()
            .endObject()
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "semantic")
            .endObject()
            .endObject()
            .startObject("options")
            .field("model_id", remoteHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "clinical trials");
    }

    /**
     * Test semantic highlighting with Neural query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithNeuralQueryWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What are the treatments for neurodegenerative diseases?")
            .modelId(textEmbeddingModelId)
            .k(3)
            .build();

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .field("query")
            .value(neuralQuery)
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "semantic")
            .endObject()
            .endObject()
            .startObject("options")
            .field("model_id", remoteHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        log.info("Neural query batch response: {}", responseBody);
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "treatments");
    }

    /**
     * Test semantic highlighting with Neural query using batch inference disabled with remote model
     */
    public void testSemanticHighlightingWithNeuralQueryWithBatchDisabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("disease mechanisms and therapeutic interventions")
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
            .field("model_id", remoteHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        log.info("Testing neural query with batch inference disabled: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Check that at least one hit has disease or therapy highlighted
        // (neural search ordering can vary, so we check any hit)
        boolean foundHighlight = false;
        List<Map<String, Object>> hits = AggregationsTestUtils.getNestedHits(responseMap);
        for (Map<String, Object> hit : hits) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(TEST_FIELD)) {
                List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
                if (contentHighlights != null && !contentHighlights.isEmpty()) {
                    String highlightText = contentHighlights.get(0);
                    if (highlightText.contains("disease") || highlightText.contains("therapy")) {
                        foundHighlight = true;
                        break;
                    }
                }
            }
        }
        assertTrue("Should have found disease or therapy in highlights", foundHighlight);
    }

    /**
     * Test semantic highlighting throws exception when batch mode is requested without system processor enabled
     */
    public void testSemanticHighlightingBatchModeWithoutSystemProcessor() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // First, ensure system processor is disabled
        updateClusterSettings("cluster.search.enabled_system_generated_factories", java.util.Collections.emptyList());
        try {
            // Test 1: Batch mode should fail without system factory
            XContentBuilder batchSearchBody = XContentFactory.jsonBuilder()
                .startObject()
                .field("size", 1)
                .startObject("query")
                .startObject("match")
                .field(TEST_FIELD, "treatments for neurodegenerative diseases")
                .endObject()
                .endObject()
                .startObject("highlight")
                .startObject("fields")
                .startObject(TEST_FIELD)
                .field("type", "semantic")
                .endObject()
                .endObject()
                .startObject("options")
                .field("model_id", remoteHighlightModelId)
                .field("batch_inference", true)  // Test batch mode which requires system factory
                .endObject()
                .endObject()
                .endObject();

            Request batchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
            batchRequest.setJsonEntity(batchSearchBody.toString());

            // When batch mode is enabled but system factory is disabled, should throw exception or return error
            Response batchResponse = null;
            boolean caughtException = false;
            String errorMessage = null;

            try {
                batchResponse = client().performRequest(batchRequest);
                // If we get here, the request succeeded - check if there's an error in the response
                String responseBody = EntityUtils.toString(batchResponse.getEntity());
                log.info("Batch request response status: {}", batchResponse.getStatusLine());
                log.info("Batch request response body: {}", responseBody);

                // Check if the response contains an error or failure
                Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
                if (responseMap.containsKey("error")) {
                    Map<String, Object> errorObj = (Map<String, Object>) responseMap.get("error");
                    if (errorObj != null) {
                        errorMessage = errorObj.toString();
                        caughtException = true;
                        log.info("Found error in response: {}", errorMessage);
                    }
                } else if (responseMap.containsKey("failures")) {
                    errorMessage = responseMap.get("failures").toString();
                    caughtException = true;
                    log.info("Found failures in response: {}", errorMessage);
                } else if (responseMap.containsKey("_shards")) {
                    // Check for shard failures (common pattern for search errors)
                    Map<String, Object> shardsInfo = (Map<String, Object>) responseMap.get("_shards");
                    if (shardsInfo != null && shardsInfo.containsKey("failures")) {
                        List<Map<String, Object>> shardFailures = (List<Map<String, Object>>) shardsInfo.get("failures");
                        if (shardFailures != null && !shardFailures.isEmpty()) {
                            Map<String, Object> firstFailure = shardFailures.get(0);
                            if (firstFailure != null && firstFailure.containsKey("reason")) {
                                Map<String, Object> reasonObj = (Map<String, Object>) firstFailure.get("reason");
                                if (reasonObj != null && reasonObj.containsKey("reason")) {
                                    errorMessage = (String) reasonObj.get("reason");
                                    caughtException = true;
                                    log.info("Found shard failure reason: {}", errorMessage);
                                }
                            }
                        }
                    }
                }

                // Also check if the response has hits but no highlighting (which would indicate the error was swallowed)
                if (!caughtException) {
                    List<Map<String, Object>> hits = AggregationsTestUtils.getNestedHits(responseMap);
                    if (hits != null && !hits.isEmpty()) {
                        // Check if any hit has highlighting - if not, the batch mode was likely ignored
                        boolean hasHighlights = false;
                        for (Map<String, Object> hit : hits) {
                            if (hit.containsKey("highlight")) {
                                hasHighlights = true;
                                break;
                            }
                        }
                        if (!hasHighlights) {
                            log.warn("Response has hits but no highlights - batch mode might have been silently ignored");
                        }
                    }
                    fail(
                        "Expected exception or error response when batch mode is requested without system processor enabled. Response: "
                            + responseBody
                    );
                }
            } catch (Exception e) {
                // Got an exception from the HTTP request itself
                caughtException = true;
                errorMessage = e.getMessage();
                log.info("Caught exception from request: {}", errorMessage);
            }

            // Verify we got an error (either as exception or in response)
            assertTrue("Should have caught an exception or error response", caughtException);
            assertNotNull("Error message should not be null", errorMessage);

            // Verify the error message contains helpful information
            assertTrue(
                "Error should mention batch inference is disabled: " + errorMessage,
                errorMessage.contains("Batch inference for semantic highlighting is disabled")
                    || errorMessage.contains("batch_inference")
                    || errorMessage.contains("Batch inference for semantic highlighting requires")
            );

            assertTrue(
                "Error should provide configuration guidance: " + errorMessage,
                errorMessage.contains("cluster.search.enabled_system_generated_factories") || errorMessage.contains("semantic-highlighter")
            );

            // Test 2: Single inference mode should work without system factory
            XContentBuilder singleSearchBody = XContentFactory.jsonBuilder()
                .startObject()
                .field("size", 1)
                .startObject("query")
                .startObject("match")
                .field(TEST_FIELD, "novel treatments for disease")
                .endObject()
                .endObject()
                .startObject("highlight")
                .startObject("fields")
                .startObject(TEST_FIELD)
                .field("type", "semantic")
                .endObject()
                .endObject()
                .startObject("options")
                .field("model_id", remoteHighlightModelId)
                .field("batch_inference", false)  // Single inference mode (default)
                .endObject()
                .endObject()
                .endObject();

            Request singleRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
            singleRequest.setJsonEntity(singleSearchBody.toString());
            Response singleResponse = client().performRequest(singleRequest);
            String singleResponseBody = EntityUtils.toString(singleResponse.getEntity());
            Map<String, Object> singleSearchResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), singleResponseBody, false);

            // Single inference mode should work and produce highlights
            assertSemanticHighlighting(singleSearchResponse, TEST_FIELD, "treatments");
        } finally {
            // Restore default setting
            updateClusterSettings("cluster.search.enabled_system_generated_factories", null);
        }
    }

    /**
     * Test semantic highlighting with Hybrid query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithHybridQueryWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Create hybrid query with neural and match components
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What are the treatments for neurodegenerative diseases?")
            .modelId(textEmbeddingModelId)
            .k(2)
            .build();

        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(TEST_FIELD, "clinical trials therapy");

        HybridQueryBuilder hybridQuery = new HybridQueryBuilder();
        hybridQuery.add(neuralQuery);
        hybridQuery.add(matchQuery);

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .field("query")
            .value(hybridQuery)
            .startObject("highlight")
            .startObject("fields")
            .startObject(TEST_FIELD)
            .field("type", "semantic")
            .endObject()
            .endObject()
            .startObject("options")
            .field("model_id", remoteHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        log.info("Testing hybrid query with batch inference enabled: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify semantic highlighting worked
        List<Map<String, Object>> hits = AggregationsTestUtils.getNestedHits(responseMap);
        assertNotNull("Should have search hits", hits);
        assertTrue("Should have at least one hit", !hits.isEmpty());

        // Check that highlights exist
        boolean foundHighlight = false;
        for (Map<String, Object> hit : hits) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(TEST_FIELD)) {
                foundHighlight = true;
                List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
                log.info("Hybrid query highlights: {}", contentHighlights);
                assertNotNull("Highlights should not be null", contentHighlights);
                assertTrue("Should have at least one highlight", !contentHighlights.isEmpty());
            }
        }
        assertTrue("Should have found at least one highlight", foundHighlight);
    }
}
