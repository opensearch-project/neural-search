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
import org.opensearch.neuralsearch.stats.events.EventStatName;
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

                // Enable semantic-highlighter system factory for batch inference tests
                updateClusterSettings(
                    "cluster.search.enabled_system_generated_factories",
                    java.util.Collections.singletonList("semantic-highlighter")
                );

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

        // Enable stats to verify single inference tracking
        enableStats();

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

        // Verify stats - single inference mode should track per document
        String statsResponseBody = executeNeuralStatRequest(new java.util.ArrayList<>(), new java.util.ArrayList<>());
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(statsResponseBody);

        // Get number of hits that were highlighted
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        int hitCount = hitsList.size();

        // Verify single inference count matches number of documents highlighted
        int singleInferenceCount = (int) getNestedValue(allNodesStats, EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT);
        assertEquals("Single inference count should match number of documents highlighted", hitCount, singleInferenceCount);
    }

    /**
     * Test semantic highlighting with match query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithQueryMatchWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Enable stats to verify batch inference tracking
        enableStats();

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
            .field("batch_inference", true)
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "treatments");

        // Verify stats - batch inference mode should track per request
        String statsResponseBody = executeNeuralStatRequest(new java.util.ArrayList<>(), new java.util.ArrayList<>());
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(statsResponseBody);

        // Batch inference mode: one request = one batch stat increment
        int batchInferenceCount = (int) getNestedValue(allNodesStats, EventStatName.SEMANTIC_HIGHLIGHTING_BATCH_REQUEST_COUNT);
        assertEquals("Batch inference count should be 1 for batch mode", 1, batchInferenceCount);

        // Single inference count should be 0 for batch mode
        int singleInferenceCount = (int) getNestedValue(allNodesStats, EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT);
        assertEquals("Single inference count should be 0 for batch mode", 0, singleInferenceCount);
    }

    /**
     * Test semantic highlighting with Neural query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithNeuralQueryWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("treatments for neurodegenerative diseases")
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
            .field("batch_inference", true)
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
            .queryText("treatments for neurodegenerative diseases")
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

        // Use helper method - checks all hits for disease or therapy highlights
        assertSemanticHighlighting(responseMap, TEST_FIELD, "treatments");
    }

    /**
     * Test semantic highlighting throws exception when batch mode is requested without system processor enabled
     */
    public void testSemanticHighlightingBatchModeWithoutSystemProcessor() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // First, ensure system processor is disabled
        updateClusterSettings("cluster.search.enabled_system_generated_factories", java.util.Collections.emptyList());
        try {
            // Test: Batch mode should fail without system factory
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

            // Execute request - exception is caught by OpenSearch and returned as shard failure
            Response response = client().performRequest(batchRequest);
            String responseBody = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

            Map<String, Object> shardsInfo = (Map<String, Object>) responseMap.get("_shards");
            List<Map<String, Object>> shardFailures = (List<Map<String, Object>>) shardsInfo.get("failures");
            Map<String, Object> firstFailure = shardFailures.get(0);
            Map<String, Object> reasonObj = (Map<String, Object>) firstFailure.get("reason");
            String errorMessage = (String) reasonObj.get("reason");

            assertTrue(
                "Error should mention batch inference is disabled: " + errorMessage,
                errorMessage.contains("Batch inference for semantic highlighting is disabled")
            );
            assertTrue(
                "Error should provide configuration guidance: " + errorMessage,
                errorMessage.contains("cluster.search.enabled_system_generated_factories") || errorMessage.contains("semantic-highlighter")
            );
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
            .queryText("treatments for neurodegenerative diseases")
            .modelId(textEmbeddingModelId)
            .k(2)
            .build();

        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(TEST_FIELD, "treatments");

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
            .field("batch_inference", true)
            .endObject()
            .endObject()
            .endObject();

        log.info("Testing hybrid query with batch inference enabled: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Use helper method - verifies highlights exist with expected terms
        assertSemanticHighlighting(responseMap, TEST_FIELD, "treatments");
    }
}
