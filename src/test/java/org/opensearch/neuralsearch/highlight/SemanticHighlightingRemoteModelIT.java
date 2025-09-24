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
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;
import org.opensearch.neuralsearch.util.TestUtils;

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
            .field("model_id", remoteHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        TestUtils.assertSemanticHighlighting(responseMap, TEST_FIELD, "OpenSearch");
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
            .field(TEST_FIELD, "What is machine learning?")
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

        TestUtils.assertSemanticHighlighting(responseMap, TEST_FIELD, "machine learning");
    }

    /**
     * Test semantic highlighting with Neural query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithNeuralQueryWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What is machine learning and how does it work?")
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

        log.info("Testing neural query with batch inference enabled: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        log.info("Neural query batch response: {}", responseBody);
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        TestUtils.assertSemanticHighlighting(responseMap, TEST_FIELD, "machine learning");
    }

    /**
     * Test semantic highlighting with Neural query using batch inference disabled with remote model
     */
    public void testSemanticHighlightingWithNeuralQueryWithBatchDisabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("OpenSearch is a software for search and analytics")
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

        // Check that at least one hit has OpenSearch highlighted
        // (neural search ordering can vary, so we check any hit)
        boolean foundHighlight = false;
        List<Map<String, Object>> hits = TestUtils.getNestedHits(responseMap);
        for (Map<String, Object> hit : hits) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            if (highlight != null && highlight.containsKey(TEST_FIELD)) {
                List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
                if (contentHighlights != null && !contentHighlights.isEmpty()) {
                    String highlightText = contentHighlights.get(0);
                    if (highlightText.contains("OpenSearch")) {
                        foundHighlight = true;
                        break;
                    }
                }
            }
        }
        assertTrue("Should have found OpenSearch in highlights", foundHighlight);
    }

    /**
     * Test semantic highlighting with Hybrid query using batch inference enabled with remote model
     */
    public void testSemanticHighlightingWithHybridQueryWithBatchEnabledWithRemoteModel() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        // Create hybrid query with neural and match components
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What is machine learning?")
            .modelId(textEmbeddingModelId)
            .k(2)
            .build();

        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(TEST_FIELD, "OpenSearch analytics");

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
        log.info("Hybrid query batch response: {}", responseBody);
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify semantic highlighting worked
        List<Map<String, Object>> hits = TestUtils.getNestedHits(responseMap);
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
