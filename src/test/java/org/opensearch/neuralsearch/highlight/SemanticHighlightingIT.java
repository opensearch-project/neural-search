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
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.annotations.RequiresRemoteModel;
import org.opensearch.neuralsearch.util.RemoteModelTestUtils;
import org.opensearch.neuralsearch.util.TestUtils;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

/**
 * Integration tests for Semantic Highlighting functionality with TorchServe support
 */
@Log4j2
public class SemanticHighlightingIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-semantic-highlight-index";
    private static final String TEST_FIELD = "content";

    private String remoteHighlightModelId;
    private String remoteHighlightConnectorId;
    private boolean isTorchServeAvailable = false;
    private String torchServeEndpoint;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        updateMLCommonsSettings();

        // Check for TorchServe endpoint from environment or system properties
        torchServeEndpoint = System.getenv("TORCHSERVE_ENDPOINT");
        if (torchServeEndpoint == null || torchServeEndpoint.isEmpty()) {
            torchServeEndpoint = System.getProperty("tests.torchserve.endpoint");
        }

        if (torchServeEndpoint != null && !torchServeEndpoint.isEmpty()) {
            isTorchServeAvailable = RemoteModelTestUtils.isRemoteEndpointAvailable(torchServeEndpoint);
            if (isTorchServeAvailable) {
                log.info("TorchServe is available at: {}", torchServeEndpoint);

                // Create connector and deploy single remote model
                remoteHighlightConnectorId = createRemoteModelConnector(torchServeEndpoint);
                remoteHighlightModelId = deployRemoteSemanticHighlightingModel(remoteHighlightConnectorId, "semantic-highlighter-remote");
                log.info("Deployed remote semantic highlighting model, model ID: {}", remoteHighlightModelId);

                // Create simple index
                createSimpleIndex();
                indexTestDocuments();
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
            try {
                deleteIndex(TEST_INDEX);
            } catch (Exception e) {
                log.debug("Failed to delete index: {}", e.getMessage());
            }

            cleanupSemanticHighlightingResources(remoteHighlightConnectorId, remoteHighlightModelId);
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
                "^http://127\\.0\\.0\\.1:.*",
                "^http://torchserve:.*"
            )
        );

        // Enable all system-generated factories for testing
        // Using "*" enables all system-generated factories including our semantic_highlighting_factory
        updateClusterSettings("cluster.search.enabled_system_generated_factories", List.of("*"));
    }

    @SneakyThrows
    private void createSimpleIndex() {
        // Create index with simple mapping
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_FIELD)
            .field("type", "text")
            .endObject()
            .startObject("title")
            .field("type", "text")
            .endObject()
            .startObject("category")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        String mappingJson = mapping.toString();
        Request request = new Request("PUT", "/" + TEST_INDEX);
        request.setJsonEntity("{\"mappings\": " + mappingJson + "}");

        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @SneakyThrows
    private void indexTestDocuments() {
        addSemanticHighlightingDocument(
            TEST_INDEX,
            "1",
            "OpenSearch is a scalable, flexible, and extensible open-source software suite for search, analytics, and observability applications. It is licensed under Apache 2.0.",
            "OpenSearch Overview",
            "software"
        );
        addSemanticHighlightingDocument(
            TEST_INDEX,
            "2",
            "Machine learning is a method of data analysis that automates analytical model building. It is a branch of artificial intelligence based on the idea that systems can learn from data.",
            "Machine Learning Basics",
            "technology"
        );

        // Refresh to make documents searchable
        Request refreshRequest = new Request("POST", "/" + TEST_INDEX + "/_refresh");
        client().performRequest(refreshRequest);
    }

    @RequiresRemoteModel(value = "torchserve", model = "semantic_highlighter")
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
            .field("batch_inference", false)
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

    @RequiresRemoteModel(value = "torchserve", model = "semantic_highlighter")
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
            .field("batch_inference", true)
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
     * Test semantic highlighting with local TORCH_SCRIPT model using QUESTION_ANSWERING function
     * This tests backward compatibility with OpenSearch 3.1 local models
     */
    public void testSemanticHighlightingWithQueryMatchWithBatchDisabledWithLocalModel() throws Exception {
        // Set up ML Commons settings for local model
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);

        // Prepare local sentence highlighting model
        String localModelId = prepareSentenceHighlightingModel();
        log.info("Prepared local model with ID: {}", localModelId);

        // Create a separate index for local model testing
        String localTestIndex = TEST_INDEX + "-local";

        try {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(TEST_FIELD)
                .field("type", "text")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .endObject()
                .startObject("category")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();

            Request createIndexRequest = new Request("PUT", "/" + localTestIndex);
            createIndexRequest.setJsonEntity("{\"mappings\": " + mapping.toString() + "}");
            Response createIndexResponse = client().performRequest(createIndexRequest);
            assertEquals(200, createIndexResponse.getStatusLine().getStatusCode());

            // Index test documents
            addSemanticHighlightingDocument(
                localTestIndex,
                "1",
                "OpenSearch is a scalable, flexible, and extensible open-source software suite for search, analytics, and observability applications. It is licensed under Apache 2.0.",
                "OpenSearch Overview",
                "software"
            );
            addSemanticHighlightingDocument(
                localTestIndex,
                "2",
                "Machine learning is a method of data analysis that automates analytical model building. It is a branch of artificial intelligence based on the idea that systems can learn from data.",
                "Machine Learning Basics",
                "technology"
            );

            // Refresh index
            Request refreshRequest = new Request("POST", "/" + localTestIndex + "/_refresh");
            client().performRequest(refreshRequest);

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
                .field("model_id", localModelId)  // Local model ID specified here
                .field("batch_inference", false)  // Local models don't support batch
                .endObject()
                .endObject()
                .endObject();

            log.info("Sending search request with semantic highlighting: {}", searchBody.toString());
            Request request = new Request("POST", "/" + localTestIndex + "/_search");
            request.setJsonEntity(searchBody.toString());
            Response response = client().performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            log.info("Got search response: {}", responseBody);
            Map<String, Object> searchResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

            // Verify semantic highlighting worked
            TestUtils.assertSemanticHighlighting(searchResponse, TEST_FIELD, "OpenSearch");

            // Log highlights for debugging
            List<Map<String, Object>> hitsList = TestUtils.getNestedHits(searchResponse);
            if (!hitsList.isEmpty()) {
                for (Map<String, Object> hit : hitsList) {
                    Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
                    if (highlight != null) {
                        List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
                        log.info("Local model semantic highlights: {}", contentHighlights);
                    }
                }
            }

            log.info("Local model test completed successfully");

        } finally {
            // Cleanup
            try {
                deleteIndex(localTestIndex);
            } catch (Exception e) {
                log.debug("Failed to delete local test index: {}", e.getMessage());
            }
            try {
                deleteModel(localModelId);
            } catch (Exception e) {
                log.debug("Failed to delete local model: {}", e.getMessage());
            }
        }
    }
}
