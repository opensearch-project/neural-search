/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
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
    private static final String PIPELINE_NAME = "semantic_highlighting_pipeline";

    private String modelIdBatchDisabled;
    private String modelIdBatchEnabled;
    private String connectorIdBatchDisabled;
    private String connectorIdBatchEnabled;
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

                // Create connectors for both batch disabled and enabled
                connectorIdBatchDisabled = createRemoteModelConnector(torchServeEndpoint, false);
                connectorIdBatchEnabled = createRemoteModelConnector(torchServeEndpoint, true);

                // Deploy models
                modelIdBatchDisabled = deployRemoteSemanticHighlightingModel(
                    connectorIdBatchDisabled,
                    "semantic-highlighter-batch-disabled"
                );
                log.info("Deployed model with batch disabled, model ID: {}", modelIdBatchDisabled);
                modelIdBatchEnabled = deployRemoteSemanticHighlightingModel(connectorIdBatchEnabled, "semantic-highlighter-batch-enabled");
                log.info("Deployed model with batch enabled, model ID: {}", modelIdBatchEnabled);

                createIndexWithSemanticHighlightingPipeline(TEST_INDEX, PIPELINE_NAME);
                indexTestDocuments();
                createSemanticHighlightingPipeline(PIPELINE_NAME, modelIdBatchDisabled, TEST_FIELD, false);
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

            cleanupSemanticHighlightingResources(connectorIdBatchDisabled, modelIdBatchDisabled);
            cleanupSemanticHighlightingResources(connectorIdBatchEnabled, modelIdBatchEnabled);
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
    }

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
            .field("model_id", modelIdBatchDisabled)
            .field("batch_inference", false)
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> responseMap = performSemanticHighlightingSearch(TEST_INDEX, searchBody);
        TestUtils.assertSemanticHighlighting(responseMap, TEST_FIELD, "OpenSearch");

        // Log the highlights for debugging
        List<Map<String, Object>> hitsList = TestUtils.getNestedHits(responseMap);
        Map<String, Object> firstHit = hitsList.get(0);
        Map<String, Object> highlight = (Map<String, Object>) firstHit.get("highlight");
        List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
        log.info("Semantic highlights with batch disabled: {}", contentHighlights);
    }

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
            .field("model_id", modelIdBatchEnabled)
            .field("batch_inference", true)
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> responseMap = performSemanticHighlightingSearch(TEST_INDEX, searchBody);

        // Log the response for debugging
        log.info("Search response: {}", responseMap);

        // Check if highlights exist at all
        List<Map<String, Object>> hitsList = TestUtils.getNestedHits(responseMap);
        if (!hitsList.isEmpty()) {
            Map<String, Object> firstHit = hitsList.get(0);
            Map<String, Object> highlight = (Map<String, Object>) firstHit.get("highlight");
            if (highlight != null) {
                List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
                log.info("Actual highlights: {}", contentHighlights);
            }
        }

        TestUtils.assertSemanticHighlighting(responseMap, TEST_FIELD, "machine learning");

        // Log the highlights for debugging
        for (Map<String, Object> hit : hitsList) {
            Map<String, Object> highlight = (Map<String, Object>) hit.get("highlight");
            List<String> contentHighlights = (List<String>) highlight.get(TEST_FIELD);
            log.info("Semantic highlights with batch enabled: {}", contentHighlights);
        }
    }
}
