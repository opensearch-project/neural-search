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
     * Verifies that when batch semantic highlighting is requested but the system-generated
     * factory is disabled in cluster settings, the request fails with a clear error so the
     * customer notices the misconfiguration instead of silently getting no highlights.
     */
    public void testSemanticHighlightingBatchModeWithoutSystemProcessor() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        updateClusterSettings("cluster.search.enabled_system_generated_factories", java.util.Collections.emptyList());
        try {
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
                .field("batch_inference", true)
                .endObject()
                .endObject()
                .endObject();

            Request batchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
            batchRequest.setJsonEntity(batchSearchBody.toString());

            String body;
            try {
                Response response = client().performRequest(batchRequest);
                body = EntityUtils.toString(response.getEntity());
            } catch (org.opensearch.client.ResponseException ex) {
                body = EntityUtils.toString(ex.getResponse().getEntity());
            }
            assertTrue(
                "response must surface the missing system-generated processor: " + body,
                body.contains("system-generated processor is not enabled")
            );
        } finally {
            // Restore the system factory for the rest of the test methods in this class.
            updateClusterSettings(
                "cluster.search.enabled_system_generated_factories",
                java.util.Collections.singletonList("semantic-highlighter")
            );
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

    private static final String NESTED_TEST_INDEX = "test-semantic-highlight-nested-index";

    /**
     * Nested query with {@code type: semantic} declared inside the {@code inner_hits.highlight}
     * block, opted into batch semantic highlighting via the request-level
     * {@code ext.semantic_highlighting_batch: true}. The system processor walks the query tree,
     * discovers the inner_hits target, and produces highlights for every inner hit.
     */
    public void testBatchSemanticHighlightingWithExtBlockOnNestedInnerHits() throws Exception {
        Assume.assumeTrue("TorchServe is not available, skipping test", isTorchServeAvailable);

        createNestedHighlightingIndex(NESTED_TEST_INDEX);
        try {
            indexNestedTestDocuments(NESTED_TEST_INDEX);
            String queryBody = nestedInnerHitsQuery(
                "chunks",
                "chunks.text",
                "treatments for neurodegenerative diseases",
                remoteHighlightModelId,
                false,
                true
            );
            Request searchRequest = new Request("POST", "/" + NESTED_TEST_INDEX + "/_search");
            searchRequest.setJsonEntity(queryBody);
            Response searchResponse = client().performRequest(searchRequest);
            String responseBody = EntityUtils.toString(searchResponse.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

            assertInnerHitsHaveSemanticHighlight(responseMap, "chunks", "chunks.text", "treatments");
        } finally {
            try {
                deleteIndex(NESTED_TEST_INDEX);
            } catch (Exception e) {
                log.debug("Failed to delete nested index: {}", e.getMessage());
            }
        }
    }

    @SneakyThrows
    private void createNestedHighlightingIndex(String indexName) {
        String mapping = "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },"
            + "  \"mappings\": { \"properties\": {"
            + "    \"title\": { \"type\": \"text\" },"
            + "    \"chunks\": { \"type\": \"nested\", \"properties\": { \"text\": { \"type\": \"text\" } } }"
            + "  } } }";
        Request createIdx = new Request("PUT", "/" + indexName);
        createIdx.setJsonEntity(mapping);
        Response resp = client().performRequest(createIdx);
        assertEquals(200, resp.getStatusLine().getStatusCode());
    }

    @SneakyThrows
    private void indexNestedTestDocuments(String indexName) {
        String bulk = "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"title\":\"Parkinson Therapies Overview\",\"chunks\":[{\"text\":\"Treatments for neurodegenerative diseases like Parkinson "
            + "disease include cholinesterase inhibitors and clinical trials of disease-modifying agents.\"}]}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"title\":\"ALS Research Update\",\"chunks\":[{\"text\":\"Treatments for neurodegenerative diseases like ALS are advancing "
            + "rapidly thanks to new biomarker research and targeted molecular interventions that slow motor neuron loss.\"}]}\n";
        Request req = new Request("POST", "/" + indexName + "/_bulk?refresh=true");
        req.setJsonEntity(bulk);
        Response resp = client().performRequest(req);
        assertEquals(200, resp.getStatusLine().getStatusCode());
    }

    private static String nestedInnerHitsQuery(
        String nestedPath,
        String nestedField,
        String queryText,
        String modelId,
        boolean batchInference,
        boolean useExtBlock
    ) {
        StringBuilder body = new StringBuilder();
        body.append('{');
        body.append("\"query\":{\"nested\":{\"path\":\"")
            .append(nestedPath)
            .append("\",")
            .append("\"query\":{\"match\":{\"")
            .append(nestedField)
            .append("\":\"")
            .append(queryText)
            .append("\"}},")
            .append("\"inner_hits\":{\"size\":5,\"highlight\":{\"fields\":{\"")
            .append(nestedField)
            .append("\":{\"type\":\"semantic\"}},\"options\":{\"model_id\":\"")
            .append(modelId)
            .append("\"");
        if (batchInference) {
            body.append(",\"batch_inference\":true");
        }
        body.append("}}}}}");
        if (useExtBlock) {
            body.append(",\"ext\":{\"semantic_highlighting_batch\":true}");
        }
        body.append('}');
        return body.toString();
    }

    @SuppressWarnings("unchecked")
    private void assertInnerHitsHaveSemanticHighlight(
        Map<String, Object> responseMap,
        String bucketName,
        String fieldName,
        String expectedSubstring
    ) {
        assertNotNull("response must not be null", responseMap);
        Map<String, Object> hitsSection = (Map<String, Object>) responseMap.get("hits");
        assertNotNull("hits section missing", hitsSection);
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsSection.get("hits");
        assertNotNull("hits list missing", hits);
        assertFalse("nested query should match at least one doc", hits.isEmpty());

        boolean foundHighlight = false;
        for (Map<String, Object> hit : hits) {
            Map<String, Object> innerHitsMap = (Map<String, Object>) hit.get("inner_hits");
            if (innerHitsMap == null) continue;
            Map<String, Object> bucket = (Map<String, Object>) innerHitsMap.get(bucketName);
            if (bucket == null) continue;
            Map<String, Object> innerHitsHits = (Map<String, Object>) bucket.get("hits");
            List<Map<String, Object>> innerList = (List<Map<String, Object>>) innerHitsHits.get("hits");
            for (Map<String, Object> ih : innerList) {
                Map<String, Object> hl = (Map<String, Object>) ih.get("highlight");
                if (hl == null) continue;
                List<String> fragments = (List<String>) hl.get(fieldName);
                if (fragments == null) continue;
                for (String frag : fragments) {
                    assertTrue("fragment must contain <em>: " + frag, frag.contains("<em>") && frag.contains("</em>"));
                    String plain = frag.replaceAll("<[^>]*>", "");
                    if (plain.toLowerCase(java.util.Locale.ROOT).contains(expectedSubstring.toLowerCase(java.util.Locale.ROOT))) {
                        foundHighlight = true;
                    }
                }
            }
        }
        assertTrue("expected at least one inner_hit highlight containing '" + expectedSubstring + "'", foundHighlight);
    }
}
