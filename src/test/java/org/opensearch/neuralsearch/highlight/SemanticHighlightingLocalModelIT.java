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
import org.opensearch.neuralsearch.stats.events.EventStatName;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

/**
 * Integration tests for Semantic Highlighting functionality with local models
 */
@Log4j2
public class SemanticHighlightingLocalModelIT extends BaseSemanticHighlightingIT {

    private static final String TEST_INDEX = "test-semantic-highlight-index";
    private String localHighlightModelId;  // For local model tests

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();

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

        // Enable the semantic-highlighter system factory so batch-inference tests can run.
        updateClusterSettings("cluster.search.enabled_system_generated_factories", Collections.singletonList("semantic-highlighter"));

        // Create index for tests (supports both text and neural searches)
        prepareHighlightingIndex(TEST_INDEX);
        indexTestDocuments(TEST_INDEX);
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

        // Cleanup local model
        try {
            if (localHighlightModelId != null) {
                deleteModel(localHighlightModelId);
            }
        } catch (Exception e) {
            log.debug("Failed to delete local highlight model: {}", e.getMessage());
        }

        super.tearDown();
    }

    /**
     * Test semantic highlighting with local TORCH_SCRIPT model using QUESTION_ANSWERING function
     * This tests backward compatibility with OpenSearch 3.1 local models
     */
    public void testSemanticHighlightingWithQueryMatchWithBatchDisabledWithLocalModel() throws Exception {
        // Enable stats to verify single inference tracking
        enableStats();

        // Use the already prepared local model from setUp()
        log.info("Using pre-prepared local model with ID: {}", localHighlightModelId);

        // Create query with semantic highlighting and model_id in options
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
            .field("model_id", localHighlightModelId)
            .endObject()
            .endObject()
            .endObject();

        log.info("Sending search request with semantic highlighting: {}", searchBody.toString());
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        request.setJsonEntity(searchBody.toString());
        Response response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> searchResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        // Verify semantic highlighting worked
        assertSemanticHighlighting(searchResponse, TEST_FIELD, "treatments");

        // Verify stats - single inference mode should track per document
        String statsResponseBody = executeNeuralStatRequest(new java.util.ArrayList<>(), new java.util.ArrayList<>());
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(statsResponseBody);

        // Get number of hits that were highlighted
        Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
        int hitCount = hitsList.size();

        // Verify single inference count matches number of documents highlighted
        int singleInferenceCount = (int) getNestedValue(allNodesStats, EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT);
        assertEquals("Single inference count should match number of documents highlighted", hitCount, singleInferenceCount);

        // Batch count should be 0 for single inference mode
        int batchInferenceCount = (int) getNestedValue(allNodesStats, EventStatName.SEMANTIC_HIGHLIGHTING_BATCH_REQUEST_COUNT);
        assertEquals("Batch inference count should be 0 for single mode", 0, batchInferenceCount);

        log.info(
            "Local model test completed successfully - Stats verified: single={}, batch={}",
            singleInferenceCount,
            batchInferenceCount
        );
    }

    /**
     * Test semantic highlighting with term query using local model
     */
    public void testSemanticHighlightingWithTermQueryWithLocalModel() throws Exception {
        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("term")
            .startObject(TEST_FIELD)
            .field("value", "neurodegenerative")
            .endObject()
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
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "neurodegenerative");
    }

    /**
     * Test semantic highlighting with boolean query using local model
     */
    public void testSemanticHighlightingWithBooleanQueryWithLocalModel() throws Exception {
        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 3)
            .startObject("query")
            .startObject("bool")
            .startArray("must")
            .startObject()
            .startObject("match")
            .field(TEST_FIELD, "disease")
            .endObject()
            .endObject()
            .startObject()
            .startObject("match")
            .field(TEST_FIELD, "therapy")
            .endObject()
            .endObject()
            .endArray()
            .startArray("should")
            .startObject()
            .startObject("match")
            .field(TEST_FIELD, "clinical")
            .endObject()
            .endObject()
            .endArray()
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
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "disease");
    }

    /**
     * Test semantic highlighting with query_string query using local model
     */
    public void testSemanticHighlightingWithQueryStringQueryWithLocalModel() throws Exception {
        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("query_string")
            .field("query", "neurodegenerative AND therapy")
            .field("default_field", TEST_FIELD)
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
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "neurodegenerative");
    }

    /**
     * Test semantic highlighting with custom tags using local model
     */
    public void testSemanticHighlightingWithCustomTagsWithLocalModel() throws Exception {
        String customPreTag = "<custom>";
        String customPostTag = "</custom>";

        XContentBuilder searchBody = XContentFactory.jsonBuilder()
            .startObject()
            .field("size", 2)
            .startObject("query")
            .startObject("match")
            .field(TEST_FIELD, "clinical trials for disease treatment")
            .endObject()
            .endObject()
            .startObject("highlight")
            .startArray("pre_tags")
            .value(customPreTag)
            .endArray()
            .startArray("post_tags")
            .value(customPostTag)
            .endArray()
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
        Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);

        assertSemanticHighlighting(responseMap, TEST_FIELD, "clinical trials", customPreTag, customPostTag);
    }

    /**
     * Test semantic highlighting with Neural query using batch inference disabled with local model
     */
    public void testSemanticHighlightingWithNeuralQueryWithLocalModel() throws Exception {
        // Create neural query
        NeuralQueryBuilder neuralQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD)
            .queryText("What are the treatments for neurodegenerative diseases?")
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
        assertSemanticHighlighting(responseMap, TEST_FIELD, "treatments");
    }

    /**
     * Verifies that when batch semantic highlighting is requested but the system-generated
     * factory is disabled in cluster settings, the request surfaces a clear error to the
     * customer instead of silently producing no highlights. Single inference mode (no opt-in
     * to batch) continues to work without the factory.
     */
    public void testSemanticHighlightingDisabledWhenFactoryNotEnabled() throws Exception {
        updateClusterSettings("cluster.search.enabled_system_generated_factories", Collections.emptyList());
        try {
            // Test 1: Batch mode without system factory → request must surface the misconfiguration.
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
                .field("model_id", localHighlightModelId)
                .field("batch_inference", true)
                .endObject()
                .endObject()
                .endObject();

            Request batchRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
            batchRequest.setJsonEntity(batchSearchBody.toString());

            // The misconfiguration surfaces as either a top-level ResponseException or a 200 OK
            // with the error captured in _shards.failures (shard exceptions don't always escalate).
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

            // Test 2: Single inference mode continues to work without the system factory.
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
                .field("model_id", localHighlightModelId)
                .field("batch_inference", false)
                .endObject()
                .endObject()
                .endObject();

            Request singleRequest = new Request("POST", "/" + TEST_INDEX + "/_search");
            singleRequest.setJsonEntity(singleSearchBody.toString());
            Response singleResponse = client().performRequest(singleRequest);
            String singleResponseBody = EntityUtils.toString(singleResponse.getEntity());
            Map<String, Object> singleSearchResponse = XContentHelper.convertToMap(XContentType.JSON.xContent(), singleResponseBody, false);

            assertSemanticHighlighting(singleSearchResponse, TEST_FIELD, "treatments");
        } finally {
            // Restore the system factory for the rest of the test methods in this class.
            updateClusterSettings("cluster.search.enabled_system_generated_factories", Collections.singletonList("semantic-highlighter"));
        }
    }

    /**
     * Local-model variant: nested query with {@code type: semantic} declared inside the
     * {@code inner_hits.highlight} block, opted into batch semantic highlighting via the
     * request-level {@code ext.semantic_highlighting_batch: true}. Batch inference is only
     * supported for REMOTE models, so the request must surface a clear error rather than
     * silently producing no highlights.
     */
    public void testBatchSemanticHighlightingWithExtBlockOnNestedInnerHitsLocalModel() throws Exception {
        final String nestedIndex = "test-semantic-highlight-nested-local-index";
        createNestedHighlightingIndex(nestedIndex);
        try {
            indexNestedTestDocuments(nestedIndex);
            String queryBody = nestedInnerHitsQueryWithExt(
                "chunks",
                "chunks.text",
                "treatments for neurodegenerative diseases",
                localHighlightModelId
            );
            Request searchRequest = new Request("POST", "/" + nestedIndex + "/_search");
            searchRequest.setJsonEntity(queryBody);

            // The unsupported combination surfaces either as a top-level ResponseException or
            // as a 200 OK whose body captures the error in _shards.failures.
            String body;
            try {
                Response response = client().performRequest(searchRequest);
                body = EntityUtils.toString(response.getEntity());
            } catch (org.opensearch.client.ResponseException ex) {
                body = EntityUtils.toString(ex.getResponse().getEntity());
            }
            assertTrue(
                "response must surface that batch inference is REMOTE-only: " + body,
                body.contains("only supported for REMOTE models")
            );
        } finally {
            try {
                deleteIndex(nestedIndex);
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

    private static String nestedInnerHitsQueryWithExt(String nestedPath, String nestedField, String queryText, String modelId) {
        return "{"
            + "\"query\":{\"nested\":{\"path\":\""
            + nestedPath
            + "\","
            + "\"query\":{\"match\":{\""
            + nestedField
            + "\":\""
            + queryText
            + "\"}},"
            + "\"inner_hits\":{\"size\":5,\"highlight\":{\"fields\":{\""
            + nestedField
            + "\":{\"type\":\"semantic\"}},\"options\":{\"model_id\":\""
            + modelId
            + "\"}}}}},"
            + "\"ext\":{\"semantic_highlighting_batch\":true}"
            + "}";
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
