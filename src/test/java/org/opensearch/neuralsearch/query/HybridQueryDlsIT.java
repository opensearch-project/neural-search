/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.HttpHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

/**
 * Cross-plugin coverage for DLS applied to a Neural Search hybrid query.
 *
 * @see <a href="https://github.com/opensearch-project/neural-search/issues/1303">neural-search#1303</a>
 */
public class HybridQueryDlsIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "hybrid-query-dls-test";
    private static final String SEARCH_PIPELINE_NAME = "hybrid-query-dls-test-pipeline";
    private static final String ROLE_NAME = "hybrid_query_dls_test_role";
    private static final String USER_NAME = "hybrid_query_dls_test_user";
    private static final String USER_PASSWORD = "HybridQueryDlsTest1!";
    private static final String KNN_VECTOR_FIELD = "knn_embedding";
    private static final String NEURAL_VECTOR_FIELD = "neural_embedding";
    private static final int NEURAL_VECTOR_DIMENSION = 768;
    private static final String DLS_QUERY_JSON = "{\"term\":{\"access\":\"allowed\"}}";
    private static final Set<String> ALL_DOCUMENT_IDS = Set.of("allowed-alpha", "allowed-beta", "blocked-alpha", "blocked-beta");
    private static final Set<String> ALLOWED_DOCUMENT_IDS = Set.of("allowed-alpha", "allowed-beta");
    private static final Set<String> PUBLIC_DOCUMENT_IDS = Set.of("allowed-alpha", "blocked-alpha", "blocked-beta");
    private static final Set<String> PUBLIC_ALLOWED_DOCUMENT_IDS = Set.of("allowed-alpha");

    @BeforeClass
    public static void requireSecurityPlugin() {
        assumeTrue("requires the Security plugin", isSecurityPluginEnabled());
    }

    @Before
    public void setUpDlsResources() throws IOException {
        createIndexAndDocuments();

        assertOk(performAdminJsonRequest("PUT", "/_search/pipeline/" + SEARCH_PIPELINE_NAME, """
            {
              "description": "Normalize hybrid query results for DLS testing",
              "phase_results_processors": [
                {
                  "normalization-processor": {
                    "normalization": { "technique": "min_max" },
                    "combination": { "technique": "arithmetic_mean" }
                  }
                }
              ]
            }
            """));
        assertCreatedOrOk(performAdminJsonRequest("PUT", "/_plugins/_security/api/roles/" + ROLE_NAME, roleBody()));
        assertCreatedOrOk(performAdminJsonRequest("PUT", "/_plugins/_security/api/internalusers/" + USER_NAME, userBody()));
    }

    @After
    public void cleanUpResources() throws IOException {
        if (!isSecurityPluginEnabled()) {
            return;
        }
        deleteObjects(
            List.of(
                "/_plugins/_security/api/internalusers/" + USER_NAME,
                "/_plugins/_security/api/roles/" + ROLE_NAME,
                "/_search/pipeline/" + SEARCH_PIPELINE_NAME,
                "/" + INDEX_NAME
            )
        );
    }

    private static boolean isSecurityPluginEnabled() {
        return Boolean.parseBoolean(System.getProperty("security.enabled", "false"));
    }

    public void testDlsFiltersHybridHitsAndAggregations() throws Exception {
        Map<String, Object> adminResponse = performSearch(primaryHybridRequest(), false);
        assertHitIds(adminResponse, ALL_DOCUMENT_IDS);
        assertGlobalAccessBuckets(adminResponse, Map.of("allowed", 2, "blocked", 2));

        Map<String, Object> restrictedResponse = performSearch(primaryHybridRequest(), true);
        assertHitIds(restrictedResponse, ALLOWED_DOCUMENT_IDS);
        assertGlobalAccessBuckets(restrictedResponse, Map.of("allowed", 2));
    }

    public void testDlsSupportsSuggestionBearingHybridRequest() throws Exception {
        Map<String, Object> adminResponseWithSuggest = performSearch(hybridRequestWithSuggest(), false);
        assertHitIds(adminResponseWithSuggest, ALL_DOCUMENT_IDS);
        assertSuggestionContains(adminResponseWithSuggest, "allowed-label-suggest", "document");

        Map<String, Object> restrictedResponseWithSuggest = performSearch(hybridRequestWithSuggest(), true);
        assertHitIds(restrictedResponseWithSuggest, ALLOWED_DOCUMENT_IDS);
        // This verifies that a suggestion-bearing hybrid request remains compatible with DLS. Term-dictionary candidate
        // filtering is existing Security plugin behavior and is outside this regression test's scope, so the absence of
        // suggestions sourced only from blocked documents is intentionally not asserted here.
        assertSuggestionContains(restrictedResponseWithSuggest, "allowed-label-suggest", "document");
    }

    public void testDlsCombinesWithExistingHybridFilter() throws Exception {
        Map<String, Object> responseWithHybridFilter = performSearch(hybridRequestWithFilter(), true);
        assertHitIds(responseWithHybridFilter, Set.of("allowed-alpha"));
    }

    public void testDlsSupportsSingleClauseHybridQuery() throws Exception {
        Map<String, Object> singleClauseResponse = performSearch(singleClauseHybridRequest(), true);
        assertHitIds(singleClauseResponse, Set.of("allowed-alpha"));
    }

    public void testDlsFiltersHybridKnnQuery() throws Exception {
        Map<String, Object> adminKnnResponse = performSearch(hybridKnnRequest(), false);
        assertHitIds(adminKnnResponse, PUBLIC_DOCUMENT_IDS);

        Map<String, Object> restrictedKnnResponse = performSearch(hybridKnnRequest(), true);
        assertHitIds(restrictedKnnResponse, PUBLIC_ALLOWED_DOCUMENT_IDS);
    }

    public void testDlsFiltersHybridNeuralQuery() throws Exception {
        String modelId = prepareModelForNeuralQuery();
        Map<String, Object> adminNeuralResponse = performSearch(hybridNeuralRequest(modelId), false);
        assertHitIds(adminNeuralResponse, PUBLIC_DOCUMENT_IDS);

        Map<String, Object> restrictedNeuralResponse = performSearch(hybridNeuralRequest(modelId), true);
        assertHitIds(restrictedNeuralResponse, PUBLIC_ALLOWED_DOCUMENT_IDS);

        Map<String, Object> adminMixedResponse = performSearch(hybridKnnAndNeuralRequest(modelId), false);
        assertHitIds(adminMixedResponse, PUBLIC_DOCUMENT_IDS);

        Map<String, Object> restrictedMixedResponse = performSearch(hybridKnnAndNeuralRequest(modelId), true);
        assertHitIds(restrictedMixedResponse, PUBLIC_ALLOWED_DOCUMENT_IDS);
    }

    private void deleteObjects(List<String> endpoints) throws IOException {
        IOException cleanupFailure = null;
        for (String endpoint : endpoints) {
            try {
                deleteObject(endpoint);
            } catch (IOException e) {
                if (cleanupFailure == null) {
                    cleanupFailure = e;
                } else {
                    cleanupFailure.addSuppressed(e);
                }
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private void createIndexAndDocuments() throws IOException {
        Response createIndexResponse = performAdminJsonRequest("PUT", "/" + INDEX_NAME, """
            {
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.knn": true
              },
              "mappings": {
                "properties": {
                  "access": { "type": "keyword" },
                  "signal": { "type": "keyword" },
                  "visibility": { "type": "keyword" },
                  "label": { "type": "text" },
                  "knn_embedding": {
                    "type": "knn_vector",
                    "dimension": 2,
                    "method": { "name": "hnsw", "space_type": "l2", "engine": "lucene" }
                  },
                  "neural_embedding": {
                    "type": "knn_vector",
                    "dimension": 768,
                    "method": { "name": "hnsw", "space_type": "l2", "engine": "lucene" }
                  }
                }
              }
            }
            """);
        assertEquals(RestStatus.OK.getStatus(), createIndexResponse.getStatusLine().getStatusCode());

        indexDocument("allowed-alpha", "allowed", "alpha", "public", "document", List.of(1.0f, 0.0f), 0.1f);
        indexDocument("allowed-beta", "allowed", "beta", "private", "manual", List.of(0.9f, 0.1f), 0.2f);
        indexDocument("blocked-alpha", "blocked", "alpha", "public", "confidential", List.of(0.0f, 1.0f), 0.3f);
        indexDocument("blocked-beta", "blocked", "beta", "public", "classified", List.of(0.1f, 0.9f), 0.4f);
        Response refreshResponse = client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh"));
        assertOk(refreshResponse);
        assertNoFailedShards(responseAsMap(refreshResponse));
    }

    private void indexDocument(
        String id,
        String access,
        String signal,
        String visibility,
        String label,
        List<Float> knnVector,
        float neuralVectorValue
    ) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject()
                .field("access", access)
                .field("signal", signal)
                .field("visibility", visibility)
                .field("label", label)
                .field(KNN_VECTOR_FIELD, knnVector)
                .field(NEURAL_VECTOR_FIELD, Collections.nCopies(NEURAL_VECTOR_DIMENSION, neuralVectorValue))
                .endObject();
            assertCreatedOrOk(performAdminJsonRequest("PUT", "/" + INDEX_NAME + "/_doc/" + id, builder.toString()));
        }
    }

    private String prepareModelForNeuralQuery() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        String modelId = registerModelGroupAndUploadModel(requestBody);

        updateClusterSettings("plugins.ml_commons.allow_custom_deployment_plan", true);
        Map<String, Object> nodesResponse = responseAsMap(client().performRequest(new Request("GET", "/_nodes/_local")));
        String nodeId = asMap(nodesResponse.get("nodes")).keySet().iterator().next();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field("node_ids", List.of(nodeId)).endObject();
            assertOk(performAdminJsonRequest("POST", "/_plugins/_ml/models/" + modelId + "/_deploy", builder.toString()));
        }
        waitForModelToBeReady(modelId);
        return modelId;
    }

    private Map<String, Object> performSearch(String requestBody, boolean asDlsUser) throws IOException {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_search");
        request.addParameter("search_pipeline", SEARCH_PIPELINE_NAME);
        request.setJsonEntity(requestBody);
        if (asDlsUser) {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            String credentials = USER_NAME + ":" + USER_PASSWORD;
            options.addHeader(
                HttpHeaders.AUTHORIZATION,
                "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8))
            );
            request.setOptions(options.build());
        }

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return responseAsMap(response);
    }

    private String primaryHybridRequest() {
        return """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    { "term": { "signal": "alpha" } },
                    { "term": { "signal": "beta" } }
                  ]
                }
              },
              "aggs": {
                "all_documents": {
                  "global": {},
                  "aggs": {
                    "visible_access": {
                      "terms": { "field": "access" }
                    }
                  }
                }
              }
            }
            """;
    }

    private String hybridRequestWithSuggest() {
        return """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    { "term": { "signal": "alpha" } },
                    { "term": { "signal": "beta" } }
                  ]
                }
              },
              "suggest": {
                "allowed-label-suggest": {
                  "text": "documnt",
                  "term": { "field": "label" }
                }
              }
            }
            """;
    }

    private String hybridRequestWithFilter() {
        return """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    { "term": { "signal": "alpha" } },
                    { "term": { "signal": "beta" } }
                  ],
                  "filter": { "term": { "visibility": "public" } }
                }
              }
            }
            """;
    }

    private String singleClauseHybridRequest() {
        return """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    { "term": { "signal": "alpha" } }
                  ]
                }
              }
            }
            """;
    }

    private String hybridKnnRequest() {
        return """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    {
                      "knn": {
                        "knn_embedding": {
                          "vector": [1.0, 0.0],
                          "k": 10,
                          "filter": { "term": { "visibility": "public" } }
                        }
                      }
                    }
                  ]
                }
              }
            }
            """;
    }

    private String hybridNeuralRequest(String modelId) {
        return String.format(
            Locale.ROOT,
            """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    {
                      "neural": {
                        "neural_embedding": {
                          "query_text": "document",
                          "model_id": "%s",
                          "k": 10,
                          "filter": { "term": { "visibility": "public" } }
                        }
                      }
                    }
                  ]
                }
              }
            }
            """,
            modelId
        );
    }

    private String hybridKnnAndNeuralRequest(String modelId) {
        return String.format(
            Locale.ROOT,
            """
            {
              "size": 10,
              "query": {
                "hybrid": {
                  "queries": [
                    {
                      "knn": {
                        "knn_embedding": {
                          "vector": [1.0, 0.0],
                          "k": 10,
                          "filter": { "term": { "visibility": "public" } }
                        }
                      }
                    },
                    {
                      "neural": {
                        "neural_embedding": {
                          "query_text": "document",
                          "model_id": "%s",
                          "k": 10,
                          "filter": { "term": { "visibility": "public" } }
                        }
                      }
                    }
                  ]
                }
              }
            }
            """,
            modelId
        );
    }

    private void assertHitIds(Map<String, Object> responseBody, Set<String> expectedIds) {
        assertNoFailedShards(responseBody);

        Map<String, Object> hits = asMap(responseBody.get("hits"));
        Map<String, Object> total = asMap(hits.get("total"));
        assertEquals(expectedIds.size(), ((Number) total.get("value")).intValue());

        List<Map<String, Object>> hitList = asList(hits.get("hits"));
        List<String> hitIds = hitList.stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toList());
        assertEquals("hybrid results must not contain duplicate documents", expectedIds.size(), hitIds.size());
        assertEquals(expectedIds, Set.copyOf(hitIds));
    }

    private void assertGlobalAccessBuckets(Map<String, Object> responseBody, Map<String, Integer> expectedBuckets) {
        assertNotNull("response missing aggregations", responseBody.get("aggregations"));
        Map<String, Object> aggregations = asMap(responseBody.get("aggregations"));
        Map<String, Object> allDocuments = asMap(aggregations.get("all_documents"));
        Map<String, Object> visibleAccess = asMap(allDocuments.get("visible_access"));
        List<Map<String, Object>> buckets = asList(visibleAccess.get("buckets"));
        Map<String, Integer> actualBuckets = buckets.stream()
            .collect(Collectors.toMap(bucket -> (String) bucket.get("key"), bucket -> ((Number) bucket.get("doc_count")).intValue()));

        assertEquals(
            expectedBuckets.values().stream().mapToInt(Integer::intValue).sum(),
            ((Number) allDocuments.get("doc_count")).intValue()
        );
        assertEquals(expectedBuckets, actualBuckets);
    }

    private void assertSuggestionContains(Map<String, Object> responseBody, String suggestionName, String expectedOption) {
        Map<String, Object> suggestions = asMap(responseBody.get("suggest"));
        List<Map<String, Object>> suggestionEntries = asList(suggestions.get(suggestionName));
        assertEquals(1, suggestionEntries.size());

        List<Map<String, Object>> options = asList(suggestionEntries.get(0).get("options"));
        Set<String> actualOptions = options.stream().map(option -> (String) option.get("text")).collect(Collectors.toSet());
        assertTrue(
            "expected suggestion options to contain " + expectedOption + " but got " + actualOptions,
            actualOptions.contains(expectedOption)
        );
    }

    private Response performAdminJsonRequest(String method, String endpoint, String body) throws IOException {
        Request request = new Request(method, endpoint);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private void assertNoFailedShards(Map<String, Object> responseBody) {
        Map<String, Object> shards = asMap(responseBody.get("_shards"));
        assertEquals(0, ((Number) shards.get("failed")).intValue());
    }

    private String roleBody() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject()
                .field("cluster_permissions", List.of("cluster:admin/opensearch/ml/predict"))
                .startArray("index_permissions")
                .startObject()
                .field("index_patterns", List.of(INDEX_NAME))
                .field("allowed_actions", List.of("read"))
                // The Security REST API expects DLS as a JSON-encoded string, not a nested object.
                .field("dls", DLS_QUERY_JSON)
                .endObject()
                .endArray()
                .endObject();
            return builder.toString();
        }
    }

    private String userBody() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field("password", USER_PASSWORD).field("opendistro_security_roles", List.of(ROLE_NAME)).endObject();
            return builder.toString();
        }
    }

    private void assertCreatedOrOk(Response response) {
        int status = response.getStatusLine().getStatusCode();
        assertTrue(
            "expected status 200 or 201 but got " + status,
            status == RestStatus.OK.getStatus() || status == RestStatus.CREATED.getStatus()
        );
    }

    private void assertOk(Response response) {
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

    private void deleteObject(String endpoint) throws IOException {
        try {
            Response response = client().performRequest(new Request("DELETE", endpoint));
            assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != RestStatus.NOT_FOUND.getStatus()) {
                throw e;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> asList(Object value) {
        return (List<Map<String, Object>>) value;
    }
}
