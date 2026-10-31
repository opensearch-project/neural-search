/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

/**
 * Hybrid query with a point in time and a slice. Core sends a sliced point in time search to every shard of the point in
 * time: a shard that is not part of the slice gets a match-no-docs query instead of the request's query, and a shard that is
 * split across slices gets the query wrapped into a boolean query with the slice filter. Each slice has to return its own
 * part of the result without a failed shard, and the slices together have to return the result of the search without a slice.
 */
public class HybridQuerySliceIT extends BaseNeuralSearchIT {
    private static final String TEST_INDEX_MULTIPLE_SHARDS = "test-hybrid-slice-multiple-shards";
    private static final String TEST_INDEX_SINGLE_SHARD = "test-hybrid-slice-single-shard";
    private static final String TEST_FILTERED_ALIAS = "test-hybrid-slice-filtered-alias";
    private static final String TEST_INDEX_WITHOUT_ALIAS_FILTER_FIELD = "test-hybrid-slice-without-alias-filter-field";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-slice-pipeline";
    private static final String TEXT_FIELD = "title";
    private static final String INTEGER_FIELD = "seq";
    private static final int NUMBER_OF_DOCS = 40;
    private static final int MAX_SEQ_IN_FILTERED_ALIAS = 20;
    private static final String HYBRID_QUERY = "{\"hybrid\":{\"queries\":[{\"match\":{\""
        + TEXT_FIELD
        + "\":\"spindle\"}},{\"match\":{\""
        + TEXT_FIELD
        + "\":\"motor\"}}]}}";

    @SneakyThrows
    public void testSlice_whenMultipleShards_thenSlicesPartitionTheResult() {
        createIndexWithDocuments(TEST_INDEX_MULTIPLE_SHARDS, 3);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        // with 2 or 3 slices every shard belongs to a single slice and is excluded from the others, with 5 slices some
        // shards are split across slices and filtered by the slice
        Set<String> result = assertSlicesPartitionTheResult(TEST_INDEX_MULTIPLE_SHARDS, List.of(2, 3, 5));
        assertEquals(NUMBER_OF_DOCS, result.size());
    }

    @SneakyThrows
    public void testSlice_whenSingleShard_thenSlicesPartitionTheResult() {
        createIndexWithDocuments(TEST_INDEX_SINGLE_SHARD, 1);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        Set<String> result = assertSlicesPartitionTheResult(TEST_INDEX_SINGLE_SHARD, List.of(2, 3));
        assertEquals(NUMBER_OF_DOCS, result.size());
    }

    @SneakyThrows
    public void testSlice_whenFilteredAlias_thenSlicesPartitionTheFilteredResult() {
        createIndexWithDocuments(TEST_INDEX_MULTIPLE_SHARDS, 3);
        createIndexAlias(
            TEST_INDEX_MULTIPLE_SHARDS,
            TEST_FILTERED_ALIAS,
            QueryBuilders.rangeQuery(INTEGER_FIELD).lte(MAX_SEQ_IN_FILTERED_ALIAS)
        );
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        Set<String> result = assertSlicesPartitionTheResult(TEST_FILTERED_ALIAS, List.of(2, 5));
        assertEquals(MAX_SEQ_IN_FILTERED_ALIAS, result.size());
        result.forEach(
            id -> assertTrue("document " + id + " is outside of the alias filter", Integer.parseInt(id) <= MAX_SEQ_IN_FILTERED_ALIAS)
        );
    }

    @SneakyThrows
    public void testFilteredAlias_whenFilterMatchesNoDocumentsOfOneIndex_thenNoShardFails() {
        createIndexWithDocuments(TEST_INDEX_MULTIPLE_SHARDS, 3);
        createIndexWithDocuments(TEST_INDEX_WITHOUT_ALIAS_FILTER_FIELD, 1, false);
        // the filter field is not mapped in the second index, so the alias filter matches no documents on its shard
        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity(
            String.format(
                Locale.ROOT,
                "{\"actions\":[{\"add\":{\"indices\":[\"%s\",\"%s\"],\"alias\":\"%s\",\"filter\":{\"range\":{\"%s\":{\"lte\":%d}}}}}]}",
                TEST_INDEX_MULTIPLE_SHARDS,
                TEST_INDEX_WITHOUT_ALIAS_FILTER_FIELD,
                TEST_FILTERED_ALIAS,
                INTEGER_FIELD,
                MAX_SEQ_IN_FILTERED_ALIAS
            )
        );
        client().performRequest(aliasRequest);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        Request request = new Request("POST", "/" + TEST_FILTERED_ALIAS + "/_search");
        request.addParameter("search_pipeline", SEARCH_PIPELINE);
        request.addParameter("allow_partial_search_results", Boolean.FALSE.toString());
        request.setJsonEntity("{\"size\":100,\"_source\":false,\"query\":" + HYBRID_QUERY + "}");
        Response response = client().performRequest(request);
        Map<String, Object> searchResponse = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );

        Map<String, Object> shards = getShards(searchResponse);
        assertEquals("no shard must fail: " + shards, 0, shards.get("failed"));
        Set<String> ids = idsOf(searchResponse);
        assertEquals(MAX_SEQ_IN_FILTERED_ALIAS, ids.size());
        ids.forEach(
            id -> assertTrue("document " + id + " is outside of the alias filter", Integer.parseInt(id) <= MAX_SEQ_IN_FILTERED_ALIAS)
        );
    }

    /**
     * Runs the hybrid query over one point in time without a slice and then once per slice for every given number of slices,
     * and returns the ids of the search without a slice.
     */
    @SneakyThrows
    private Set<String> assertSlicesPartitionTheResult(final String target, final List<Integer> numbersOfSlices) {
        String pitId = createPointInTime(target);
        try {
            Set<String> result = idsOf(searchOverPointInTime(pitId, null));
            for (int max : numbersOfSlices) {
                Set<String> resultOfSlices = new HashSet<>();
                for (int id = 0; id < max; id++) {
                    Map<String, Object> response = searchOverPointInTime(
                        pitId,
                        String.format(Locale.ROOT, "{\"id\":%d,\"max\":%d}", id, max)
                    );
                    Map<String, Object> shards = getShards(response);
                    assertEquals("slice " + id + " of " + max + " must not fail any shard: " + shards, 0, shards.get("failed"));
                    for (String docId : idsOf(response)) {
                        assertTrue("document " + docId + " is returned by more than one slice of " + max, resultOfSlices.add(docId));
                    }
                }
                assertEquals("the " + max + " slices together must return the result of the search without slice", result, resultOfSlices);
            }
            return result;
        } finally {
            deletePointInTime(pitId);
        }
    }

    @SneakyThrows
    private Map<String, Object> searchOverPointInTime(final String pitId, final String slice) {
        Request request = new Request("POST", "/_search");
        request.addParameter("search_pipeline", SEARCH_PIPELINE);
        // a failed shard has to fail the test instead of being reported as a partial result
        request.addParameter("allow_partial_search_results", Boolean.FALSE.toString());
        String body = "{\"size\":100,\"_source\":false,\"query\":"
            + HYBRID_QUERY
            + ",\"pit\":{\"id\":\""
            + pitId
            + "\",\"keep_alive\":\"5m\"}"
            + (Objects.isNull(slice) ? "" : ",\"slice\":" + slice)
            + "}";
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    private void createIndexWithDocuments(final String indexName, final int numberOfShards) {
        createIndexWithDocuments(indexName, numberOfShards, true);
    }

    @SneakyThrows
    private void createIndexWithDocuments(final String indexName, final int numberOfShards, final boolean withIntegerField) {
        Request createIndexRequest = new Request("PUT", "/" + indexName);
        String integerFieldMapping = withIntegerField ? String.format(Locale.ROOT, ",\"%s\":{\"type\":\"integer\"}", INTEGER_FIELD) : "";
        createIndexRequest.setJsonEntity(
            String.format(
                Locale.ROOT,
                "{\"settings\":{\"index\":{\"number_of_shards\":%d,\"number_of_replicas\":0}},"
                    + "\"mappings\":{\"dynamic\":\"strict\",\"properties\":{\"%s\":{\"type\":\"text\"}%s}}}",
                numberOfShards,
                TEXT_FIELD,
                integerFieldMapping
            )
        );
        assertEquals(RestStatus.OK, RestStatus.fromCode(client().performRequest(createIndexRequest).getStatusLine().getStatusCode()));

        StringBuilder bulkBody = new StringBuilder();
        for (int seq = 1; seq <= NUMBER_OF_DOCS; seq++) {
            String title = seq % 4 == 0 ? "spindle bearing motor" : "gear shaft motor";
            bulkBody.append(String.format(Locale.ROOT, "{\"index\":{\"_id\":\"%d\"}}\n", seq));
            bulkBody.append(
                withIntegerField
                    ? String.format(Locale.ROOT, "{\"%s\":\"%s\",\"%s\":%d}\n", TEXT_FIELD, title, INTEGER_FIELD, seq)
                    : String.format(Locale.ROOT, "{\"%s\":\"%s\"}\n", TEXT_FIELD, title)
            );
        }
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.addParameter("refresh", Boolean.TRUE.toString());
        bulkRequest.setJsonEntity(bulkBody.toString());
        Response bulkResponse = client().performRequest(bulkRequest);
        Map<String, Object> bulkResult = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(bulkResponse.getEntity()),
            false
        );
        assertEquals(Boolean.FALSE, bulkResult.get("errors"));
    }

    @SneakyThrows
    private String createPointInTime(final String target) {
        Request request = new Request("POST", "/" + target + "/_search/point_in_time");
        request.addParameter("keep_alive", "5m");
        Response response = client().performRequest(request);
        Map<String, Object> body = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        String pitId = (String) body.get("pit_id");
        assertNotNull("pit_id must be returned", pitId);
        return pitId;
    }

    @SneakyThrows
    private void deletePointInTime(final String pitId) {
        Request request = new Request("DELETE", "/_search/point_in_time");
        request.setJsonEntity("{\"pit_id\":[\"" + pitId + "\"]}");
        client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getShards(final Map<String, Object> response) {
        return (Map<String, Object>) response.get("_shards");
    }

    private Set<String> idsOf(final Map<String, Object> response) {
        List<Map<String, Object>> hits = getNestedHits(response);
        Set<String> ids = new HashSet<>();
        for (Map<String, Object> hit : hits) {
            ids.add((String) hit.get("_id"));
        }
        assertEquals("a document is returned more than once in one response", hits.size(), ids.size());
        return ids;
    }
}
