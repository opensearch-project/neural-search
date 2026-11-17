/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.RELATION_GREATER_THAN_OR_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;

/**
 * {@code hits.total} of a hybrid query when a shard's hit count lands exactly on {@code track_total_hits}.
 *
 * <p>The hybrid collapse collectors stop collecting once the threshold is reached. The relation must only become
 * {@code gte} when more documents match than the threshold, which is what Lucene's collectors and a non-hybrid query
 * report for the same request.
 */
public class HybridQueryTotalHitsThresholdIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-hybrid-total-hits-threshold";
    private static final String SEARCH_PIPELINE = "test-hybrid-total-hits-threshold-pipeline";
    private static final String TEXT_FIELD = "title";
    private static final String GROUP_FIELD = "grp";
    private static final int DOCS = 24;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        createSearchPipeline(SEARCH_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        prepareIndex();
    }

    @SneakyThrows
    private void prepareIndex() {
        if (indexExists(INDEX)) {
            return;
        }
        createIndexWithConfiguration(
            INDEX,
            "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                + TEXT_FIELD
                + "\":{\"type\":\"text\"},\""
                + GROUP_FIELD
                + "\":{\"type\":\"keyword\"}}}}",
            ""
        );
        StringBuilder bulk = new StringBuilder();
        for (int i = 1; i <= DOCS; i++) {
            String title = "spindle" + " w".repeat(i % 6) + (i % 3 == 0 ? " motor" : "");
            bulk.append("{\"index\":{\"_index\":\"").append(INDEX).append("\",\"_id\":\"").append(i).append("\"}}\n");
            bulk.append("{\"")
                .append(TEXT_FIELD)
                .append("\":\"")
                .append(title)
                .append("\",\"")
                .append(GROUP_FIELD)
                .append("\":\"g")
                .append(i % 4)
                .append("\"}\n");
        }
        Request request = new Request("POST", "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private static String collapsedHybridSearch(String trackTotalHits) {
        return "{\"size\":3,\"_source\":false,\"track_total_hits\":"
            + trackTotalHits
            + ",\"collapse\":{\"field\":\""
            + GROUP_FIELD
            + "\"},\"query\":{\"hybrid\":{\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"spindle\"}},{\"match\":{\""
            + TEXT_FIELD
            + "\":\"motor\"}}]}}}";
    }

    @SneakyThrows
    private Map<String, Object> searchTotal(String body) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.addParameter("search_pipeline", SEARCH_PIPELINE);
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        return getTotalHits(responseMap);
    }

    public void testCollapse_whenMoreDocsMatchThanTrackTotalHits_thenRelationIsGte() {
        Map<String, Object> total = searchTotal(collapsedHybridSearch("10"));
        assertEquals(10, total.get("value"));
        assertEquals(RELATION_GREATER_THAN_OR_EQUAL_TO, total.get("relation"));
    }

    public void testCollapse_whenMatchingDocsEqualTrackTotalHits_thenRelationIsEq() {
        Map<String, Object> total = searchTotal(collapsedHybridSearch(String.valueOf(DOCS)));
        assertEquals(DOCS, total.get("value"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    public void testCollapse_whenTrackTotalHitsIsTrue_thenRelationIsEq() {
        Map<String, Object> total = searchTotal(collapsedHybridSearch("true"));
        assertEquals(DOCS, total.get("value"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }
}
