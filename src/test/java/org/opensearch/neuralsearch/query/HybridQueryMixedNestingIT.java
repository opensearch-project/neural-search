/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

/**
 * A hybrid query with a [fusion] block and a hybrid query without one cannot be nested in each other, in either direction.
 * A fused hybrid nested in a fused hybrid stays supported, and a hybrid without [fusion] nested in another one stays
 * rejected.
 */
public class HybridQueryMixedNestingIT extends BaseNeuralSearchIT {
    private static final String TEST_INDEX = "test-hybrid-mixed-nesting";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-mixed-nesting-pipeline";
    private static final String TEXT_FIELD = "title";
    private static final int NUMBER_OF_DOCS = 20;
    private static final String MIXED_NESTING_MESSAGE =
        "a [hybrid] query without [fusion] cannot be nested in a [hybrid] query with [fusion], or the other way around; "
            + "use [fusion] on both";
    private static final String FUSION =
        "\"fusion\":{\"window_size\":10,\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}}";
    private static final String SPINDLE = "{\"match\":{\"" + TEXT_FIELD + "\":\"spindle\"}}";
    private static final String MOTOR = "{\"match\":{\"" + TEXT_FIELD + "\":\"motor\"}}";
    private static final String BEARING = "{\"match\":{\"" + TEXT_FIELD + "\":\"bearing\"}}";
    private static final String TRACK_TOTAL_HITS_FALSE = ",\"track_total_hits\":false";
    private static final String AGGREGATION = ",\"aggs\":{\"doc_count\":{\"value_count\":{\"field\":\"_id\"}}}";

    @SneakyThrows
    public void testNesting_whenHybridWithoutFusionIsNestedInFusedHybrid_thenFail() {
        prepareIndexAndPipeline();
        String query = fused(classic(SPINDLE, MOTOR), BEARING);

        // the outcome used to depend on track_total_hits and aggregations: a wrong ranking, or an unrelated error
        for (String extraBody : List.of("", TRACK_TOTAL_HITS_FALSE, AGGREGATION)) {
            assertMixedNestingRejected(query, extraBody, false);
        }
        // and a search pipeline on the request did not change it, because the inner query's search does not use it
        assertMixedNestingRejected(query, TRACK_TOTAL_HITS_FALSE, true);
    }

    @SneakyThrows
    public void testNesting_whenHybridWithoutFusionIsInsideABoolInAFusedHybridLeg_thenFail() {
        prepareIndexAndPipeline();
        String query = fused("{\"bool\":{\"must\":[" + classic(SPINDLE, MOTOR) + "]}}", BEARING);

        assertMixedNestingRejected(query, TRACK_TOTAL_HITS_FALSE, false);
    }

    @SneakyThrows
    public void testNesting_whenFusedHybridIsNestedInHybridWithoutFusion_thenFail() {
        prepareIndexAndPipeline();
        String query = classic(fused(SPINDLE, MOTOR), BEARING);

        for (String extraBody : List.of("", TRACK_TOTAL_HITS_FALSE, AGGREGATION)) {
            assertMixedNestingRejected(query, extraBody, true);
        }
    }

    @SneakyThrows
    public void testNesting_whenFusedHybridIsNestedInFusedHybrid_thenSuccessful() {
        prepareIndexAndPipeline();

        Map<String, Object> response = search(fused(fused(SPINDLE, MOTOR), BEARING), "", false);

        List<Map<String, Object>> hits = getNestedHits(response);
        assertEquals(10, hits.size());
        assertEquals(0, ((Map<?, ?>) response.get("_shards")).get("failed"));
    }

    @SneakyThrows
    public void testNesting_whenHybridWithoutFusionIsNestedInHybridWithoutFusion_thenFail() {
        prepareIndexAndPipeline();

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> search(classic(classic(SPINDLE, MOTOR), BEARING), "", true)
        );

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody, responseBody.contains("hybrid query cannot be nested in another hybrid query"));
    }

    @SneakyThrows
    public void testNesting_whenHybridWithoutFusionIsInsideAWrapperOrTemplateLegOfFusedHybrid_thenFail() {
        prepareIndexAndPipeline();
        String wrapper = "{\"wrapper\":{\"query\":\""
            + Base64.getEncoder().encodeToString(classic(SPINDLE, MOTOR).getBytes(StandardCharsets.UTF_8))
            + "\"}}";
        String template = "{\"template\":" + classic(SPINDLE, MOTOR) + "}";

        for (String leg : List.of(wrapper, template)) {
            assertMixedNestingRejected(fused(leg, BEARING), TRACK_TOTAL_HITS_FALSE, false);
        }
    }

    @SneakyThrows
    public void testNesting_whenFusedHybridIsInsideAWrapperOrTemplateInHybridWithoutFusion_thenFail() {
        prepareIndexAndPipeline();
        String wrapper = "{\"wrapper\":{\"query\":\""
            + Base64.getEncoder().encodeToString(fused(SPINDLE, MOTOR).getBytes(StandardCharsets.UTF_8))
            + "\"}}";
        String template = "{\"template\":" + fused(SPINDLE, MOTOR) + "}";

        for (String subQuery : List.of(wrapper, template)) {
            assertMixedNestingRejected(classic(subQuery, BEARING), "", true);
        }
    }

    @SneakyThrows
    private void assertMixedNestingRejected(final String query, final String extraBody, final boolean withPipeline) {
        ResponseException exception = expectThrows(ResponseException.class, () -> search(query, extraBody, withPipeline));

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody, responseBody.contains(MIXED_NESTING_MESSAGE));
    }

    @SneakyThrows
    private Map<String, Object> search(final String query, final String extraBody, final boolean withPipeline) {
        Request request = new Request("POST", "/" + TEST_INDEX + "/_search");
        if (withPipeline) {
            request.addParameter("search_pipeline", SEARCH_PIPELINE);
        }
        request.setJsonEntity("{\"size\":10,\"_source\":false,\"query\":" + query + Objects.requireNonNull(extraBody) + "}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    private static String fused(final String... subQueries) {
        return "{\"hybrid\":{" + FUSION + ",\"queries\":[" + String.join(",", subQueries) + "]}}";
    }

    private static String classic(final String... subQueries) {
        return "{\"hybrid\":{\"queries\":[" + String.join(",", subQueries) + "]}}";
    }

    @SneakyThrows
    private void prepareIndexAndPipeline() {
        if (indexExists(TEST_INDEX) == false) {
            Request createIndexRequest = new Request("PUT", "/" + TEST_INDEX);
            createIndexRequest.setJsonEntity(
                String.format(
                    Locale.ROOT,
                    "{\"settings\":{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}},"
                        + "\"mappings\":{\"properties\":{\"%s\":{\"type\":\"text\"}}}}",
                    TEXT_FIELD
                )
            );
            assertEquals(RestStatus.OK, RestStatus.fromCode(client().performRequest(createIndexRequest).getStatusLine().getStatusCode()));
            StringBuilder bulkBody = new StringBuilder();
            for (int id = 1; id <= NUMBER_OF_DOCS; id++) {
                StringBuilder title = new StringBuilder("motor");
                if (id % 4 == 0) {
                    title.append(" spindle");
                }
                if (id % 5 == 0) {
                    title.append(" bearing");
                }
                bulkBody.append(String.format(Locale.ROOT, "{\"index\":{\"_id\":\"%d\"}}\n", id));
                bulkBody.append(String.format(Locale.ROOT, "{\"%s\":\"%s\"}\n", TEXT_FIELD, title));
            }
            Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
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
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
    }
}
