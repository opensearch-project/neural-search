/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getTotalHits;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * {@code pagination_depth} on a fused hybrid query: accepted, so a classic hybrid query keeps its syntax when
 * {@code fusion} is added, and used as the fused window when {@code fusion.window_size} is unset.
 *
 * <p>Both legs score by a unique numeric rank, so classic and fused agree on the order and every assertion about which
 * documents a page holds is exact. Multi-shard on purpose: classic's depth is per shard and the fused window is not, which
 * is the one way the two differ here, and a single shard would hide it.
 */
public class HybridQueryFusedModePaginationDepthIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-hybrid-fused-pagination-depth";
    private static final String PIPELINE = "fused-pagination-depth-norm-pipeline";
    private static final String RANK_FIELD = "rank";
    private static final int SHARDS = 3;
    private static final int DOCS = 30;
    private static final int DEPTH = 10;
    private static final String INLINE_FUSION = "{\"normalization\":{\"technique\":\"min_max\"},"
        + "\"combination\":{\"technique\":\"arithmetic_mean\"}}";

    @SneakyThrows
    private void ensureIndex() {
        if (indexExists(INDEX)) {
            return;
        }
        createIndex(
            INDEX,
            "{\"settings\":{\"number_of_shards\":"
                + SHARDS
                + ",\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                + RANK_FIELD
                + "\":{\"type\":\"integer\"}}}}"
        );
        StringBuilder bulk = new StringBuilder();
        for (int id = 1; id <= DOCS; id++) {
            bulk.append("{\"index\":{\"_id\":\"").append(id).append("\"}}\n");
            bulk.append("{\"").append(RANK_FIELD).append("\":").append(id).append("}\n");
        }
        Request request = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        assertFalse(EntityUtils.toString(response.getEntity()).contains("\"errors\":true"));
    }

    /** A two-leg hybrid query, both legs scoring by rank, with {@code fields} (e.g. depth, fusion) ahead of the legs. */
    private static String hybrid(String fields) {
        String leg = "{\"function_score\":{\"query\":{\"match_all\":{}},\"field_value_factor\":{\"field\":\""
            + RANK_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
        return "{\"hybrid\":{" + fields + "\"queries\":[" + leg + "," + leg + "]}}";
    }

    @SneakyThrows
    private Map<String, Object> search(String params, String body) {
        Request request = new Request("POST", "/" + INDEX + "/_search" + params);
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SneakyThrows
    private String searchExpectingBadRequest(String body) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(body);
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        return EntityUtils.toString(exception.getResponse().getEntity());
    }

    private static List<String> ids(Map<String, Object> response) {
        return getNestedHits(response).stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toList());
    }

    private static List<Double> scores(Map<String, Object> response) {
        return getNestedHits(response).stream().map(hit -> ((Number) hit.get("_score")).doubleValue()).collect(Collectors.toList());
    }

    /** The ids of ranks {@code highest} down to {@code lowest}, which is the order both modes return them in. */
    private static List<String> ranks(int highest, int lowest) {
        List<String> ids = new java.util.ArrayList<>();
        for (int rank = highest; rank >= lowest; rank--) {
            ids.add(String.valueOf(rank));
        }
        return ids;
    }

    /**
     * Without {@code window_size} the depth is the window: a Top-only request ({@code track_total_hits: false}) asking for
     * the whole corpus gets exactly the depth's worth of ranked documents. With {@code window_size} beside it, the window
     * is {@code window_size}; with neither, it is the default, which covers this corpus.
     */
    public void testPaginationDepth_whenNoWindowSize_thenItIsTheFusedWindow() {
        ensureIndex();
        String depthOnly = hybrid("\"pagination_depth\":" + DEPTH + ",\"fusion\":" + INLINE_FUSION + ",");
        Map<String, Object> response = search("", "{\"track_total_hits\":false,\"size\":" + DOCS + ",\"query\":" + depthOnly + "}");
        assertEquals(ranks(DOCS, DOCS - DEPTH + 1), ids(response));
        assertTrue("every ranked document carries a fused score", scores(response).stream().allMatch(score -> score > 0.0));

        String windowWins = hybrid(
            "\"pagination_depth\":" + DEPTH + ",\"fusion\":{\"window_size\":15,\"normalization\":{\"technique\":\"min_max\"}},"
        );
        assertEquals(
            ranks(DOCS, DOCS - 15 + 1),
            ids(search("", "{\"track_total_hits\":false,\"size\":" + DOCS + ",\"query\":" + windowWins + "}"))
        );

        String neither = hybrid("\"fusion\":" + INLINE_FUSION + ",");
        assertEquals(DOCS, ids(search("", "{\"track_total_hits\":false,\"size\":" + DOCS + ",\"query\":" + neither + "}")).size());
    }

    /**
     * A classic body with {@code pagination_depth} runs fused by adding {@code "fusion": "pipeline"} and nothing else, and
     * pages within the depth the same way: the same first page as classic, then the next page of the ranking.
     */
    @SneakyThrows
    public void testPaginationDepth_whenClassicBodyAddsFusion_thenPagesWithinTheDepthMatchClassic() {
        ensureIndex();
        createSearchPipeline(PIPELINE, "min_max", "arithmetic_mean", Map.of());
        String params = "?search_pipeline=" + PIPELINE;
        String classic = hybrid("\"pagination_depth\":" + DEPTH + ",");
        String fused = hybrid("\"pagination_depth\":" + DEPTH + ",\"fusion\":\"pipeline\",");

        List<String> classicPageOne = ids(search(params, "{\"from\":0,\"size\":5,\"query\":" + classic + "}"));
        List<String> classicPageTwo = ids(search(params, "{\"from\":5,\"size\":5,\"query\":" + classic + "}"));
        Map<String, Object> fusedPageOne = search(params, "{\"from\":0,\"size\":5,\"query\":" + fused + "}");
        Map<String, Object> fusedPageTwo = search(params, "{\"from\":5,\"size\":5,\"query\":" + fused + "}");

        assertEquals(ranks(DOCS, DOCS - 4), classicPageOne);
        assertEquals(classicPageOne, ids(fusedPageOne));
        assertEquals(classicPageTwo, ids(fusedPageTwo));
        assertTrue(scores(fusedPageTwo).stream().allMatch(score -> score > 0.0));
    }

    /**
     * A page that reaches past the depth: the fused window holds the depth's top documents, so the ranked part ends there.
     * Top-only, the page is short and then empty (HTTP 200 — classic instead fails with "Reached end of search result"
     * once {@code from} passes the candidates it collected). With totals tracked, the Tail fills the rest at score 0.
     */
    public void testPaginationDepth_whenPageReachesPastTheDepth_thenRankedPartEndsAtTheDepth() {
        ensureIndex();
        String query = hybrid("\"pagination_depth\":" + DEPTH + ",\"fusion\":" + INLINE_FUSION + ",");

        Map<String, Object> straddling = search("", "{\"track_total_hits\":false,\"from\":8,\"size\":5,\"query\":" + query + "}");
        assertEquals(ranks(DOCS - 8, DOCS - DEPTH + 1), ids(straddling));
        Map<String, Object> beyond = search("", "{\"track_total_hits\":false,\"from\":" + DEPTH + ",\"size\":5,\"query\":" + query + "}");
        assertTrue(ids(beyond).isEmpty());

        Map<String, Object> tailFilled = search("", "{\"from\":" + DEPTH + ",\"size\":5,\"query\":" + query + "}");
        assertEquals(Map.of("value", DOCS, "relation", "eq"), getTotalHits(tailFilled));
        assertEquals(5, ids(tailFilled).size());
        assertTrue("past the window every hit is a Tail match at score 0", scores(tailFilled).stream().allMatch(score -> score == 0.0));
        Set<String> window = new HashSet<>(ranks(DOCS, DOCS - DEPTH + 1));
        assertTrue("no ranked document repeats past the window", ids(tailFilled).stream().noneMatch(window::contains));
    }

    /**
     * The depth gets the window's bounds when it is the window, and classic's bound when {@code window_size} overrides it;
     * a non-positive depth fails at parse, since a fused query never reaches the shard where classic checks it.
     */
    public void testPaginationDepth_whenOutOfBounds_thenBadRequestNamingTheDepth() {
        ensureIndex();
        String aboveClauseCeiling = searchExpectingBadRequest(
            "{\"query\":" + hybrid("\"pagination_depth\":1024,\"fusion\":" + INLINE_FUSION + ",") + "}"
        );
        assertTrue(aboveClauseCeiling, aboveClauseCeiling.contains("[hybrid] query [pagination_depth] (1024) must be less than"));
        assertTrue(aboveClauseCeiling, aboveClauseCeiling.contains("indices.query.bool.max_clause_count"));

        String aboveMaxResultWindow = searchExpectingBadRequest(
            "{\"query\":" + hybrid("\"pagination_depth\":10001,\"fusion\":" + INLINE_FUSION + ",") + "}"
        );
        assertTrue(
            aboveMaxResultWindow,
            aboveMaxResultWindow.contains(
                "[hybrid] query [pagination_depth] (10001) must be less than or equal to [index.max_result_window]"
            )
        );

        String overriddenAboveMaxResultWindow = searchExpectingBadRequest(
            "{\"query\":" + hybrid("\"pagination_depth\":20000,\"fusion\":{\"window_size\":10},") + "}"
        );
        assertTrue(overriddenAboveMaxResultWindow, overriddenAboveMaxResultWindow.contains("[hybrid] query [pagination_depth] (20000)"));

        String zero = searchExpectingBadRequest("{\"query\":" + hybrid("\"pagination_depth\":0,\"fusion\":" + INLINE_FUSION + ",") + "}");
        assertTrue(zero, zero.contains("[hybrid] query [pagination_depth] must be greater than 0"));
    }
}
