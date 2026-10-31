/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getTotalHits;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * {@code pagination_depth} on a fused hybrid query: accepted, so a classic hybrid query keeps its syntax when
 * {@code fusion} is added. Without {@code fusion.window_size} it sizes the fused window, never below the default, and
 * the page ends where the ranked documents end.
 *
 * <p>Both legs score by a unique numeric rank, so classic and fused agree on the order and every assertion about which
 * documents a page holds is exact. Multi-shard on purpose: classic's depth is per shard and the fused window is not, and
 * a single shard would hide the difference.
 */
public class HybridQueryFusedModePaginationDepthIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-hybrid-fused-pagination-depth";
    private static final String PIPELINE = "fused-pagination-depth-norm-pipeline";
    private static final String RANK_FIELD = "rank";
    private static final int SHARDS = 3;
    private static final int DOCS = 150;
    /** Above the default window (100), so the depth is what sizes it, and below the corpus, so pages can pass it. */
    private static final int DEPTH = 120;
    private static final int DEFAULT_WINDOW = 100;
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

    private static String fusedWithDepth(int depth) {
        return hybrid("\"pagination_depth\":" + depth + ",\"fusion\":" + INLINE_FUSION + ",");
    }

    @SneakyThrows
    private Map<String, Object> search(String params, String body) {
        return search(INDEX, params, body);
    }

    @SneakyThrows
    private Map<String, Object> search(String index, String params, String body) {
        return body(searchResponse(index, params, body));
    }

    /**
     * A bent default window is reported in a {@code Warning} header. The REST test client fails a request on any warning
     * by default, so a request expected to carry one is sent permissive and its warnings handed back for assertion.
     */
    @SneakyThrows
    private Response searchResponse(String index, String params, String body) {
        Request request = new Request("POST", "/" + index + "/_search" + params);
        request.setJsonEntity(body);
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return response;
    }

    @SneakyThrows
    private static Map<String, Object> body(Response response) {
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SneakyThrows
    private String searchExpectingBadRequest(String body) {
        return searchExpectingBadRequest(INDEX, body);
    }

    @SneakyThrows
    private String searchExpectingBadRequest(String index, String body) {
        Request request = new Request("POST", "/" + index + "/_search");
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
        List<String> ids = new ArrayList<>();
        for (int rank = highest; rank >= lowest; rank--) {
            ids.add(String.valueOf(rank));
        }
        return ids;
    }

    /**
     * Without {@code window_size} the depth sizes the window: a Top-only request ({@code track_total_hits: false}) asking for
     * the whole corpus gets the depth's worth of ranked documents. A depth below the default does not lower the window,
     * {@code window_size} beside a depth wins, and with neither the window is the default.
     */
    public void testPaginationDepth_whenNoWindowSize_thenItSizesTheFusedWindow() {
        ensureIndex();
        String wholeCorpusTopOnly = "{\"track_total_hits\":false,\"size\":" + DOCS + ",\"query\":";

        Map<String, Object> depthSized = search("", wholeCorpusTopOnly + fusedWithDepth(DEPTH) + "}");
        assertEquals(ranks(DOCS, DOCS - DEPTH + 1), ids(depthSized));
        assertTrue("every ranked document carries a fused score", scores(depthSized).stream().allMatch(score -> score > 0.0));

        assertEquals(
            "a depth below the default does not lower the window",
            ranks(DOCS, DOCS - DEFAULT_WINDOW + 1),
            ids(search("", wholeCorpusTopOnly + fusedWithDepth(10) + "}"))
        );

        String windowWins = hybrid(
            "\"pagination_depth\":" + DEPTH + ",\"fusion\":{\"window_size\":15,\"normalization\":{\"technique\":\"min_max\"}},"
        );
        assertEquals(ranks(DOCS, DOCS - 15 + 1), ids(search("", wholeCorpusTopOnly + windowWins + "}")));

        String neither = hybrid("\"fusion\":" + INLINE_FUSION + ",");
        assertEquals(ranks(DOCS, DOCS - DEFAULT_WINDOW + 1), ids(search("", wholeCorpusTopOnly + neither + "}")));
    }

    /**
     * The documentation's own pagination example — {@code pagination_depth: 10}, pages {@code from: 0} and {@code from: 6}
     * of {@code size: 5} — runs fused by adding {@code "fusion": "pipeline"} and nothing else, and returns the same pages
     * as classic.
     */
    @SneakyThrows
    public void testPaginationDepth_whenClassicBodyAddsFusion_thenTheDocumentedPagesMatchClassic() {
        ensureIndex();
        createSearchPipeline(PIPELINE, "min_max", "arithmetic_mean", Map.of());
        String params = "?search_pipeline=" + PIPELINE;
        String classic = hybrid("\"pagination_depth\":10,");
        String fused = hybrid("\"pagination_depth\":10,\"fusion\":\"pipeline\",");

        for (int from : new int[] { 0, 6 }) {
            String page = "{\"from\":" + from + ",\"size\":5,\"query\":";
            List<String> classicPage = ids(search(params, page + classic + "}"));
            Map<String, Object> fusedPage = search(params, page + fused + "}");
            assertEquals(ranks(DOCS - from, DOCS - from - 4), classicPage);
            assertEquals(classicPage, ids(fusedPage));
            assertTrue(scores(fusedPage).stream().allMatch(score -> score > 0.0));
        }
    }

    /**
     * A page that reaches past the ranked documents is short, and one entirely past them is empty — HTTP 200, whether or
     * not totals are tracked. Totals and aggregations still cover the whole match set. The same holds for every fused
     * page, with or without {@code pagination_depth}.
     */
    public void testPaginationDepth_whenPageReachesPastTheRankedDocuments_thenShortThenEmpty() {
        ensureIndex();
        String query = fusedWithDepth(DEPTH);
        // An empty page still reports the top ranked score as max_score, as core does for a page past the matches —
        // whether the trim emptied it (the Tail runs for totals) or round 2 returned nothing past the window (Top-only).
        Object topScore = getNestedHits(search("", "{\"size\":1,\"query\":" + query + "}")).get(0).get("_score");
        for (String totals : new String[] { "", "\"track_total_hits\":false," }) {
            Map<String, Object> straddling = search("", "{" + totals + "\"from\":" + (DEPTH - 2) + ",\"size\":5,\"query\":" + query + "}");
            assertEquals(totals, ranks(DOCS - DEPTH + 2, DOCS - DEPTH + 1), ids(straddling));
            Map<String, Object> beyond = search("", "{" + totals + "\"from\":" + DEPTH + ",\"size\":5,\"query\":" + query + "}");
            assertTrue(totals, ids(beyond).isEmpty());
            assertEquals(totals, topScore, ((Map<String, Object>) beyond.get("hits")).get("max_score"));
        }

        Map<String, Object> beyondWithAggs = search(
            "",
            "{\"from\":"
                + DEPTH
                + ",\"size\":5,\"query\":"
                + query
                + ",\"aggs\":{\"docs\":{\"value_count\":{\"field\":\""
                + RANK_FIELD
                + "\"}}}}"
        );
        assertTrue(ids(beyondWithAggs).isEmpty());
        assertEquals(Map.of("value", DOCS, "relation", "eq"), getTotalHits(beyondWithAggs));
        Map<String, Object> aggregations = (Map<String, Object>) beyondWithAggs.get("aggregations");
        assertEquals(DOCS, ((Number) ((Map<String, Object>) aggregations.get("docs")).get("value")).intValue());

        // Collapsed and profiled pages end at the ranked documents the same way.
        String collapsed = "{\"collapse\":{\"field\":\""
            + RANK_FIELD
            + "\"},\"from\":"
            + (DEPTH - 2)
            + ",\"size\":5,\"query\":"
            + query
            + "}";
        assertEquals(ranks(DOCS - DEPTH + 2, DOCS - DEPTH + 1), ids(search("", collapsed)));
        assertTrue(ids(search("", "{\"profile\":true,\"from\":" + DEPTH + ",\"size\":5,\"query\":" + query + "}")).isEmpty());

        // search_after with a _score-led sort: the page after the last ranked document is empty too.
        String sorted = "\"sort\":[\"_score\",{\"" + RANK_FIELD + "\":\"asc\"}],";
        List<Map<String, Object>> rankedPage = getNestedHits(search("", "{" + sorted + "\"size\":" + DEPTH + ",\"query\":" + query + "}"));
        assertEquals(DEPTH, rankedPage.size());
        List<Object> lastSortValues = (List<Object>) rankedPage.get(DEPTH - 1).get("sort");
        String cursor = lastSortValues.stream().map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
        assertTrue(ids(search("", "{" + sorted + "\"search_after\":" + cursor + ",\"size\":5,\"query\":" + query + "}")).isEmpty());

        // Not only with pagination_depth: a window_size-only or default-window page past the ranked documents is empty too,
        // which is what lets fusion.window_size: max(depth, 100) reproduce a pagination_depth request exactly.
        String windowOnly = hybrid("\"fusion\":{\"window_size\":" + DEPTH + ",\"normalization\":{\"technique\":\"min_max\"}},");
        assertEquals(
            ids(search("", "{\"from\":" + (DEPTH - 2) + ",\"size\":5,\"query\":" + query + "}")),
            ids(search("", "{\"from\":" + (DEPTH - 2) + ",\"size\":5,\"query\":" + windowOnly + "}"))
        );
        assertTrue(ids(search("", "{\"from\":" + DEPTH + ",\"size\":5,\"query\":" + windowOnly + "}")).isEmpty());
        String defaultWindow = hybrid("\"fusion\":" + INLINE_FUSION + ",");
        assertTrue(ids(search("", "{\"from\":" + DEFAULT_WINDOW + ",\"size\":5,\"query\":" + defaultWindow + "}")).isEmpty());
    }

    /**
     * The depth gets the window's bounds when it sizes the window, and classic's bound when {@code window_size} overrides
     * it; a non-positive depth fails at parse, since a fused query never reaches the shard where classic checks it.
     */
    public void testPaginationDepth_whenOutOfBounds_thenBadRequestNamingTheDepth() {
        ensureIndex();
        String aboveClauseCeiling = searchExpectingBadRequest("{\"query\":" + fusedWithDepth(1024) + "}");
        assertTrue(aboveClauseCeiling, aboveClauseCeiling.contains("[hybrid] query [pagination_depth] (1024) must be less than"));
        assertTrue(aboveClauseCeiling, aboveClauseCeiling.contains("indices.query.bool.max_clause_count"));

        String aboveMaxResultWindow = searchExpectingBadRequest("{\"query\":" + fusedWithDepth(10001) + "}");
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

        String zero = searchExpectingBadRequest("{\"query\":" + fusedWithDepth(0) + "}");
        assertTrue(zero, zero.contains("[hybrid] query [pagination_depth] must be greater than 0"));
    }

    /**
     * On an index whose {@code index.max_result_window} is below the default window, the default — and the floor a smaller
     * depth is raised to — bends to that ceiling instead of failing the request: the classic body with
     * {@code pagination_depth: 10} keeps working when it adds {@code fusion}, exactly as it works without it, and so does a
     * fused request with no depth. What the user wrote is still refused by name. End to end, because the ceiling is also
     * core's own {@code from + size} limit on the legs, which the clamped window has to satisfy on every shard.
     */
    @SneakyThrows
    public void testPaginationDepth_whenTheIndexCeilingIsBelowTheDefaultWindow_thenTheDefaultBendsToIt() {
        String smallWindowIndex = INDEX + "-max-result-window-50";
        if (indexExists(smallWindowIndex) == false) {
            createIndex(
                smallWindowIndex,
                "{\"settings\":{\"number_of_shards\":"
                    + SHARDS
                    + ",\"number_of_replicas\":0,\"index.max_result_window\":50},\"mappings\":{\"properties\":{\""
                    + RANK_FIELD
                    + "\":{\"type\":\"integer\"}}}}"
            );
            StringBuilder bulk = new StringBuilder();
            for (int id = 1; id <= 60; id++) {
                bulk.append("{\"index\":{\"_id\":\"").append(id).append("\"}}\n");
                bulk.append("{\"").append(RANK_FIELD).append("\":").append(id).append("}\n");
            }
            Request request = new Request("POST", "/" + smallWindowIndex + "/_bulk?refresh=true");
            request.setJsonEntity(bulk.toString());
            assertEquals(RestStatus.OK, RestStatus.fromCode(client().performRequest(request).getStatusLine().getStatusCode()));
        }
        String topOnlyWholeIndex = "{\"track_total_hits\":false,\"size\":50,\"query\":";

        // classic, the documentation's pagination_depth: 10 — works on this index
        createSearchPipeline(PIPELINE, "min_max", "arithmetic_mean", Map.of());
        List<String> classic = ids(
            search(smallWindowIndex, "?search_pipeline=" + PIPELINE, "{\"size\":5,\"query\":" + hybrid("\"pagination_depth\":10,") + "}")
        );
        assertEquals(ranks(60, 56), classic);

        // the same body with fusion: the floor bends to 50 instead of failing about a window_size the request never set, and
        // says so in a Warning header, naming the ceiling and the knob — the shallower ranking is otherwise a normal 200
        Response bent = searchResponse(smallWindowIndex, "", topOnlyWholeIndex + fusedWithDepth(10) + "}");
        assertEquals("the floor bends to the ceiling: 50 ranked documents, not a 400", ranks(60, 11), ids(body(bent)));
        assertEquals(
            List.of(
                "[hybrid] query [fusion] window lowered from 100 to 50 by [index.max_result_window] (50) of index ["
                    + smallWindowIndex
                    + "]; set [fusion.window_size] to choose it"
            ),
            bent.getWarnings()
        );
        assertEquals(classic, ids(search(smallWindowIndex, "", "{\"size\":5,\"query\":" + fusedWithDepth(10) + "}")));

        // and so does the plain default
        assertEquals(
            ranks(60, 11),
            ids(search(smallWindowIndex, "", topOnlyWholeIndex + hybrid("\"fusion\":" + INLINE_FUSION + ",") + "}"))
        );

        // a window the user chose — window_size, or a depth at the ceiling — is not a bent default: no warning
        Response chosen = searchResponse(
            smallWindowIndex,
            "",
            topOnlyWholeIndex + hybrid("\"fusion\":{\"window_size\":50,\"normalization\":{\"technique\":\"min_max\"}},") + "}"
        );
        assertEquals(ranks(60, 11), ids(body(chosen)));
        assertEquals("an explicit window_size is the user's choice", List.of(), chosen.getWarnings());
        assertEquals(
            "so is a depth at the ceiling",
            List.of(),
            searchResponse(smallWindowIndex, "", topOnlyWholeIndex + fusedWithDepth(50) + "}").getWarnings()
        );

        // what the user wrote is still refused, naming it
        String depthOverCeiling = searchExpectingBadRequest(smallWindowIndex, "{\"query\":" + fusedWithDepth(60) + "}");
        assertTrue(
            depthOverCeiling,
            depthOverCeiling.contains("[hybrid] query [pagination_depth] (60) must be less than or equal to [index.max_result_window] (50)")
        );
        String windowOverCeiling = searchExpectingBadRequest(
            smallWindowIndex,
            "{\"query\":" + hybrid("\"fusion\":{\"window_size\":70,\"normalization\":{\"technique\":\"min_max\"}},") + "}"
        );
        assertTrue(
            windowOverCeiling,
            windowOverCeiling.contains(
                "[hybrid] query [fusion.window_size] (70) must be less than or equal to [index.max_result_window] (50)"
            )
        );
    }
}
