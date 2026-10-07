/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.DEFAULT_MAX_FUSION_LEG_SEARCHES;
import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.MAX_FUSION_LEG_SEARCHES;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * End-to-end coverage of the NESTED fused/resolver hybrid case: a hybrid carrying a {@code fusion} block wrapped inside
 * another compound query ({@code bool}, {@code dis_max}, or another fused hybrid). Classic hybrid cannot nest at all
 * (its top-level-only guard throws), so these behaviors have no classic equivalent — this is capability the coordinator
 * self-erase unlocks.
 *
 * <p><b>The Tail decision is depth-independent.</b> {@code HybridFusionOrchestrator#buildFusedQuery} decides the Tail
 * from the request alone, never from whether the hybrid is top-level or nested. So a nested hybrid whose request wants
 * totals/aggregations/highlight still keeps the Tail ({@code bool.filter: bool{should: legs}}), and {@code total_hits}
 * plus aggregations cover the full leg-union at any depth.
 *
 * <p><b>Ranking stays fuse-then-filter.</b> Only the fused-window Top clauses carry a non-zero score; Tail-only docs
 * score 0 and sort last. An enclosing {@code bool.filter} is a sibling clause and is NOT pushed into the legs before
 * fusion, so the window is the GLOBAL fused top-K and the enclosing filter intersects it at the query phase. To narrow
 * retrieval itself, use the hybrid's own {@code filter} field (which IS pushed into each leg). When the enclosing filter
 * excludes the whole fused window, the Tail still surfaces the matching docs as score-0 hits, so totals/aggregations
 * remain correct rather than collapsing.
 *
 * <p>Determinism: each leg is a {@code function_score} over a numeric field, so the fused window is shard-independent.
 *
 * <p>Dataset (30 docs, ids 1..30): {@code grp} is "A" for ids 1-5 (highest {@code s} — the global fused top-5 window)
 * and "B" for ids 6-30 (never in the window). {@code s = tier*1000 - id}.
 */
public class HybridQueryFusedModeNestedIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-nested";
    private static final String NORM_PIPELINE = "fused-nested-norm-pipeline";
    private static final String GRP_FIELD = "grp";
    private static final String SCORE_FIELD = "s";
    private static final int WINDOW_SIZE = 5;
    private static final int WINDOW_LAST_ID = 5;
    private static final int TOTAL_DOCS = 30;

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + GRP_FIELD
            + "\":{\"type\":\"keyword\"},\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        if (indexExists(INDEX)) {
            return;
        }
        createIndex(INDEX, indexConfig());
        for (int id = 1; id <= TOTAL_DOCS; id++) {
            // grp A = ids 1..5 (highest s, own the top-5 window). grp B = ids 6..30 (never in the window).
            String grp = id <= WINDOW_LAST_ID ? "A" : "B";
            int tier = id <= WINDOW_LAST_ID ? 3 : (id <= 17 ? 2 : 1);
            int s = tier * 1000 - id;
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity("{\"" + GRP_FIELD + "\":\"" + grp + "\",\"" + SCORE_FIELD + "\":" + s + "}");
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(
                "indexing doc " + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    /** A leg that matches ALL docs and scores by the numeric field (deterministic, shard-independent). */
    private String leg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},"
            + "\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    /** A fused hybrid: presence of the {@code fusion} block enables the resolver; {@code window_size} lives inside it. */
    private String fusedHybrid() {
        return fusedHybrid(WINDOW_SIZE);
    }

    private String fusedHybrid(int windowSize) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}}";
    }

    /**
     * A chain of {@code levels} fused hybrids, each holding the level below as its first leg plus one plain leg — so the
     * body grows by one clause per level and declares {@code 2 x levels} leg sub-searches. One level is exactly
     * {@link #fusedHybrid()}.
     */
    private String nestedFusedChain(int levels) {
        String query = leg();
        for (int level = 0; level < levels; level++) {
            query = "{\"hybrid\":{\"fusion\":{\"window_size\":"
                + WINDOW_SIZE
                + ",\"normalization\":{\"technique\":\"min_max\"},"
                + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
                + "\"queries\":["
                + query
                + ","
                + leg()
                + "]}}";
        }
        return query;
    }

    /**
     * Top-level baseline, for contrast with the nested cases: with {@code size <= window_size} the request returns
     * exactly the fused window; with {@code size > window_size} the Tail also exposes union docs (score 0) below it.
     */
    @SneakyThrows
    public void testFused_topLevel_windowThenTail() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "}";

        List<String> windowIds = hitIds(searchRaw(body, WINDOW_SIZE));
        assertEquals("size<=window returns exactly the fused window", WINDOW_SIZE, windowIds.size());
        assertTrue("window is grp A (ids 1..5)", windowIds.stream().allMatch(id -> Integer.parseInt(id) <= WINDOW_LAST_ID));

        List<String> allIds = hitIds(searchRaw(body, 20));
        assertEquals("size>window: the Tail exposes union docs beyond the window", 20, allIds.size());
        assertTrue(
            "the fused window (grp A) still ranks first",
            allIds.subList(0, WINDOW_SIZE).stream().allMatch(id -> Integer.parseInt(id) <= WINDOW_LAST_ID)
        );
    }

    /**
     * Fuse-then-filter with a compatible enclosing filter: the window is entirely grp A, so {@code filter grp:A} keeps
     * all of it.
     */
    @SneakyThrows
    public void testFused_nestedInBool_filterMatchesWindow_keepsAll() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{\"must\":[" + fusedHybrid() + "],\"filter\":[{\"term\":{\"" + GRP_FIELD + "\":\"A\"}}]}}}";

        List<String> ids = hitIds(searchRaw(body, 20));

        assertEquals("filter grp:A intersects the all-A window -> keeps 5", WINDOW_SIZE, ids.size());
    }

    /**
     * Depth-independent Tail with a restrictive enclosing filter: the nested hybrid still matches the full leg-union, so
     * {@code filter grp:B} intersects that union and the 25 grp-B docs survive as score-0 Tail hits (the fused window is
     * all grp A and is filtered out). They are not lost from the result or from {@code total_hits}.
     */
    @SneakyThrows
    public void testFused_nestedInBool_restrictiveFilter_tailSurfacesFullUnion() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{\"must\":["
            + fusedHybrid()
            + "],\"filter\":[{\"term\":{\""
            + GRP_FIELD
            + "\":\"B\"}}]}},\"track_total_hits\":true}";

        Map<String, Object> resp = searchRaw(body, TOTAL_DOCS);

        assertEquals("depth-independent Tail: filter(grpB) intersect full-union = 25 grp-B docs", 25, hitIds(resp).size());
        assertEquals("all surviving hits are grp B", Integer.valueOf(25), grpCounts(resp).getOrDefault("B", 0));
        assertEquals("total_hits counts the full filtered union", 25L, totalHits(resp));
    }

    /**
     * Depth-independent Tail with an outer aggregation: the aggregation covers the full leg-union, not just the fused
     * window — the case that would silently window if nesting forced Top-only.
     */
    @SneakyThrows
    public void testFused_nestedInBool_outerAggregationCoversFullUnion() {
        ensureDataset();
        String body = "{\"query\":{\"bool\":{\"must\":["
            + fusedHybrid()
            + "]}},\"aggregations\":{\"by_grp\":{\"terms\":{\"field\":\""
            + GRP_FIELD
            + "\",\"size\":10}}},\"track_total_hits\":true}";

        Map<String, Object> resp = searchRaw(body, 20);
        Map<String, Integer> grp = grpBuckets(resp);

        assertEquals("nested agg covers grp A", Integer.valueOf(WINDOW_LAST_ID), grp.getOrDefault("A", 0));
        assertEquals("nested agg covers grp B via the depth-independent Tail", Integer.valueOf(25), grp.getOrDefault("B", 0));
        assertEquals("nested agg total is the full union", (long) TOTAL_DOCS, totalHits(resp));
    }

    /** Self-erase composes inside a {@code dis_max}, and the fused-scored window still ranks first. */
    @SneakyThrows
    public void testFused_nestedInDisMax_composesWindowRanksFirst() {
        ensureDataset();
        String body = "{\"query\":{\"dis_max\":{\"queries\":[" + fusedHybrid() + "]}}}";

        List<String> ids = hitIds(searchRaw(body, 20));

        assertFalse("dis_max over fused composes (self-erase resolved)", ids.isEmpty());
        assertTrue(
            "fused window (grp A) ranks first",
            ids.subList(0, Math.min(WINDOW_SIZE, ids.size())).stream().allMatch(id -> Integer.parseInt(id) <= WINDOW_LAST_ID)
        );
    }

    /** A fused hybrid nested as a leg of another fused hybrid — the async round-trip must resolve at both levels. */
    @SneakyThrows
    public void testFused_hybridNestedInHybrid_fusionOfFusion() {
        ensureDataset();
        String body = "{\"query\":{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + fusedHybrid()
            + ","
            + leg()
            + "]}}}";

        List<String> ids = hitIds(searchRaw(body, 20));

        assertFalse("fusion-of-fusion must return hits", ids.isEmpty());
    }

    /**
     * Fusion of fusion where NEITHER level carries an inline config — both read the index's default pipeline. Legs are
     * fanned out with the search pipeline disabled (so per-leg processors do not run), so the nested fused hybrid has no
     * pipeline of its own to read and depends on the enclosing rewrite projecting the config it already resolved.
     * Without that projection this fails claiming no normalization processor is configured — on an index that has one.
     */
    @SneakyThrows
    public void testFused_hybridNestedInHybrid_whenBothLevelsConfigFromPipeline_thenResolves() {
        ensureDataset();
        String pipelineConfiguredHybrid = "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + "},\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}}";
        String body = "{\"query\":{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + "},\"queries\":["
            + pipelineConfiguredHybrid
            + ","
            + leg()
            + "]}}}";

        List<String> ids = hitIds(searchRaw(body, 20));

        assertFalse("a nested fused hybrid must resolve its config from the index default pipeline", ids.isEmpty());
        assertTrue(
            "fused window (grp A) ranks first",
            ids.subList(0, Math.min(WINDOW_SIZE, ids.size())).stream().allMatch(id -> Integer.parseInt(id) <= WINDOW_LAST_ID)
        );
    }

    /**
     * Nesting costs what the body spells out, and no more. Every level of this chain adds two leg sub-searches, and the
     * deepest chain the leg budget admits ({@code max_leg_searches / 2} levels) has to actually run: each level is executed
     * exactly once, as its parent's leg sub-search, and the enclosing Tail contributes the nested legs' union instead of
     * firing them again. Two failures this rules out, both of which reproduce on a body of a few hundred bytes: the fan-out
     * doubling per level ({@code 2^(depth+1) - 2} leg searches), and one rewrite round spent per level of nesting, which
     * exhausts core's 16 and answers a within-budget request with an internal error.
     */
    @SneakyThrows
    public void testFused_deepestNestingWithinTheLegBudget_runs() {
        ensureDataset();
        int levels = DEFAULT_MAX_FUSION_LEG_SEARCHES / 2;
        String body = "{\"query\":" + nestedFusedChain(levels) + ",\"track_total_hits\":true}";

        Map<String, Object> resp = searchRaw(body, 20);

        assertFalse(levels + " levels of fused nesting must return hits", hitIds(resp).isEmpty());
        assertEquals("and the Tail still covers the whole union at that depth", (long) TOTAL_DOCS, totalHits(resp));
    }

    /**
     * One leg sub-search past the budget is the user's 400 — naming the count, the ceiling and the setting that raises it —
     * not a 500. The rejection is decided from the request body alone, before the first fan-out, so an over-budget request
     * costs one tree walk rather than the searches it asked for.
     */
    @SneakyThrows
    public void testFused_whenNestingExceedsTheLegBudget_thenBadRequestNotServerError() {
        ensureDataset();
        int levels = DEFAULT_MAX_FUSION_LEG_SEARCHES / 2 + 1;
        String body = "{\"query\":" + nestedFusedChain(levels) + "}";

        ResponseException e = expectThrows(ResponseException.class, () -> searchRawExpectingFailure(body));

        assertEquals(
            "an over-budget body is a user error, not a server error",
            RestStatus.BAD_REQUEST.getStatus(),
            e.getResponse().getStatusLine().getStatusCode()
        );
        String message = EntityUtils.toString(e.getResponse().getEntity());
        assertTrue("the response states what the body declares: " + message, message.contains("declares " + (2 * levels) + " leg"));
        assertTrue("and how to raise it: " + message, message.contains(MAX_FUSION_LEG_SEARCHES.getKey()));
    }

    /**
     * A nested leg's rejection must reach the user as the 400 it is. The leg runs as its own search, so its refusal
     * arrives at the enclosing coordinator as a MultiSearch item failure; wrapping that in an {@code IllegalStateException}
     * made every leg-side rejection a 500 with the real status reachable only under {@code caused_by} — a fused hybrid
     * answered a plain user error the way it would answer a bug, and the same masking hit 429s and cluster blocks.
     */
    @SneakyThrows
    public void testFused_whenNestedLegRejects_thenBadRequestNotServerError() {
        ensureDataset();
        // The inner window_size exceeds index.max_result_window, so the inner level refuses it during its own rewrite.
        String body = "{\"query\":{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + fusedHybrid(20000)
            + ","
            + leg()
            + "]}}}";

        ResponseException e = expectThrows(ResponseException.class, () -> searchRawExpectingFailure(body));

        assertEquals(
            "a leg-side rejection is the user's 400, not a 500",
            RestStatus.BAD_REQUEST.getStatus(),
            e.getResponse().getStatusLine().getStatusCode()
        );
        assertTrue(
            "and the response names the real cause rather than an illegal_state_exception",
            EntityUtils.toString(e.getResponse().getEntity()).contains("max_result_window")
        );
    }

    // ------------------------------------------------ helpers ------------------------------------------------

    @SneakyThrows
    private void searchRawExpectingFailure(String jsonBody) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", "20");
        client().performRequest(request);
    }

    @SneakyThrows
    private Map<String, Object> searchRaw(String jsonBody, int size) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(jsonBody);
        request.addParameter("size", Integer.toString(size));
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<String> hitIds(Map<String, Object> resp) {
        List<String> out = new ArrayList<>();
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        if (hitList != null) {
            for (Map<String, Object> hit : hitList) {
                out.add((String) hit.get("_id"));
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> grpBuckets(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggs = (Map<String, Object>) resp.get("aggregations");
        if (aggs == null) {
            return out;
        }
        Map<String, Object> byGrp = (Map<String, Object>) aggs.get("by_grp");
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) byGrp.get("buckets");
        for (Map<String, Object> bucket : buckets) {
            out.put((String) bucket.get("key"), ((Number) bucket.get("doc_count")).intValue());
        }
        return out;
    }

    private Map<String, Integer> grpCounts(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (String id : hitIds(resp)) {
            out.merge(Integer.parseInt(id) <= WINDOW_LAST_ID ? "A" : "B", 1, Integer::sum);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(Map<String, Object> resp) {
        Map<String, Object> hits = (Map<String, Object>) resp.get("hits");
        Object totalObj = hits.get("total");
        if (totalObj instanceof Map) {
            Object value = ((Map<String, Object>) totalObj).get("value");
            return value == null ? -1 : ((Number) value).longValue();
        }
        return -1;
    }
}
