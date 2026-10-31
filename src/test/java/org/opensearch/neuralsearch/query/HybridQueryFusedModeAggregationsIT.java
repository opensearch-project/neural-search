/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * Proves that aggregations over a fused/resolver hybrid cover the full leg-union, not just the fused ranking window.
 *
 * <p>This is the property the Tail exists for. The scored Top clauses only carry the fused window, so if the Tail were
 * missing (or were dropped) the aggregation collector would only ever see those window documents and every bucket outside
 * it would silently read zero. The Tail adds the real legs as a non-scoring {@code filter}, which is what defines the
 * aggregation match set.
 *
 * <p>Each test is paired against a classic-hybrid oracle so the numbers are anchored to known-correct behavior rather
 * than to this feature's own implementation. Multi-shard on purpose (3 shards) so results are genuinely distributed.
 *
 * <p>Dataset (30 docs, ids 1..30), tiered so both legs rank the 5 blue docs into the window and never the red docs:
 * <pre>
 *   color:  ids 1-5   = "blue"  (tier 3 — always in the fused top-5 window)
 *           ids 6-25  = "green" (tier 2 — always outside the window)
 *           ids 26-30 = "red"   (tier 1 — lowest, guaranteed outside the window)
 *   s = tier*1000 - id;  window_size = 5  =>  fused window = {1..5} = all blue
 * </pre>
 * So {@code terms(color)} covering the union must report {@code blue=5, green=20, red=5} and {@code total_hits=30}. The
 * {@code red} bucket is the smoking gun: it can only be non-zero if aggregations see past the window.
 */
public class HybridQueryFusedModeAggregationsIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-aggs-matchset";
    private static final String NORM_PIPELINE = "fused-aggs-norm-pipeline";
    private static final String COLOR_FIELD = "color";
    private static final String SCORE_FIELD = "s";
    private static final int WINDOW_SIZE = 5;
    private static final int BLUE_COUNT = 5;
    private static final int GREEN_COUNT = 20;
    private static final int RED_COUNT = 5;
    private static final int TOTAL_DOCS = 30;

    private String indexConfig() {
        return "{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0,"
            + "\"index.search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"},"
            + "\"mappings\":{\"properties\":{\""
            + COLOR_FIELD
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
            String color = id <= 5 ? "blue" : (id <= 25 ? "green" : "red");
            int tier = id <= 5 ? 3 : (id <= 25 ? 2 : 1);
            int s = tier * 1000 - id;
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity("{\"" + COLOR_FIELD + "\":\"" + color + "\",\"" + SCORE_FIELD + "\":" + s + "}");
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(
                "indexing doc " + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    /** A leg that matches ALL docs and scores each by the numeric field (deterministic, shard-independent). */
    private String leg() {
        return "{\"function_score\":{\"query\":{\"match_all\":{}},"
            + "\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    private String classicHybrid() {
        return "{\"hybrid\":{\"queries\":[" + leg() + "," + leg() + "]}}";
    }

    private String fusedHybrid() {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + leg()
            + ","
            + leg()
            + "]}}";
    }

    private String termsAgg() {
        return "\"aggregations\":{\"by_color\":{\"terms\":{\"field\":\"" + COLOR_FIELD + "\",\"size\":10}}}";
    }

    /** Oracle: classic hybrid aggregations cover the full leg-union — the baseline the fused numbers must match. */
    @SneakyThrows
    public void testClassicHybrid_aggregationsCoverFullUnion() {
        ensureDataset();
        String body = "{\"query\":" + classicHybrid() + "," + termsAgg() + ",\"track_total_hits\":true}";

        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> buckets = colorBuckets(resp);

        assertEquals("classic: blue", Integer.valueOf(BLUE_COUNT), buckets.getOrDefault("blue", 0));
        assertEquals("classic: green proves aggs see beyond the window", Integer.valueOf(GREEN_COUNT), buckets.getOrDefault("green", 0));
        assertEquals("classic: red is the bucket entirely outside the window", Integer.valueOf(RED_COUNT), buckets.getOrDefault("red", 0));
        assertEquals("classic: total_hits", (long) TOTAL_DOCS, totalHits(resp));
    }

    /** The core guarantee: fused aggregations equal classic's — the Tail puts the full union into the self-erased query. */
    @SneakyThrows
    public void testFusedHybrid_aggregationsCoverFullUnion_notJustWindow() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "," + termsAgg() + ",\"track_total_hits\":true}";

        Map<String, Object> resp = searchRaw(body, 10);
        Map<String, Integer> buckets = colorBuckets(resp);

        // Had fused windowed the aggregation, green/red would read 0 and total would be ~5.
        assertEquals("fused: blue", Integer.valueOf(BLUE_COUNT), buckets.getOrDefault("blue", 0));
        assertEquals(
            "fused: green must equal classic (full union, not windowed)",
            Integer.valueOf(GREEN_COUNT),
            buckets.getOrDefault("green", 0)
        );
        assertEquals("fused: red must equal classic — the smoking gun", Integer.valueOf(RED_COUNT), buckets.getOrDefault("red", 0));
        assertEquals("fused: total_hits is the full match set", (long) TOTAL_DOCS, totalHits(resp));
    }

    /**
     * Aggregations outrank the totals preference: {@code track_total_hits:false} alone would allow a Top-only query, but
     * an aggregation still requires the Tail, so buckets stay full.
     */
    @SneakyThrows
    public void testFusedHybrid_trackTotalHitsFalse_aggregationsStillFull() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "," + termsAgg() + ",\"track_total_hits\":false}";

        Map<String, Integer> buckets = colorBuckets(searchRaw(body, 10));

        assertEquals("tth:false: green still full union", Integer.valueOf(GREEN_COUNT), buckets.getOrDefault("green", 0));
        assertEquals("tth:false: red still full union", Integer.valueOf(RED_COUNT), buckets.getOrDefault("red", 0));
    }

    /**
     * KNOWN LIMITATION (pins current behavior, not desired behavior): {@code min_score > 0} collapses the aggregation to
     * the fused window.
     *
     * <p>Tail-only documents match just the non-scoring filter, so they score 0. Core applies the min-score collector
     * <i>after</i> the aggregation collector precisely so it filters aggregations too, which drops every score-0 Tail doc
     * before it can be counted — leaving only the scored window. Classic hybrid avoids this by unsetting min_score
     * shard-side and re-applying it on the coordinator, but neither half of that machinery is reachable for a fused query
     * (its shard-side guard keys on the classic query type, and its coordinator half keys on delimiter score-docs fused
     * never emits).
     *
     * <p>When the fix lands — filter the fused Top by min_score on the coordinator and unset it on the outer source —
     * this test should flip to expecting the full union, i.e. treat a failure here as the reminder that it did.
     */
    @SneakyThrows
    public void testFusedHybrid_minScorePositive_thenAggregationsCollapseToWindow_knownLimitation() {
        ensureDataset();
        String body = "{\"query\":" + fusedHybrid() + "," + termsAgg() + ",\"min_score\":0.0005,\"track_total_hits\":true}";

        Map<String, Integer> buckets = colorBuckets(searchRaw(body, 10));

        assertEquals("min_score: the scored window survives", Integer.valueOf(BLUE_COUNT), buckets.getOrDefault("blue", 0));
        assertEquals("min_score: score-0 Tail docs are filtered before aggregation", Integer.valueOf(0), buckets.getOrDefault("green", 0));
        assertEquals("min_score: same for the loser bucket", Integer.valueOf(0), buckets.getOrDefault("red", 0));
    }

    // ------------------------------------------------ helpers ------------------------------------------------

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
    private Map<String, Integer> colorBuckets(Map<String, Object> resp) {
        Map<String, Integer> out = new LinkedHashMap<>();
        Map<String, Object> aggs = (Map<String, Object>) resp.get("aggregations");
        if (aggs == null) {
            return out;
        }
        Map<String, Object> byColor = (Map<String, Object>) aggs.get("by_color");
        for (Map<String, Object> bucket : (List<Map<String, Object>>) byColor.get("buckets")) {
            out.put((String) bucket.get("key"), ((Number) bucket.get("doc_count")).intValue());
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
