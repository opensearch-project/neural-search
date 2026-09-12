/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import lombok.SneakyThrows;

/**
 * {@code hits.total} of a fused hybrid when round 2 runs without its Tail.
 *
 * <p>A fused request that sets nothing but wants a count beyond its window used to carry the Tail for that count alone.
 * Now the lexical legs count up to the request's threshold, and when one of them already exceeds it the Tail is dropped
 * and the response carries the total the Tail would have produced — core caps a tracked count at the threshold, so both
 * paths say {@code {threshold, gte}}. These tests pin that the visible response is the same either way, and that the shapes
 * where the Tail's documents (not just its count) are part of the answer keep it.
 *
 * <p>The threshold is set low ({@code track_total_hits: N}) so a handful of documents is enough to cross it; the default
 * threshold (10 000) exercises exactly the same code with a larger corpus.
 */
public class HybridQueryFusedModeTotalHitsIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-hybrid-fused-total-hits";
    private static final String TEXT_FIELD = "text";
    /** Every document matches the lexical leg. */
    private static final int DOCS = 12;
    /** Below DOCS, so the lexical leg's own count is capped at it and reported {@code gte}. */
    private static final int THRESHOLD = 8;
    /** The fused window: smaller than THRESHOLD so the count is genuinely "beyond the window". */
    private static final int WINDOW = 5;

    @SneakyThrows
    private void prepareIndex() {
        if (indexExists(INDEX)) {
            return;
        }
        createIndexWithConfiguration(
            INDEX,
            "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                + TEXT_FIELD
                + "\":{\"type\":\"text\"}}}}",
            ""
        );
        for (int i = 1; i <= DOCS; i++) {
            // Every doc carries "hello" (lexical leg); the odd ones also carry "place" (second leg), so the union is all DOCS
            // and the two legs' match sets differ.
            addDocument(INDEX, String.valueOf(i), TEXT_FIELD, i % 2 == 1 ? "hello place " + i : "hello there " + i, null, null);
        }
    }

    private static String fusedQuery(int windowSize) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":\"place\"}}]}}";
    }

    @SneakyThrows
    private Map<String, Object> search(String body) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> total(Map<String, Object> response) {
        return (Map<String, Object>) ((Map<String, Object>) response.get("hits")).get("total");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hits(Map<String, Object> response) {
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    /**
     * The lexical leg has DOCS matches, above THRESHOLD, so its count comes back capped and round 2 runs Top-only — and the
     * response's total is exactly what the Tail path reports: {@code {THRESHOLD, gte}}. The Tail path is produced here by
     * asking for exact totals, which always keeps it, then compared at the same threshold semantics.
     */
    @SneakyThrows
    public void testTotalHits_whenALegExceedsTheThreshold_thenTheTailIsDroppedAndTheTotalIsUnchanged() {
        prepareIndex();
        String body = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + THRESHOLD + ",\"query\":" + fusedQuery(WINDOW) + "}";

        Map<String, Object> response = search(body);

        assertEquals("a capped count says exactly what the Tail would have said", THRESHOLD, total(response).get("value"));
        assertEquals("gte", total(response).get("relation"));
        assertEquals("the page is the fused window", WINDOW, hits(response).size());
        // Exact totals keep the Tail; the union it counts is every document.
        Map<String, Object> exact = search("{\"size\":" + WINDOW + ",\"track_total_hits\":true,\"query\":" + fusedQuery(WINDOW) + "}");
        assertEquals(DOCS, total(exact).get("value"));
        assertEquals("eq", total(exact).get("relation"));
        assertEquals("and the ranked page is identical on both paths", hits(exact), hits(response));
    }

    /**
     * A threshold no leg reaches: the union's size is unknown without the Tail, so the Tail stays and the total is exact,
     * as before.
     */
    @SneakyThrows
    public void testTotalHits_whenNoLegReachesTheThreshold_thenTheTailCountsTheUnionExactly() {
        prepareIndex();
        String body = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + (DOCS + 5) + ",\"query\":" + fusedQuery(WINDOW) + "}";

        Map<String, Object> response = search(body);

        assertEquals(DOCS, total(response).get("value"));
        assertEquals("eq", total(response).get("relation"));
    }

    /**
     * A page reaching past the ranked window is filled by Tail-only documents; dropping the Tail would shorten it. The
     * count alone is not allowed to stand in for the Tail here, so every requested slot is filled exactly as before.
     */
    @SneakyThrows
    public void testTotalHits_whenThePageReachesPastTheWindow_thenTheTailIsKeptAndThePageIsFull() {
        prepareIndex();
        int size = WINDOW + 3;
        String body = "{\"size\":" + size + ",\"track_total_hits\":" + THRESHOLD + ",\"query\":" + fusedQuery(WINDOW) + "}";

        Map<String, Object> response = search(body);

        assertEquals("Tail-only documents fill the slots past the fused window", size, hits(response).size());
        assertEquals(THRESHOLD, total(response).get("value"));
        assertEquals("gte", total(response).get("relation"));
    }

    /** The explicit opt-out is unchanged: no total, Top-only, the fused window and nothing else. */
    @SneakyThrows
    public void testTotalHits_whenTotalsAreDisabled_thenNothingIsReported() {
        prepareIndex();
        Map<String, Object> response = search("{\"size\":" + WINDOW + ",\"track_total_hits\":false,\"query\":" + fusedQuery(WINDOW) + "}");

        assertNull(((Map<String, Object>) response.get("hits")).get("total"));
        assertEquals(WINDOW, hits(response).size());
    }

    /**
     * The proof that the Tail was actually dropped, not merely that the total reads the same: the coordinator's own profile
     * entry says whether round 2 carried a Tail. Above the threshold it does not; below it (no leg proves the count) it does.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testTotalHits_whenProfiled_thenTheCoordinatorEntryShowsTheTailDroppedOnlyWhenALegProvesTheCount() {
        prepareIndex();
        assertEquals(
            Boolean.FALSE,
            tailBuilt(
                search(
                    "{\"profile\":true,\"size\":" + WINDOW + ",\"track_total_hits\":" + THRESHOLD + ",\"query\":" + fusedQuery(WINDOW) + "}"
                )
            )
        );
        assertEquals(
            Boolean.TRUE,
            tailBuilt(
                search(
                    "{\"profile\":true,\"size\":"
                        + WINDOW
                        + ",\"track_total_hits\":"
                        + (DOCS + 5)
                        + ",\"query\":"
                        + fusedQuery(WINDOW)
                        + "}"
                )
            )
        );
        assertEquals(
            "a page past the window keeps the Tail whatever the legs counted",
            Boolean.TRUE,
            tailBuilt(
                search(
                    "{\"profile\":true,\"size\":"
                        + (WINDOW + 3)
                        + ",\"track_total_hits\":"
                        + THRESHOLD
                        + ",\"query\":"
                        + fusedQuery(WINDOW)
                        + "}"
                )
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static Object tailBuilt(Map<String, Object> response) {
        List<Map<String, Object>> shards = (List<Map<String, Object>>) ((Map<String, Object>) response.get("profile")).get("shards");
        for (Map<String, Object> shard : shards) {
            if (String.valueOf(shard.get("id")).contains("[coordinator]")) {
                List<Map<String, Object>> searches = (List<Map<String, Object>>) shard.get("searches");
                List<Map<String, Object>> query = (List<Map<String, Object>>) searches.get(0).get("query");
                return ((Map<String, Object>) query.get(0).get("debug")).get("tail_built");
            }
        }
        fail("no [coordinator] entry in the profile: " + shards.stream().map(shard -> shard.get("id")).toList());
        return null;
    }
}
