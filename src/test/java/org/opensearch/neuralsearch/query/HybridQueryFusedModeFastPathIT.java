/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.stats.events.EventStatName;

import lombok.SneakyThrows;

/**
 * The fused hybrid's fast path: a request whose shape needs nothing from round 2 is answered from the legs alone —
 * the legs fetch the user's fields, the coordinator assembles the page, round 2 runs as {@code match_none}.
 *
 * <p>The contract is that a client cannot tell, with one stated exception. Every test here compares the fast-path
 * response with the two-round response for the same request and asserts they are identical in everything a client sees
 * — hits, order, scores, {@code _source} and fields, {@code max_score}, {@code hits.total}. The exception is the order
 * of documents with bit-identical fused scores inside one shard: round 2 orders them by Lucene doc id, which the
 * coordinator never sees, and the fast path by {@code _id} (see {@code HybridFusionOrchestrator#roundTwoOrder}) — and
 * where such a run straddles the page edge, that order decides which tied document the page shows. The comparison
 * therefore canonicalizes the order within each run of equal scores on both sides (on corpora with distinct scores, where
 * no run can straddle an edge), and
 * {@link #testFastPath_whenFusedScoresTie_thenTiesAreOrderedByIdAndEverythingElseMatchesTwoRounds} pins the fast
 * path's own order down. The two-round control is the same request with
 * {@code profile: true}, which the fast path refuses (round 2's own tree is part of what profile reports), with the
 * profile section stripped before comparing. Which path ran is proved through the stats API:
 * {@code hybrid_query_fused_fast_path_requests} moves for the fast path and not for the control.
 */
public class HybridQueryFusedModeFastPathIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-hybrid-fused-fast-path";
    private static final String TEXT_FIELD = "text";
    private static final String NUM_FIELD = "num";
    private static final int DOCS = 12;
    /** Below DOCS so the lexical leg's count is capped and reported {@code gte}: the default-totals request can take the fast path. */
    private static final int THRESHOLD = 8;
    private static final int WINDOW = 6;

    @SneakyThrows
    private void prepareIndex() {
        if (indexExists(INDEX)) {
            return;
        }
        createIndexWithConfiguration(
            INDEX,
            "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                + TEXT_FIELD
                + "\":{\"type\":\"text\"},\""
                + NUM_FIELD
                + "\":{\"type\":\"integer\"}}}}",
            ""
        );
        for (int i = 1; i <= DOCS; i++) {
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + i + "?refresh=true");
            request.setJsonEntity("{\"" + TEXT_FIELD + "\":\""
            // distinct lengths (3 + i tokens, all inside BM25's exact-norm range) → distinct scores, so no fused-score ties
                + (i % 2 == 1 ? "hello place " + i : "hello there " + i)
                + " filler".repeat(i)
                + "\",\""
                + NUM_FIELD
                + "\":"
                + i
                + "}");
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus());
        }
    }

    private static String fusedQuery() {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":\"place\"}}]}}";
    }

    /** A request body from its non-query fields; {@code extra} is spliced in verbatim (may be empty). */
    private static String body(String extra) {
        return "{" + (extra.isEmpty() ? "" : extra + ",") + "\"query\":" + fusedQuery() + "}";
    }

    @SneakyThrows
    private Map<String, Object> search(String requestBody) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(requestBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /** The same request forced down the two-round path ({@code profile: true} is refused by the fast path), profile stripped. */
    private Map<String, Object> twoRoundControl(String extra) {
        Map<String, Object> control = search(body((extra.isEmpty() ? "" : extra + ",") + "\"profile\":true"));
        control.remove("profile");
        return control;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> hits(Map<String, Object> response) {
        return (Map<String, Object>) response.get("hits");
    }

    /**
     * Everything a client reads, minus timing: the whole {@code hits} object and the shard summary. Within a run of
     * hits with equal {@code _score} the order is canonicalized by {@code _id} on both sides before comparing — the one
     * place the two paths are allowed to differ (doc-id order versus {@code _id} order for same-shard ties). Everything
     * else, including the order across different scores, must match exactly.
     */
    @SuppressWarnings("unchecked")
    private static void assertClientVisibleIdentical(Map<String, Object> fast, Map<String, Object> twoRound) {
        Map<String, Object> expected = new java.util.HashMap<>(hits(twoRound));
        Map<String, Object> actual = new java.util.HashMap<>(hits(fast));
        expected.put("hits", canonicalTieOrder((List<Map<String, Object>>) expected.get("hits")));
        actual.put("hits", canonicalTieOrder((List<Map<String, Object>>) actual.get("hits")));
        assertEquals("hits (ids, order up to ties, scores, sources, fields, total, max_score)", expected, actual);
        assertEquals(twoRound.get("_shards"), fast.get("_shards"));
        assertEquals(twoRound.get("timed_out"), fast.get("timed_out"));
    }

    /** The hits with each maximal run of equal {@code _score} re-sorted by {@code _id}; runs never cross a score change. */
    private static List<Map<String, Object>> canonicalTieOrder(List<Map<String, Object>> hits) {
        List<Map<String, Object>> out = new ArrayList<>();
        int i = 0;
        while (i < hits.size()) {
            int j = i + 1;
            while (j < hits.size() && java.util.Objects.equals(hits.get(j).get("_score"), hits.get(i).get("_score"))) {
                j++;
            }
            List<Map<String, Object>> run = new ArrayList<>(hits.subList(i, j));
            run.sort(java.util.Comparator.comparing(h -> (String) h.get("_id")));
            out.addAll(run);
            i = j;
        }
        return out;
    }

    @SneakyThrows
    private int fastPathCount() {
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);
        return ((Number) getNestedValue(allNodesStats, EventStatName.HYBRID_QUERY_FUSED_FAST_PATH_REQUESTS)).intValue();
    }

    /** The common shape: default totals (the lexical leg proves them), a page inside the window, {@code _source} on. */
    @SneakyThrows
    public void testFastPath_whenDefaultRequest_thenResponseIsIdenticalToTwoRoundsAndRoundTwoIsSkipped() {
        prepareIndex();
        enableStats();
        String extra = "\"size\":" + WINDOW + ",\"track_total_hits\":" + THRESHOLD;

        Map<String, Object> control = twoRoundControl(extra);
        int before = fastPathCount();
        Map<String, Object> fast = search(body(extra));

        assertClientVisibleIdentical(fast, control);
        assertEquals("the default request took the fast path", before + 1, fastPathCount());
        assertEquals(WINDOW, ((List<?>) hits(fast).get("hits")).size());
        assertEquals(Map.of("value", THRESHOLD, "relation", "gte"), hits(fast).get("total"));
    }

    /** Fetch-phase fields travel with the legs: filtered _source, docvalue fields, fields, version, seq_no/primary_term. */
    @SneakyThrows
    public void testFastPath_whenFetchFieldsRequested_thenTheyMatchRoundTwosFetch() {
        prepareIndex();
        enableStats();
        String extra = "\"size\":3,\"track_total_hits\":false,\"_source\":{\"includes\":[\""
            + NUM_FIELD
            + "\"]},\"docvalue_fields\":[\""
            + NUM_FIELD
            + "\"],\"fields\":[\""
            + TEXT_FIELD
            + "\"],\"version\":true,\"seq_no_primary_term\":true";

        Map<String, Object> control = twoRoundControl(extra);
        int before = fastPathCount();
        Map<String, Object> fast = search(body(extra));

        assertClientVisibleIdentical(fast, control);
        assertEquals(before + 1, fastPathCount());
        Map<String, Object> first = (Map<String, Object>) ((List<?>) hits(fast).get("hits")).get(0);
        assertNotNull("filtered _source came back", first.get("_source"));
        assertNotNull("_version came back", first.get("_version"));
        assertNotNull("_seq_no came back", first.get("_seq_no"));
        assertNotNull("fields came back", first.get("fields"));
        assertNull("totals disabled → no total", hits(fast).get("total"));
    }

    /** Paging inside the window, and the count-only request: both assembled without round 2. */
    @SneakyThrows
    public void testFastPath_whenFromSizeInsideTheWindowOrSizeZero_thenIdenticalToTwoRounds() {
        prepareIndex();
        enableStats();
        for (String extra : List.of("\"from\":2,\"size\":3,\"track_total_hits\":false", "\"size\":0,\"track_total_hits\":" + THRESHOLD)) {
            Map<String, Object> control = twoRoundControl(extra);
            int before = fastPathCount();
            Map<String, Object> fast = search(body(extra));
            assertClientVisibleIdentical(fast, control);
            assertEquals(extra, before + 1, fastPathCount());
        }
    }

    /** explain is answered from the legs: the fused breakdown attaches to the assembled hits exactly as to round 2's. */
    @SneakyThrows
    public void testFastPath_whenExplain_thenTheFusedBreakdownIsIdentical() {
        prepareIndex();
        enableStats();
        String extra = "\"size\":3,\"track_total_hits\":false,\"explain\":true";

        Map<String, Object> control = twoRoundControl(extra);
        int before = fastPathCount();
        Map<String, Object> fast = search(body(extra));

        assertEquals(before + 1, fastPathCount());
        List<Map<String, Object>> fastHits = (List<Map<String, Object>>) hits(fast).get("hits");
        List<Map<String, Object>> controlHits = (List<Map<String, Object>>) hits(control).get("hits");
        assertEquals(controlHits.size(), fastHits.size());
        for (int i = 0; i < fastHits.size(); i++) {
            assertEquals(controlHits.get(i).get("_id"), fastHits.get(i).get("_id"));
            assertEquals(controlHits.get(i).get("_score"), fastHits.get(i).get("_score"));
            assertEquals(
                "the fused explanation tree is the same on both paths",
                controlHits.get(i).get("_explanation"),
                fastHits.get(i).get("_explanation")
            );
        }
    }

    /**
     * Shapes that need round 2 take it: the page reaching past the ranked window (Tail documents fill it), a threshold no
     * leg proves (the Tail counts the union), rescore, a sort, a named leg. Each answers exactly as before and none moves
     * the fast-path counter.
     */
    @SneakyThrows
    public void testFastPath_whenTheRequestNeedsRoundTwo_thenTwoRoundsRunAndTheCounterDoesNotMove() {
        prepareIndex();
        enableStats();
        List<String> twoRoundShapes = List.of(
            "\"size\":" + (WINDOW + 3) + ",\"track_total_hits\":false",                       // page past the window
            "\"size\":3,\"track_total_hits\":" + (DOCS + 5),                                   // no leg reaches the threshold
            "\"size\":3,\"track_total_hits\":true",                                            // exact totals
            "\"size\":3,\"track_total_hits\":false,\"sort\":[\"_score\"]",                     // any sort
            "\"size\":3,\"track_total_hits\":false,\"rescore\":{\"window_size\":10,\"query\":{\"rescore_query\":{\"term\":{\""
                + TEXT_FIELD
                + "\":\"place\"}},\"query_weight\":1.0,\"rescore_query_weight\":2.0}}"        // rescore
        );
        for (String extra : twoRoundShapes) {
            int before = fastPathCount();
            Map<String, Object> response = search(body(extra));
            assertEquals(extra, before, fastPathCount());
            assertNotNull(extra, hits(response).get("hits"));
        }
        // page past the window: identical to the two-round path for the same request (with default totals the Tail fills
        // every slot; with totals off the Top-only round is short of the window either way — pre-existing behaviour)
        String pastWindow = "\"size\":" + (WINDOW + 3) + ",\"track_total_hits\":" + THRESHOLD;
        assertClientVisibleIdentical(search(body(pastWindow)), twoRoundControl(pastWindow));
        assertEquals(WINDOW + 3, ((List<?>) hits(search(body(pastWindow))).get("hits")).size());
        // a named leg: matched_queries keeps its exact two-round answer
        String named = "{\"size\":3,\"track_total_hits\":false,\"query\":{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":{\"query\":\"hello\",\"_name\":\"lex\"}}},{\"term\":{\""
            + TEXT_FIELD
            + "\":{\"value\":\"place\",\"_name\":\"place\"}}}]}}}";
        int before = fastPathCount();
        Map<String, Object> namedResponse = search(named);
        assertEquals(before, fastPathCount());
        Map<String, Object> first = (Map<String, Object>) ((List<?>) hits(namedResponse).get("hits")).get(0);
        assertTrue(((List<?>) first.get("matched_queries")).contains("lex"));
    }

    /**
     * The one client-visible difference, pinned down. One shard, twelve identical documents: every fused score ties, so
     * round 2 would order the page by Lucene doc id while the fast path orders it by {@code _id} — "1", "10", "11", "12",
     * "2", ... — deterministically, on every replica, across merges. Ids, scores, sources, total and max_score are the
     * two-round path's; the fast path ran (the counter moved) and the control did not.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testFastPath_whenFusedScoresTie_thenTiesAreOrderedByIdAndEverythingElseMatchesTwoRounds() {
        String index = INDEX + "-ties";
        if (indexExists(index) == false) {
            createIndexWithConfiguration(
                index,
                "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                    + TEXT_FIELD
                    + "\":{\"type\":\"text\"}}}}",
                ""
            );
            for (int i = 1; i <= DOCS; i++) {
                Request request = new Request("PUT", "/" + index + "/_doc/" + i + "?refresh=true");
                request.setJsonEntity("{\"" + TEXT_FIELD + "\":\"hello place\"}");
                client().performRequest(request);
            }
        }
        enableStats();
        String extra = "\"size\":" + DOCS + ",\"track_total_hits\":false";
        String requestBody = "{"
            + extra
            + ",\"query\":"
            + fusedQuery().replace("\"window_size\":" + WINDOW, "\"window_size\":" + DOCS)
            + "}";
        String controlBody = "{"
            + extra
            + ",\"profile\":true,\"query\":"
            + fusedQuery().replace("\"window_size\":" + WINDOW, "\"window_size\":" + DOCS)
            + "}";

        int before = fastPathCount();
        Map<String, Object> fast = searchIndex(index, requestBody);
        assertEquals("the tie-heavy request took the fast path", before + 1, fastPathCount());
        Map<String, Object> control = searchIndex(index, controlBody);
        control.remove("profile");
        assertEquals("the profile control ran two rounds", before + 1, fastPathCount());

        List<Map<String, Object>> fastHits = (List<Map<String, Object>>) hits(fast).get("hits");
        List<String> fastIds = fastHits.stream().map(h -> (String) h.get("_id")).toList();
        assertEquals(DOCS, fastIds.size());
        assertEquals("every document ties, so the fast path orders the whole page by _id", fastIds.stream().sorted().toList(), fastIds);
        assertEquals("all twelve fused scores are the same value", 1, fastHits.stream().map(h -> h.get("_score")).distinct().count());
        assertClientVisibleIdentical(fast, control);
    }

    @SneakyThrows
    private Map<String, Object> searchIndex(String index, String requestBody) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(requestBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /**
     * The fetch-volume gate. On an index whose documents carry a 768-dim {@code knn_vector}, a {@code size:3, window:100}
     * request that would return the vector ({@code _source} on) is refused the fast path — the legs would fetch up to
     * 2 × 100 − 3 such documents, ~1.5 MB more than the page round 2 fetches — and takes two rounds, answering exactly as
     * before; the same request with the vector excluded from {@code _source}, or with {@code _source} off, takes the
     * fast path. All answers are identical to the two-round control for the same request.
     */
    @SneakyThrows
    public void testFastPath_whenTheReturnedSourceCarriesAVector_thenTwoRoundsUnlessTheVectorIsExcluded() {
        String index = INDEX + "-vectors";
        if (indexExists(index) == false) {
            createIndex(
                index,
                "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":1,\"number_of_replicas\":0}},"
                    + "\"mappings\":{\"properties\":{\""
                    + TEXT_FIELD
                    + "\":{\"type\":\"text\"},\"vec\":{\"type\":\"knn_vector\",\"dimension\":768,"
                    + "\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}}}}"
            );
            for (int i = 1; i <= DOCS; i++) {
                Request request = new Request("PUT", "/" + index + "/_doc/" + i + "?refresh=true");
                // distinct lengths and distinct vectors: distinct leg scores, so identity is asserted on distinct fused scores
                request.setJsonEntity(
                    "{\"" + TEXT_FIELD + "\":\"hello place " + i + " filler".repeat(i) + "\",\"vec\":" + vector768(1.0f + i / 10.0f) + "}"
                );
                client().performRequest(request);
            }
        }
        enableStats();
        // window 100 with size 3: the fast path would fetch up to 2 × 100 − 3 documents of ~7.7 KB (768 floats) — over
        // the 1 MB budget — so a request that returns the vector is refused; the same request without it is not
        int window = 100;
        String legs = "[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"knn\":{\"vec\":{\"vector\":"
            + vector768(1.05f)
            + ",\"k\":"
            + window
            + "}}}]";
        String query = "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + window
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},\"queries\":"
            + legs
            + "}}";
        String common = "\"size\":3,\"track_total_hits\":false";

        for (String[] shape : new String[][] {
            { "\"_source\":true", "false" },
            { "", "false" },
            { "\"_source\":{\"excludes\":[\"vec\"]}", "true" },
            { "\"_source\":false", "true" } }) {
            String source = shape[0].isEmpty() ? "" : shape[0] + ",";
            boolean fastPathExpected = Boolean.parseBoolean(shape[1]);
            String body = "{" + source + common + ",\"query\":" + query + "}";
            Map<String, Object> control = searchIndex(index, "{" + source + common + ",\"profile\":true,\"query\":" + query + "}");
            control.remove("profile");
            int before = fastPathCount();
            Map<String, Object> response = searchIndex(index, body);
            assertEquals("fast path taken? shape=" + shape[0], fastPathExpected ? before + 1 : before, fastPathCount());
            assertClientVisibleIdentical(response, control);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) hits(response).get("hits");
            assertEquals(3, hits.size());
            boolean vectorReturned = hits.stream().anyMatch(h -> h.get("_source") instanceof Map<?, ?> src && src.containsKey("vec"));
            assertEquals(
                "the vector is in the page exactly when _source carries it: shape=" + shape[0],
                fastPathExpected == false,
                vectorReturned
            );
        }
    }

    /** A 768-dim vector whose first component is {@code lead} and the rest 1.0, as a JSON array. */
    private static String vector768(float lead) {
        StringBuilder sb = new StringBuilder("[").append(lead);
        for (int d = 1; d < 768; d++) {
            sb.append(",1.0");
        }
        return sb.append("]").toString();
    }
}
