/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

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
 * profile section stripped before comparing. Which path ran is proved intrinsically where the paths are
 * distinguishable at all: on the one-shard all-ties index the fast path's {@code _id} tie order differs from round 2's
 * doc-id order, so {@link #testFastPath_whenFusedScoresTie_thenTiesAreOrderedByIdAndEverythingElseMatchesTwoRounds}
 * passing proves the fast path really runs; path selection for every other shape is pinned at unit level
 * ({@code requestShapeAllowsFastPath}, {@code decideFastPathBeforeLegs}, the fetch budget, and the fan-out rewrite
 * tests).
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

    /** The common shape: default totals (the lexical leg proves them), a page inside the window, {@code _source} on. */
    @SneakyThrows
    public void testFastPath_whenDefaultRequest_thenResponseIsIdenticalToTwoRounds() {
        prepareIndex();
        String extra = "\"size\":" + WINDOW + ",\"track_total_hits\":" + THRESHOLD;

        Map<String, Object> control = twoRoundControl(extra);
        Map<String, Object> fast = search(body(extra));

        assertClientVisibleIdentical(fast, control);
        assertEquals(WINDOW, ((List<?>) hits(fast).get("hits")).size());
        assertEquals(Map.of("value", THRESHOLD, "relation", "gte"), hits(fast).get("total"));
    }

    /** Fetch-phase fields travel with the legs: filtered _source, docvalue fields, fields, version, seq_no/primary_term. */
    @SneakyThrows
    public void testFastPath_whenFetchFieldsRequested_thenTheyMatchRoundTwosFetch() {
        prepareIndex();
        String extra = "\"size\":3,\"track_total_hits\":false,\"_source\":{\"includes\":[\""
            + NUM_FIELD
            + "\"]},\"docvalue_fields\":[\""
            + NUM_FIELD
            + "\"],\"fields\":[\""
            + TEXT_FIELD
            + "\"],\"version\":true,\"seq_no_primary_term\":true";

        Map<String, Object> control = twoRoundControl(extra);
        Map<String, Object> fast = search(body(extra));

        assertClientVisibleIdentical(fast, control);
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
        for (String extra : List.of("\"from\":2,\"size\":3,\"track_total_hits\":false", "\"size\":0,\"track_total_hits\":" + THRESHOLD)) {
            assertClientVisibleIdentical(search(body(extra)), twoRoundControl(extra));
        }
    }

    /** explain is answered from the legs: the fused breakdown attaches to the assembled hits exactly as to round 2's. */
    @SneakyThrows
    public void testFastPath_whenExplain_thenTheFusedBreakdownIsIdentical() {
        prepareIndex();
        String extra = "\"size\":3,\"track_total_hits\":false,\"explain\":true";

        Map<String, Object> control = twoRoundControl(extra);
        Map<String, Object> fast = search(body(extra));

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
     * leg proves (the Tail counts the union), rescore, a sort, a named leg. Each answers exactly as before; that these
     * shapes refuse the fast path is pinned at unit level, and here their answers stay correct end to end.
     */
    @SneakyThrows
    public void testFastPath_whenTheRequestNeedsRoundTwo_thenTheAnswerIsUnchanged() {
        prepareIndex();
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
            assertNotNull(extra, hits(search(body(extra))).get("hits"));
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
        Map<String, Object> namedResponse = search(named);
        Map<String, Object> first = (Map<String, Object>) ((List<?>) hits(namedResponse).get("hits")).get(0);
        assertTrue(((List<?>) first.get("matched_queries")).contains("lex"));
    }

    /**
     * The one client-visible difference, pinned down. One shard, twelve identical documents: every fused score ties, so
     * round 2 would order the page by Lucene doc id while the fast path orders it by {@code _id} — "1", "10", "11", "12",
     * "2", ... — deterministically, on every replica, across merges. Ids, scores, sources, total and max_score are the
     * two-round path's. The {@code _id} order is itself the proof the fast path ran: round 2 cannot produce it here.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testFastPath_whenFusedScoresTie_thenTiesAreOrderedByIdAndEverythingElseMatchesTwoRounds() {
        String index = allTiesIndex();
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

        prime(index, requestBody);
        Map<String, Object> fast = searchIndex(index, requestBody);
        Map<String, Object> control = searchIndex(index, controlBody);
        control.remove("profile");

        List<Map<String, Object>> fastHits = (List<Map<String, Object>>) hits(fast).get("hits");
        List<String> fastIds = fastHits.stream().map(h -> (String) h.get("_id")).toList();
        assertEquals(DOCS, fastIds.size());
        assertEquals("every document ties, so the fast path orders the whole page by _id", fastIds.stream().sorted().toList(), fastIds);
        assertEquals("all twelve fused scores are the same value", 1, fastHits.stream().map(h -> h.get("_score")).distinct().count());
        assertClientVisibleIdentical(fast, control);
    }

    /**
     * The fast path under {@code rrf} end to end. RRF is where bit-identical fused scores arise without a contrived
     * corpus — two documents that hold the same rank in every leg receive the same rank-constant sum — so it is the
     * technique whose ties the new shard→{@code _index}+{@code _id} order replaces round 2's {@code _doc} order for.
     * The intrinsic tie order is pinned where the tie can be constructed exactly (the unit test
     * {@code testBuildFusedQuery_rrf_equalScoreTiesOrderByIndexThenId}); here the whole {@code rrf} request must answer
     * identically to its two-round control (forced with {@code profile:true}), up to that tie order, on a real index —
     * proving the fast path assembles an rrf page field for field, ties included.
     */
    @SneakyThrows
    public void testFastPath_whenRrf_thenPageMatchesTwoRounds() {
        String index = allTiesIndex();
        String query = "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + DOCS
            + ",\"combination\":{\"technique\":\"rrf\"}},\"queries\":[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":\"place\"}}]}}";
        String extra = "\"size\":" + DOCS + ",\"track_total_hits\":false,\"_source\":true";

        Map<String, Object> fast = searchIndex(index, "{" + extra + ",\"query\":" + query + "}");
        Map<String, Object> control = searchIndex(index, "{" + extra + ",\"profile\":true,\"query\":" + query + "}");
        control.remove("profile");
        assertClientVisibleIdentical(fast, control);
    }

    /**
     * The one sanctioned client-visible difference, observed at the page edge. On the all-ties index a page with
     * {@code from > 0} slices an equal-score run in the middle: {@code from:5,size:3} shows tied documents 5..7 of the
     * {@code _id}-ordered run. The fast path assembles that window from its {@code _id} order; round 2 would slice its
     * {@code _doc} order — so the two may show different tied documents at the boundary, which is exactly the documented
     * allowance. The test asserts the fast path returns a valid tied occupant for every slot and, up to tie order, the
     * same page as the control — i.e. the difference is confined to which equally-scored documents fall in the slice.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testFastPath_whenATiedRunStraddlesThePageEdge_thenTheFastPathKeepsValidTiedOccupants() {
        String index = allTiesIndex();
        int from = 5, size = 3;
        String extra = "\"from\":" + from + ",\"size\":" + size + ",\"track_total_hits\":false";
        String windowed = fusedQuery().replace("\"window_size\":" + WINDOW, "\"window_size\":" + DOCS);

        prime(index, "{" + extra + ",\"query\":" + windowed + "}");
        Map<String, Object> fast = searchIndex(index, "{" + extra + ",\"query\":" + windowed + "}");
        Map<String, Object> control = searchIndex(index, "{" + extra + ",\"profile\":true,\"query\":" + windowed + "}");
        control.remove("profile");

        List<Map<String, Object>> fastHits = (List<Map<String, Object>>) hits(fast).get("hits");
        assertEquals("the straddled window is full", size, fastHits.size());
        // Every document in this index ties, so the fast path's window is exactly _id-order positions [from, from+size).
        List<String> idOrder = new ArrayList<>();
        for (int i = 1; i <= DOCS; i++) {
            idOrder.add(String.valueOf(i));
        }
        idOrder.sort(java.util.Comparator.naturalOrder());
        List<String> expectedSlice = idOrder.subList(from, from + size);
        List<String> fastIds = fastHits.stream().map(h -> (String) h.get("_id")).toList();
        assertEquals("the fast path shows the _id-ordered occupants of the straddled slice", expectedSlice, fastIds);
        // Each is a valid tied occupant (present in the corpus, single tied score); the control is a valid slice too, and
        // the two agree up to tie order — the boundary difference is which tied ids land in the slice, nothing else.
        assertEquals("one tied score across the page", 1, fastHits.stream().map(h -> h.get("_score")).distinct().count());
        List<Map<String, Object>> controlHits = (List<Map<String, Object>>) hits(control).get("hits");
        assertEquals(size, controlHits.size());
        assertEquals("same tied score value on both paths", controlHits.get(0).get("_score"), fastHits.get(0).get("_score"));
    }

    /**
     * Multi-index page assembly, end to end. Two indices hold documents keyed by {@code (_index, _id)}; a single fused
     * request spans both. The composite keying, per-hit {@code _index}/{@code _source}, and cross-index
     * {@code _index}-then-{@code _id} tie order are only unit-touched elsewhere — here the assembled page must equal the
     * two-round control (forced with {@code profile:true}) field for field, proving the fast path reproduces round 2
     * across indices and never merges two same-{@code _id} documents from different indices into one.
     */
    @SneakyThrows
    public void testFastPath_whenSpanningTwoIndices_thenPageMatchesTwoRounds() {
        String a = INDEX + "-multi-a", b = INDEX + "-multi-b";
        for (String idx : List.of(a, b)) {
            if (indexExists(idx) == false) {
                createIndexWithConfiguration(
                    idx,
                    "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                        + TEXT_FIELD
                        + "\":{\"type\":\"text\"},\""
                        + NUM_FIELD
                        + "\":{\"type\":\"integer\"}}}}",
                    ""
                );
                // Same _ids in both indices (1..6) so the composite keying is actually exercised — a bare _id match would
                // merge them. Distinct-length text gives distinct scores so ordering is deterministic (no tie ambiguity).
                for (int i = 1; i <= 6; i++) {
                    Request request = new Request("PUT", "/" + idx + "/_doc/" + i + "?refresh=true");
                    request.setJsonEntity(
                        "{\"" + TEXT_FIELD + "\":\"hello place" + " here".repeat(i) + "\",\"" + NUM_FIELD + "\":" + i + "}"
                    );
                    client().performRequest(request);
                }
            }
        }
        String extra = "\"size\":8,\"track_total_hits\":false,\"_source\":true";
        String windowed = fusedQuery().replace("\"window_size\":" + WINDOW, "\"window_size\":12");
        // teach the gate both indices' _source size under this shape, so the request below is the fast-path exercise
        Request primeReq = new Request("POST", "/" + a + "," + b + "/_search");
        primeReq.setJsonEntity("{" + extra + ",\"query\":" + windowed + "}");
        client().performRequest(primeReq);
        Request fastReq = new Request("POST", "/" + a + "," + b + "/_search");
        fastReq.setJsonEntity("{" + extra + ",\"query\":" + windowed + "}");
        Map<String, Object> fast = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(client().performRequest(fastReq).getEntity()),
            false
        );
        Request controlReq = new Request("POST", "/" + a + "," + b + "/_search");
        controlReq.setJsonEntity("{" + extra + ",\"profile\":true,\"query\":" + windowed + "}");
        Map<String, Object> control = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(client().performRequest(controlReq).getEntity()),
            false
        );
        control.remove("profile");
        assertClientVisibleIdentical(fast, control);
    }

    /**
     * A search-pipeline request processor runs after the ActionFilter that arms the fast path and before the rewrite. Core's
     * {@code filter_query} wraps the submitted hybrid in {@code bool{must:[hybrid], filter:[…]}}; the answer must honour that
     * filter exactly as the two-round path does, for the page and for the derived total alike.
     */
    @SneakyThrows
    public void testFastPath_whenAFilterQueryRequestProcessorWrapsTheHybrid_thenTheInjectedFilterIsApplied() {
        prepareIndex();
        String onlyDocOne = "fast-path-filter-query-doc-one";
        String matchesNothing = "fast-path-filter-query-none";
        createFilterQueryPipeline(onlyDocOne, "{\"term\":{\"" + NUM_FIELD + "\":1}}");
        createFilterQueryPipeline(matchesNothing, "{\"term\":{\"" + NUM_FIELD + "\":" + (DOCS + 100) + "}}");
        try {
            // Fast-path shape: a page inside the ranked window (6 candidates), totals off, no source. Primed so a learned-size
            // cold start cannot hide the path.
            String fastShape = body("\"size\":5,\"track_total_hits\":false,\"_source\":false");
            primeThroughPipeline(onlyDocOne, fastShape);
            Map<String, Object> filteredToOne = searchThroughPipeline(onlyDocOne, fastShape);
            assertEquals("the injected filter admits document 1 alone", List.of("1"), ids(filteredToOne));
            assertClientVisibleIdentical(
                filteredToOne,
                twoRoundControlThroughPipeline(onlyDocOne, "\"track_total_hits\":false,\"_source\":false")
            );

            primeThroughPipeline(matchesNothing, fastShape);
            assertEquals(
                "a filter matching nothing leaves nothing to return",
                List.of(),
                ids(searchThroughPipeline(matchesNothing, fastShape))
            );

            // Default-totals shape (the threshold the fixture makes reachable): the total is the filtered count, not the legs'.
            // The non-empty case is the discriminating one — the legs' union would prove {THRESHOLD, gte} here, and only the
            // filtered count can say {1, eq}; the empty case is kept because many defects also reach 0.
            String countingShape = body("\"size\":5,\"track_total_hits\":" + THRESHOLD + ",\"_source\":false");
            primeThroughPipeline(onlyDocOne, countingShape);
            Map<String, Object> countedToOne = searchThroughPipeline(onlyDocOne, countingShape);
            assertEquals(List.of("1"), ids(countedToOne));
            assertEquals(
                "the total is the filtered count, not the legs' union of " + DOCS,
                Map.of("value", 1, "relation", "eq"),
                hits(countedToOne).get("total")
            );
            primeThroughPipeline(matchesNothing, countingShape);
            Map<String, Object> counted = searchThroughPipeline(matchesNothing, countingShape);
            assertEquals(List.of(), ids(counted));
            assertEquals(Map.of("value", 0, "relation", "eq"), hits(counted).get("total"));
        } finally {
            deletePipeline("_search", onlyDocOne);
            deletePipeline("_search", matchesNothing);
        }
    }

    /**
     * On the fast path round 2 is {@code match_none}. Core answers a match-none shard whose request was built after the first
     * shard responded with {@code QuerySearchResult.nullInstance()} — deterministic here because 12 shards are dispatched one
     * at a time — and an attached hybridization processor must skip those results rather than read their topDocs. This is the
     * zero-migration request shape: the user's normalization pipeline stays attached and {@code fusion} is added.
     *
     * <p>No priming is needed: the request carries {@code _source: false} and no field patterns, so the fetch gate has nothing
     * to weigh and never fails closed for it. That the fast path really ran — rather than the test passing on a quietly
     * two-round request that never produced a null shard result — is proved two ways: the page is identical to a two-round
     * control (a named leg) through the same pipeline, and the shard fetch-phase counter shows round 2 fetched nothing
     * where the control's round 2 fetched the page.
     */
    @SneakyThrows
    public void testFastPath_whenAHybridizationPipelineIsAttached_thenNullShardResultsDoNotFailTheRequest() {
        String index = "test-hybrid-fused-fast-path-twelve-shards";
        String pipeline = "fast-path-attached-normalization";
        createIndexWithConfiguration(
            index,
            "{\"settings\":{\"number_of_shards\":12,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                + TEXT_FIELD
                + "\":{\"type\":\"text\"}}}}",
            ""
        );
        for (int i = 1; i <= DOCS; i++) {
            Request request = new Request("PUT", "/" + index + "/_doc/" + i + "?refresh=true");
            request.setJsonEntity(
                "{\"" + TEXT_FIELD + "\":\"" + (i % 2 == 1 ? "hello place " + i : "hello there " + i) + " filler".repeat(i) + "\"}"
            );
            client().performRequest(request);
        }
        createSearchPipelineWithResultsPostProcessor(pipeline);
        try {
            String zeroMigration =
                "{\"size\":5,\"track_total_hits\":false,\"_source\":false,\"query\":{\"hybrid\":{\"fusion\":\"pipeline\","
                    + "\"queries\":[{\"match\":{\""
                    + TEXT_FIELD
                    + "\":\"hello\"}},{\"term\":{\""
                    + TEXT_FIELD
                    + "\":\"place\"}}]}}}";
            String twoRoundControl = zeroMigration.replace(
                "{\"term\":{\"" + TEXT_FIELD + "\":\"place\"}}",
                "{\"term\":{\"" + TEXT_FIELD + "\":{\"value\":\"place\",\"_name\":\"lex\"}}}"
            );
            List<String> expected = null;
            for (int attempt = 0; attempt < 6; attempt++) {
                Map<String, Object> fast = searchSerialised(index, pipeline, zeroMigration);
                List<String> page = ids(fast);
                assertEquals(5, page.size());
                if (expected == null) {
                    expected = page;
                }
                assertEquals("the same page every time", expected, page);
                Map<String, Object> control = searchSerialised(index, pipeline, twoRoundControl);
                control.put("hits", withoutMatchedQueries(hits(control)));
                assertClientVisibleIdentical(fast, control);
            }
            // The path itself, read off the shards: the control's round 2 fetches the page, the fast path's match_none
            // round 2 fetches nothing, so it costs the index strictly fewer fetch-phase executions.
            long before = fetchOps(index);
            searchSerialised(index, pipeline, twoRoundControl);
            long twoRoundOps = fetchOps(index) - before;
            before = fetchOps(index);
            searchSerialised(index, pipeline, zeroMigration);
            long fastOps = fetchOps(index) - before;
            assertTrue("round 2 fetched nothing on the fast path: " + fastOps + " vs two-round " + twoRoundOps, fastOps < twoRoundOps);
        } finally {
            deletePipeline("_search", pipeline);
            client().performRequest(new Request("DELETE", "/" + index));
        }
    }

    /** A search through {@code pipeline} with the shards dispatched one at a time, parsed. */
    @SneakyThrows
    private Map<String, Object> searchSerialised(String index, String pipeline, String requestBody) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("search_pipeline", pipeline);
        request.addParameter("max_concurrent_shard_requests", "1");
        request.setJsonEntity(requestBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /** The control's named leg registers {@code matched_queries} on every hit; the fast-path request has no named leg. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> withoutMatchedQueries(Map<String, Object> hits) {
        Map<String, Object> copy = new java.util.LinkedHashMap<>(hits);
        List<Map<String, Object>> stripped = new java.util.ArrayList<>();
        for (Map<String, Object> hit : (List<Map<String, Object>>) hits.get("hits")) {
            Map<String, Object> h = new java.util.LinkedHashMap<>(hit);
            h.remove("matched_queries");
            stripped.add(h);
        }
        copy.put("hits", stripped);
        return copy;
    }

    @SneakyThrows
    private void createFilterQueryPipeline(String pipelineId, String filterQuery) {
        Request request = new Request("PUT", "/_search/pipeline/" + pipelineId);
        request.setJsonEntity("{\"request_processors\":[{\"filter_query\":{\"query\":" + filterQuery + "}}]}");
        assertEquals(RestStatus.OK, RestStatus.fromCode(client().performRequest(request).getStatusLine().getStatusCode()));
    }

    @SneakyThrows
    private Map<String, Object> searchThroughPipeline(String pipelineId, String requestBody) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.addParameter("search_pipeline", pipelineId);
        request.setJsonEntity(requestBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    private Map<String, Object> twoRoundControlThroughPipeline(String pipelineId, String extra) {
        Map<String, Object> control = searchThroughPipeline(pipelineId, body(extra + ",\"profile\":true"));
        control.remove("profile");
        return control;
    }

    private void primeThroughPipeline(String pipelineId, String requestBody) {
        for (int i = 0; i < 2 * clusterNodes(); i++) {
            searchThroughPipeline(pipelineId, requestBody);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> total(Map<String, Object> response) {
        return (Map<String, Object>) ((Map<String, Object>) response.get("hits")).get("total");
    }

    @SuppressWarnings("unchecked")
    private static List<String> ids(Map<String, Object> response) {
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits(response).get("hits");
        return hitList.stream().map(h -> (String) h.get("_id")).collect(Collectors.toList());
    }

    /**
     * Run the request once per cluster node so its response teaches the fetch gate the index's {@code _source} size
     * under this shape on every coordinator. The gate fails closed until an index has been observed under a request's
     * {@code _source} filter (see {@code ObservedSourceSizes}), and what it has observed lives in the coordinator's own
     * JVM, so on a fresh index the first such request on each coordinator takes two rounds. The REST client spreads
     * requests over the nodes round-robin, so two turns of the rotation reach every coordinator whatever position
     * other requests have left it in; tests that assert fast-path behaviour prime first, as a warmed production fleet
     * would be.
     */
    @SneakyThrows
    private void prime(String index, String requestBody) {
        for (int i = 0; i < 2 * clusterNodes(); i++) {
            searchIndex(index, requestBody);
        }
    }

    /** The number of nodes the build started for this run; each is a coordinator with its own learned sizes. */
    private static int clusterNodes() {
        return Integer.parseInt(System.getProperty("cluster.number_of_nodes", "1"));
    }

    /** The index's cumulative shard-level fetch-phase executions — the per-request delta tells the path apart. */
    @SneakyThrows
    private long fetchOps(String index) {
        Response response = client().performRequest(new Request("GET", "/" + index + "/_stats/search"));
        Map<String, Object> stats = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
        Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) indices.get(index)).get("total");
        return ((Number) ((Map<String, Object>) total.get("search")).get("fetch_total")).longValue();
    }

    /**
     * The fetch-volume gate on text. The mapping cannot see how large a text document is, so the gate learns it from
     * responses: on a fresh index the first request on each coordinator takes two rounds (nothing observed yet — fail
     * closed), and its page teaches that coordinator the size. Twelve ~20 KB documents then weigh 2 × 100 − 3 = 197 extra × 20 KB ≈ 3.9 MB, far over the 1 MB
     * budget, so every later request stays on two rounds; the same request with {@code _source: false} carries nothing
     * and takes the fast path; and on an index of small documents the primed request takes the fast path. Every answer
     * is identical to the two-round control.
     *
     * <p>Which path ran is read off the index's shard fetch-phase counter. The legs fetch on every shard that holds a
     * hit; round 2 then fetches the page on the shards that hold it, while the fast path's {@code match_none} round
     * fetches nothing. The two reference counts are calibrated in the test itself — a named leg forces two rounds, and
     * {@code _source: false} is a known fast-path shape — rather than hard-coded, so shard skew cannot fake a result. The
     * indices have two shards: on a single-shard index core runs query and fetch in one shard round, and the fetch phase
     * executes (and counts) even for {@code match_none}, which would hide the difference.
     */
    @SneakyThrows
    public void testFastPath_whenDocumentsAreLargeText_thenTwoRoundsUnlessSourceIsOff() {
        String large = twoShardTextIndex(INDEX + "-large-text", "lorem ipsum ".repeat(1_700));
        String small = twoShardTextIndex(INDEX + "-small-text", "brief");
        String query = fusedQuery().replace("\"window_size\":" + WINDOW, "\"window_size\":100");
        String common = "\"size\":3,\"track_total_hits\":false";
        String namedQuery = query.replace(
            "\"match\":{\"" + TEXT_FIELD + "\":\"hello\"}",
            "\"match\":{\"" + TEXT_FIELD + "\":{\"query\":\"hello\",\"_name\":\"lex\"}}"
        );

        // calibrate on the large index: a named leg is two rounds, _source off is the fast path
        long twoRoundOps = fetchOpsOf(large, "{" + common + ",\"_source\":false,\"query\":" + namedQuery + "}");
        long fastOps = fetchOpsOf(large, "{" + common + ",\"_source\":false,\"query\":" + query + "}");
        assertTrue("round 2 fetches the page where match_none fetches nothing: " + twoRoundOps + " vs " + fastOps, twoRoundOps > fastOps);

        // cold: nothing observed for this index under _source:true anywhere → the first request takes two rounds, and
        // its page teaches the coordinator that served it the size
        String sourced = "{" + common + ",\"_source\":true,\"query\":" + query + "}";
        assertEquals("unobserved _source size: two rounds", twoRoundOps, fetchOpsOf(large, sourced));
        // warm every coordinator, then: ~20 KB documents weigh 197 extra × 20 KB ≈ 3.9 MB > 1 MB → still two rounds
        prime(large, sourced);
        assertEquals("large text observed: refused, two rounds", twoRoundOps, fetchOpsOf(large, sourced));
        Map<String, Object> warm = searchIndex(large, sourced);
        Map<String, Object> control = searchIndex(large, "{" + common + ",\"_source\":true,\"profile\":true,\"query\":" + query + "}");
        control.remove("profile");
        assertClientVisibleIdentical(warm, control);

        // small documents: the first sourced request is cold (two rounds), the second takes the fast path
        String smallSourced = "{" + common + ",\"_source\":true,\"query\":" + query + "}";
        long smallTwoRoundOps = fetchOpsOf(small, "{" + common + ",\"_source\":false,\"query\":" + namedQuery + "}");
        long smallFastOps = fetchOpsOf(small, "{" + common + ",\"_source\":false,\"query\":" + query + "}");
        assertTrue(smallTwoRoundOps > smallFastOps);
        assertEquals("cold: two rounds", smallTwoRoundOps, fetchOpsOf(small, smallSourced));
        prime(small, smallSourced);
        assertEquals("small documents observed: fast path", smallFastOps, fetchOpsOf(small, smallSourced));
        Map<String, Object> smallFast = searchIndex(small, smallSourced);
        Map<String, Object> smallControl = searchIndex(small, "{" + common + ",\"_source\":true,\"profile\":true,\"query\":" + query + "}");
        smallControl.remove("profile");
        assertClientVisibleIdentical(smallFast, smallControl);
    }

    /** The fetch-phase executions one request costs the index, measured as the counter's delta around it. */
    @SneakyThrows
    private long fetchOpsOf(String index, String requestBody) {
        long before = fetchOps(index);
        searchIndex(index, requestBody);
        return fetchOps(index) - before;
    }

    /** A two-shard index of twelve documents with distinct-length {@code text} and the given {@code body}; created once. */
    @SneakyThrows
    private String twoShardTextIndex(String index, String body) {
        if (indexExists(index) == false) {
            createIndexWithConfiguration(
                index,
                "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                    + TEXT_FIELD
                    + "\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}}",
                ""
            );
            for (int i = 1; i <= DOCS; i++) {
                Request request = new Request("PUT", "/" + index + "/_doc/" + i + "?refresh=true");
                request.setJsonEntity("{\"" + TEXT_FIELD + "\":\"hello place" + " filler".repeat(i) + "\",\"body\":\"" + body + "\"}");
                client().performRequest(request);
            }
        }
        return index;
    }

    /** The shared single-shard index whose every document ties on any fused score; created once, reused by tie tests. */
    @SneakyThrows
    private String allTiesIndex() {
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
        return index;
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
            boolean vectorDroppedFromSource = Boolean.parseBoolean(shape[1]);
            String body = "{" + source + common + ",\"query\":" + query + "}";
            Map<String, Object> control = searchIndex(index, "{" + source + common + ",\"profile\":true,\"query\":" + query + "}");
            control.remove("profile");
            Map<String, Object> response = searchIndex(index, body);
            // which path each shape takes is pinned by ReturnedEmbeddingFieldsTests' budget test; here both answer alike
            assertClientVisibleIdentical(response, control);
            List<Map<String, Object>> hits = (List<Map<String, Object>>) hits(response).get("hits");
            assertEquals(3, hits.size());
            boolean vectorReturned = hits.stream().anyMatch(h -> h.get("_source") instanceof Map<?, ?> src && src.containsKey("vec"));
            assertEquals(
                "the vector is in the page exactly when _source carries it: shape=" + shape[0],
                vectorDroppedFromSource == false,
                vectorReturned
            );
        }
    }

    /** A 768-dim vector whose first component is {@code lead} and the rest 1.0, as a JSON array. */
    /**
     * The ANN-hosted overlap aggregation, end to end on a real multi-shard index — the mechanism every other test here
     * exercises only through hand-built {@code MultiSearchResponse} items.
     *
     * <p>A {@code knn} + lexical hybrid with default totals derives its {@code hits.total} from one filter aggregation
     * carried by the {@code knn} leg: the ANN leg's own count, minus how many of its candidates the lexical leg also
     * matches, plus the lexical leg's count. What has to hold is that the derived object equals what the Tail-kept twin
     * reports — value AND relation — on a 2-shard index, where the aggregation's {@code doc_count} and the leg's
     * {@code totalHits} are reduced across shards independently. Asserted below and above the threshold, since the two
     * report different relations.
     */
    @SneakyThrows
    public void testTotalHits_whenAnAnnLegHostsTheOverlapAggregation_thenTheDerivedTotalMatchesTheTailKeptTwin() {
        String index = INDEX + "-ann-host";
        if (indexExists(index) == false) {
            createIndex(
                index,
                "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":2,\"number_of_replicas\":0}},"
                    + "\"mappings\":{\"properties\":{\""
                    + TEXT_FIELD
                    + "\":{\"type\":\"text\"},\"vec\":{\"type\":\"knn_vector\",\"dimension\":768,"
                    + "\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}}}}"
            );
            for (int i = 1; i <= DOCS; i++) {
                Request request = new Request("PUT", "/" + index + "/_doc/" + i + "?refresh=true");
                // Every document matches the lexical leg; only the odd ones carry "place", so the legs' match sets differ
                // and the overlap the aggregation counts is a real subset rather than everything.
                request.setJsonEntity(
                    "{\""
                        + TEXT_FIELD
                        + "\":\""
                        + (i % 2 == 1 ? "hello place " + i : "hello there " + i)
                        + "\",\"vec\":"
                        + vector768(1.0f + i / 10.0f)
                        + "}"
                );
                client().performRequest(request);
            }
        }
        int window = 10;
        String legs = "[{\"knn\":{\"vec\":{\"vector\":"
            + vector768(1.05f)
            + ",\"k\":"
            + window
            + "}}},"
            + "{\"term\":{\""
            + TEXT_FIELD
            + "\":\"place\"}}]";
        String query = "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + window
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},\"queries\":"
            + legs
            + "}}";

        // Two thresholds: one the union cannot reach (so the derived total is exact) and one it clears (so both paths
        // report the capped {threshold, gte}).
        for (int threshold : new int[] { DOCS + 5, 2 }) {
            String derived = "{\"size\":3,\"_source\":false,\"track_total_hits\":" + threshold + ",\"query\":" + query + "}";
            // A field-free aggregation forces the Tail and refuses the fast path, so this is the same request counted the
            // old way -- the only oracle that can catch a wrong value or a wrong relation.
            String tailKept = "{\"size\":3,\"_source\":false,\"track_total_hits\":"
                + threshold
                + ",\"aggs\":{\"n\":{\"filter\":{\"match_all\":{}}}},\"query\":"
                + query
                + "}";

            Map<String, Object> derivedResponse = searchIndex(index, derived);
            Map<String, Object> tailKeptResponse = searchIndex(index, tailKept);

            assertEquals(
                "threshold " + threshold + ": the ANN-hosted derivation must equal what the Tail counts",
                total(tailKeptResponse),
                total(derivedResponse)
            );
            assertEquals("and the page itself is unchanged", ids(tailKeptResponse), ids(derivedResponse));
        }
    }

    private static String vector768(float lead) {
        StringBuilder sb = new StringBuilder("[").append(lead);
        for (int d = 1; d < 768; d++) {
            sb.append(",1.0");
        }
        return sb.append("]").toString();
    }
}
