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
 * Now the legs count up to the request's threshold, and the Tail is dropped whenever the union's count can be had without
 * it. Three sources answer, in order of cost: a leg whose own count already exceeded the threshold proves the union does
 * too (core caps a tracked count at the threshold, so both paths say {@code {threshold, gte}}); a hybrid with an ANN leg
 * derives the union from overlap aggregations the legs carried; and a <b>lexical-only</b> hybrid whose legs all came back
 * exact and short of the threshold settles it with one {@code size: 0} count round over the legs' disjunction — cheap
 * there, where no ANN graph is walked twice, which is why the aggregations are not attached to that shape at all.
 * These tests pin that the visible response is the same whichever answered, and that the shapes where the Tail's documents
 * (not just its count) are part of the answer keep it.
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
    /** More than one, and enough of them that no single shard holds THRESHOLD of the DOCS matches: the union's crossing is
     *  what a leg's capped count proves, and that is only tested if no shard crosses on its own. */
    private static final int SHARDS = 4;
    /** The fused window: smaller than THRESHOLD so the count is genuinely "beyond the window". */
    private static final int WINDOW = 5;

    @SneakyThrows
    private void prepareIndex() {
        if (indexExists(INDEX)) {
            return;
        }
        createIndexWithConfiguration(
            INDEX,
            "{\"settings\":{\"number_of_shards\":"
                + SHARDS
                + ",\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
                + TEXT_FIELD
                + "\":{\"type\":\"text\"}}}}",
            ""
        );
        for (int i = 1; i <= DOCS; i++) {
            // Every doc carries "hello" (lexical leg); the odd ones also carry "place" (second leg), so the union is all DOCS
            // and the two legs' match sets differ.
            addDocument(
                INDEX,
                String.valueOf(i),
                TEXT_FIELD,
                (i % 2 == 1 ? "hello place " + i : "hello there " + i) + " filler".repeat(i),
                null,
                null
            );
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
        return search(body, Map.of());
    }

    @SneakyThrows
    private Map<String, Object> search(String body, Map<String, String> params) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(body);
        params.forEach(request::addParameter);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    /** The same body with a field-free aggregation, which keeps the Tail and refuses the fast path: the two-round twin. */
    private static String withTailKept(String body) {
        return "{\"aggs\":{\"n\":{\"filter\":{\"match_all\":{}}}}," + body.substring(1);
    }

    /**
     * Issue a body until the fast path can arm for it, and discard the responses.
     *
     * <p>The fetch-volume gate that arms the fast path is <b>learned</b>: it refuses until the coordinator has observed
     * this index-and-shape's {@code _source} size from an earlier execution, and asking the legs to count at all rides on
     * the same decision. So the first request of a shape takes round 2 with its Tail whatever its count says, and every
     * fetch-op oracle below — which compares an armed request against a Tail-kept one — would otherwise pass or fail on
     * nothing but this test's position in a randomized execution order. Measured with
     * {@code tests.seed=4C64F1DB366F31C3}, where this class's first test was the one holding the oracle: 7 fetch ops on
     * both sides, i.e. the "derived" side was still running round 2.
     *
     * <p>The loop count is not cosmetic. That observation table is <b>per coordinator node</b>, and the REST client
     * round-robins over all {@code numNodes} of them, so warming once — or even three times on a 3-node cluster — leaves
     * it to chance whether the node that serves the measured request has an observation. Confirmed on the live 2-node-behind-
     * a-load-balancer cluster, where the same shape derives from its second execution onward but never on its first.
     */
    @SneakyThrows
    private void armFastPath(String body) {
        for (int i = 0; i < 12; i++) {
            search(body);
        }
    }

    /** Shard fetch operations one request costs the index (from {@code _stats/search}); the fast path fetches only its legs. */
    @SneakyThrows
    private long fetchOpsOf(String body) {
        long before = fetchOps();
        search(body);
        return fetchOps() - before;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private long fetchOps() {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_stats/search"));
        Map<String, Object> stats = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
        Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) indices.get(INDEX)).get("total");
        return ((Number) ((Map<String, Object>) total.get("search")).get("fetch_total")).longValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> total(Map<String, Object> response) {
        return (Map<String, Object>) ((Map<String, Object>) response.get("hits")).get("total");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hits(Map<String, Object> response) {
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    /** The page's document ids in order, which is what has to agree across the derived and the Tail-kept path. */
    private static List<String> ids(Map<String, Object> response) {
        return hits(response).stream().map(hit -> String.valueOf(hit.get("_id"))).toList();
    }

    /** The page's scores in order. */
    private static List<Double> scores(Map<String, Object> response) {
        return hits(response).stream().map(hit -> ((Number) hit.get("_score")).doubleValue()).toList();
    }

    /**
     * The parity oracle for the derived value itself: the SAME body at the SAME {@code track_total_hits}, forced onto the
     * Tail-kept path by an aggregation, has to report the same {@code hits.total} object. The exact-totals control in the
     * test above reports a different value ({@code {DOCS, eq}}) by design and so cannot catch an off-by-one or an eq/gte
     * slip at the cap; this can. The aggregation is field-free on purpose — its only job is to make
     * {@code needsTail} true.
     *
     * <p>Deliberately not profiled, unlike the {@code tail_built} test below: {@code profile} over a fused round 2 that also
     * carries an aggregation trips core's {@code ConcurrentQueryProfileBreakdown} assert and kills the node — a pre-existing
     * defect unrelated to this change. The Tail-kept side is pinned at unit level instead, by
     * {@code HybridFusionOrchestratorTests#testRequestShapeAllowsDerivedTotalHits}.
     */
    @SneakyThrows
    public void testTotalHits_whenTheTailIsKeptAtTheSameThreshold_thenTheTotalIsIdentical() {
        prepareIndex();
        String tail = ",\"track_total_hits\":" + THRESHOLD + ",\"query\":" + fusedQuery(WINDOW) + "}";
        String aggregation = ",\"aggs\":{\"all\":{\"filter\":{\"match_all\":{}}}}";

        Map<String, Object> derived = search("{\"size\":" + WINDOW + tail);
        Map<String, Object> tailKept = search("{\"size\":" + WINDOW + aggregation + tail);

        assertEquals("both paths report the same total at the same threshold", total(tailKept), total(derived));
        assertEquals(THRESHOLD, total(derived).get("value"));
        assertEquals("gte", total(derived).get("relation"));
    }

    /**
     * The property the derivation rests on: core caps a tracked count per sub-search, not per shard. No single shard of this
     * index holds THRESHOLD matches — each answers {@code eq} below it — yet the union crosses the threshold, and it is the
     * union's crossing that a leg's own capped count proves. Pinned through {@code preference=_shards:N}, which is
     * propagated to the legs, so a shard-restricted fused request really does fuse against that shard alone.
     */
    @SneakyThrows
    public void testTotalHits_whenNoSingleShardReachesTheThreshold_thenTheUnionStillProvesIt() {
        prepareIndex();
        String body = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + THRESHOLD + ",\"query\":" + fusedQuery(WINDOW) + "}";

        int summed = 0;
        for (int shard = 0; shard < SHARDS; shard++) {
            Map<String, Object> oneShard = search(body, Map.of("preference", "_shards:" + shard));
            assertEquals("shard " + shard + " counted its matches exactly", "eq", total(oneShard).get("relation"));
            int shardTotal = (int) total(oneShard).get("value");
            assertTrue("shard " + shard + " alone must stay below the threshold, was " + shardTotal, shardTotal < THRESHOLD);
            summed += shardTotal;
        }
        assertEquals("the shards partition the union", DOCS, summed);

        Map<String, Object> union = search(body);
        assertEquals(THRESHOLD, total(union).get("value"));
        assertEquals("gte", total(union).get("relation"));
    }

    /**
     * A {@code rescore} on the derived path. Dropping the Tail removes round 2's {@code filter} clause outright, so its match
     * set equals its ranked set and no weighting has a document the fusion never ranked available to promote — the inverse of
     * the Tail-kept path, where {@code FusedWindowGuardRescorer} has to demote the Tail-only documents after the request's own
     * rescorers ran. {@code rescore} also cannot move {@code hits.total}: core's rescore phase runs after the query phase's
     * collector produced it. Both are pinned here against the exact-totals Tail-kept arm — same page, same scores — while the
     * fact that the Tail really was dropped is pinned at unit level, by
     * {@code HybridQueryFusedFanOutTests#testRewrite_whenTheRequestCarriesARescore_thenTheTotalIsStillDerivedAndNoTailIsBuilt}
     * (profiling this shape trips core's {@code ConcurrentQueryProfileBreakdown} assert, and the coverage job skips ITs
     * anyway).
     */
    @SneakyThrows
    public void testTotalHits_whenARescoreRunsOnTheDerivedPath_thenThePageMatchesTheTailKeptPath() {
        prepareIndex();
        for (String queryWeight : List.of("1.0", "0.0")) {
            // match_all as the rescore query so every window document is one the rescore touched, whatever the window is.
            String rescore = ",\"rescore\":{\"window_size\":"
                + WINDOW
                + ",\"query\":{\"rescore_query\":{\"match_all\":{}},\"query_weight\":"
                + queryWeight
                + ",\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}}";
            String query = ",\"query\":" + fusedQuery(WINDOW) + rescore + "}";

            Map<String, Object> derived = search("{\"size\":" + WINDOW + ",\"track_total_hits\":" + THRESHOLD + query);
            Map<String, Object> tailKept = search("{\"size\":" + WINDOW + ",\"track_total_hits\":true" + query);

            assertEquals("query_weight " + queryWeight + ": the same page", ids(tailKept), ids(derived));
            assertEquals("query_weight " + queryWeight + ": the same scores", scores(tailKept), scores(derived));
            assertTrue(
                "query_weight " + queryWeight + ": the rescore has to have run — every window document matched it",
                scores(derived).stream().allMatch(score -> score >= 10.0)
            );
            assertEquals(THRESHOLD, total(derived).get("value"));
            assertEquals("gte", total(derived).get("relation"));
        }
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
    public void testTotalHits_whenNoLegReachesTheThreshold_thenTheUnionIsCountedExactlyFromRoundOne() {
        prepareIndex();
        String body = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + (DOCS + 5) + ",\"query\":" + fusedQuery(WINDOW) + "}";

        Map<String, Object> response = search(body);

        assertEquals(DOCS, total(response).get("value"));
        assertEquals("eq", total(response).get("relation"));
        // The count did not come from a Tail: no round 2 fetched anything, where the Tail-kept twin (an aggregation keeps
        // the Tail and refuses the fast path) fetches its page in round 2. These legs are lexical-only, so what answered
        // is the count round; testTotalHits_whenLegsAreLexicalOnly_thenACountRoundReplacesTheTail pins that directly.
        armFastPath(body);
        long derivedOps = fetchOpsOf(body);
        long tailKeptOps = fetchOpsOf(withTailKept(body));
        assertTrue("derived " + derivedOps + " fetch ops vs the Tail-kept twin's " + tailKeptOps, derivedOps < tailKeptOps);
    }

    /**
     * A union smaller than the corpus, with legs that overlap partially: {@code place} (the 6 odd documents) and the
     * document whose text carries the token {@code 3} (odd, so inside {@code place}). Union = 6. The derived count has to
     * equal what the Tail-kept path (forced by a field-free aggregation) reports for the same request.
     */
    public void testTotalHits_whenLegsOverlapPartially_thenTheDerivedCountMatchesTheTailKeptPath() {
        prepareIndex();
        String legs = "[{\"term\":{\"" + TEXT_FIELD + "\":\"place\"}},{\"term\":{\"" + TEXT_FIELD + "\":\"3\"}}]";
        String query = "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},\"queries\":"
            + legs
            + "}}";
        String derived = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + (DOCS + 5) + ",\"query\":" + query + "}";
        String tailKept = "{\"size\":"
            + WINDOW
            + ",\"track_total_hits\":"
            + (DOCS + 5)
            + ",\"aggs\":{\"n\":{\"filter\":{\"match_all\":{}}}},\"query\":"
            + query
            + "}";

        Map<String, Object> derivedResponse = search(derived);
        Map<String, Object> tailKeptResponse = search(tailKept);

        assertEquals(6, total(derivedResponse).get("value"));
        assertEquals("eq", total(derivedResponse).get("relation"));
        assertEquals(total(tailKeptResponse), total(derivedResponse));
        armFastPath(derived);
        long derivedOps = fetchOpsOf(derived);
        long tailKeptOps = fetchOpsOf(tailKept);
        assertTrue("derived " + derivedOps + " fetch ops vs the Tail-kept twin's " + tailKeptOps, derivedOps < tailKeptOps);
    }

    /**
     * Three legs — {@code hello} (all), {@code place} (odd), {@code there} (even) — every document counted once although
     * {@code place} and {@code there} each sit entirely inside {@code hello}: union = DOCS, derived, no Tail.
     */
    public void testTotalHits_whenThreeLegsOverlap_thenTheUnionIsCountedOnceFromRoundOne() {
        prepareIndex();
        String legs = "[{\"match\":{\""
            + TEXT_FIELD
            + "\":\"hello\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":\"place\"}},{\"term\":{\""
            + TEXT_FIELD
            + "\":\"there\"}}]";
        String query = "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},\"queries\":"
            + legs
            + "}}";
        String body = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + (DOCS + 5) + ",\"query\":" + query + "}";

        Map<String, Object> response = search(body);

        assertEquals(DOCS, total(response).get("value"));
        assertEquals("eq", total(response).get("relation"));
        armFastPath(body);
        long derivedOps = fetchOpsOf(body);
        long tailKeptOps = fetchOpsOf(withTailKept(body));
        assertTrue("derived " + derivedOps + " fetch ops vs the Tail-kept twin's " + tailKeptOps, derivedOps < tailKeptOps);
    }

    /** {@code hit_count} and {@code miss_count} of the index's request cache, from {@code _stats/request_cache}. */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    private long[] requestCacheCounts() {
        Response response = client().performRequest(new Request("GET", "/" + INDEX + "/_stats/request_cache"));
        Map<String, Object> stats = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
        Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) indices.get(INDEX)).get("total");
        Map<String, Object> cache = (Map<String, Object>) total.get("request_cache");
        return new long[] { ((Number) cache.get("hit_count")).longValue(), ((Number) cache.get("miss_count")).longValue() };
    }

    /**
     * The lazy count round, pinned by the trace it leaves rather than by what it reports — the only assertion in this class
     * that fails if the count round silently stops running. Every test above would still pass with the Tail quietly
     * restored, because the Tail reports the same total.
     *
     * <p>The trace is the <b>request cache</b>. The count round is a {@code size: 0} search, which is exactly the shape core
     * caches per shard; the legs ({@code size = window_size}) and round 2 are not, so any request-cache activity on this
     * index during a fused search is the count round and nothing else. So: a lexical-only request that wants a count misses
     * the cache on its first execution, while the {@code track_total_hits: false} twin — the same request with nothing to
     * count, running the same legs down the same fast path — touches the cache not at all.
     *
     * <p>The threshold is one this class uses nowhere else, so the first execution is guaranteed a cold cache.
     *
     * <p>The repeat is <b>logged, not asserted</b>: measured here it is +4 hits / +0 misses on 4 shards, so a workload of
     * recurring queries does pay for this round once — but a refresh between the two executions changes the shard's reader
     * cache key and legitimately turns the repeat back into a miss, which is not a defect and must not fail a build.
     */
    @SneakyThrows
    public void testTotalHits_whenLegsAreLexicalOnly_thenACountRoundReplacesTheTailAndIsCached() {
        prepareIndex();
        String counted = "{\"size\":" + WINDOW + ",\"track_total_hits\":" + (DOCS + 7) + ",\"query\":" + fusedQuery(WINDOW) + "}";
        String notCounted = "{\"size\":" + WINDOW + ",\"track_total_hits\":false,\"query\":" + fusedQuery(WINDOW) + "}";

        // Arm the fast path through the twin, not through `counted`. The observation table the gate reads is keyed by index
        // and _source shape, which both bodies share, so warming either arms both -- and warming the twin leaves `counted`'s
        // request cache untouched, which the cold-miss assertion below depends on. Warming `counted` here instead is what
        // made an earlier version of this test fail on tests.seed=D9AD370904EFE661: whichever body is measured first is the
        // one that is still un-armed, and an un-armed request runs round 2 and fetches its page.
        armFastPath(notCounted);

        // Phase A: the count round's trace in the request cache -- cold on the first execution, a hit on the second.
        long[] before = requestCacheCounts();
        Map<String, Object> first = search(counted);
        long[] afterCold = requestCacheCounts();
        Map<String, Object> second = search(counted);
        long[] afterWarm = requestCacheCounts();
        // Phase B: the twin's own cache delta, measured around the twin alone. Reading it after any further `counted`
        // execution would attribute that execution's hits to the twin.
        long[] twinCacheBefore = requestCacheCounts();
        search(notCounted);
        long[] twinCacheAfter = requestCacheCounts();
        // Phase C: shard fetch operations, one execution each, both bodies armed.
        long fetchesBefore = fetchOps();
        search(counted);
        long countedFetches = fetchOps() - fetchesBefore;
        fetchesBefore = fetchOps();
        search(notCounted);
        long plainFetches = fetchOps() - fetchesBefore;

        assertEquals("the union is every document, counted exactly", DOCS, total(first).get("value"));
        assertEquals("eq", total(first).get("relation"));
        assertEquals("and the cached repeat reports the same", total(first), total(second));
        assertTrue("a size:0 count round was issued: " + (afterCold[1] - before[1]) + " cache misses", afterCold[1] > before[1]);
        logger.info(
            "union count cache: cold hits/misses +{}/+{}, warm +{}/+{}",
            afterCold[0] - before[0],
            afterCold[1] - before[1],
            afterWarm[0] - afterCold[0],
            afterWarm[1] - afterCold[1]
        );
        assertEquals("the twin wants no count, so it issues no cacheable round", twinCacheBefore[0], twinCacheAfter[0]);
        assertEquals("the twin wants no count, so it issues no cacheable round", twinCacheBefore[1], twinCacheAfter[1]);
        // Both bodies are armed by now, so this compares like with like: the count is size:0, so it adds no shard fetch,
        // and the Tail it replaced is gone. Were the Tail still there this would still hold -- a Top-only round 2 fetches
        // the same page -- which is why the cache assertions above, not this one, are what pin the count round's existence.
        assertEquals("the count wants no documents, so it adds no fetch", plainFetches, countedFetches);
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
            "no leg reaches the threshold: profiled legs carry no overlap aggregation (core's profile breakdown asserts on a profiled "
                + "aggregating search), so the profiled request keeps the Tail — the unprofiled request derives the count, see the "
                + "fetch-op tests",
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
