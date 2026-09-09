/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
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
 * What {@code rescore} means on a fused ({@code fusion}) {@code hybrid} query: it may override the hybrid's score scale,
 * and it may only boost documents the hybrid already returned. A document the fusion did not rank cannot enter the
 * results because a rescore query matched it.
 *
 * <p>Both halves are pinned here against a live cluster because neither is visible to a unit test. {@code rescore} is
 * never propagated to a leg — it is core's own two-round mechanism, so it runs once per shard, after the coordinator has
 * already fused and self-erased. Round 2 is {@code bool{ Top + Tail }}, and the Tail is a non-scoring {@code filter}: every
 * document any leg matched is a candidate there, sitting at {@code 0.0}. Two request-independent multipliers then decide
 * how many of those unranked candidates a rescore actually reaches, which is why the grid below varies both:
 *
 * <ol>
 *   <li><b>The rescore window can exceed the fused window.</b> {@code rescore.window_size} is the user's number and has no
 *       relation to {@code fusion.window_size}; the moment it is larger, the surplus is filled with Tail-only documents.</li>
 *   <li><b>Per-shard shortfall.</b> Core sizes the rescore window against the <i>shard's</i> own reader, while the fused
 *       window is coordinator-global — so a shard holding 2 of 5 ranked documents rescores 3 unranked ones even when
 *       {@code rescore.window_size} equals {@code fusion.window_size} exactly. This is the ordinary multi-shard case, not
 *       an edge one, which is why every assertion below runs against a 1-shard and a 3-shard index and expects the
 *       identical page from both.</li>
 * </ol>
 *
 * <p>Confining each rescore query to the fused window closes both at once: neither multiplier can widen what the rescore
 * query matches, so the answer stops depending on either. It also closes them at the <i>second</i> place core applies the
 * request's rescorers — inside every {@code top_hits} bucket, where the bucket's collector is widened by
 * {@code rescore.window_size} in exactly the same way ({@link #testRescoreConfinementAlsoHoldsInsideTopHitsBuckets}).
 * Rewriting the query rather than the page is what makes one fix cover both sites.
 *
 * <p><b>What each combination returned before the fix</b>, measured 2026-08-27 by disabling only the confinement and
 * re-running this class — worth keeping, because which combinations failed is what identifies the multiplier. Expected page
 * is {@code [5, 1, 2, 3, 4]} throughout:
 *
 * <pre>
 *   1 shard,  rescore_window=5   ids[5,6]      [5, 1, 2, 3, 4]   correct — the rescore reached only ranked documents
 *   1 shard,  rescore_window=5   range[id>=5]  [5, 1, 2, 3, 4]   correct — same
 *   1 shard,  rescore_window=20  ids[5,6]      [5, 6, 1, 2, 3]   multiplier 1: document 6 lifted, document 4 evicted
 *   1 shard,  rescore_window=20  range[id>=5]  [5, 6, 7, 8, 9]   multiplier 1: four unranked documents took the page
 *   3 shards, rescore_window=5   ids[5,6]      [5, 6, 1, 2, 3]   multiplier 2 alone — the rescore window was not exceeded
 *   3 shards, rescore_window=5   range[id>=5]  [5, 7, 13, 22, 24] multiplier 2 alone
 *   3 shards, rescore_window=20  ids[5,6]      [5, 6, 1, 2, 3]   both
 *   3 shards, rescore_window=20  range[id>=5]  [5, 7, 13, 22, 24] both
 *  20 shards, rescore_window unset, range      [5, 11, 13, 17, 19] multiplier 2, at nothing but defaults
 *   3 shards, track_total_hits off + agg       [5, 7, 13, 22, 24] multiplier 2 with the Tail opened by the aggregation
 *   1 shard,  top_hits bucket, window=20      [5, 6, 7, 8, 9]    multiplier 1 inside a bucket, not on the page
 * </pre>
 *
 * The two correct rows are the reason the grid is a grid: a single-shard test at a rescore window equal to the fused window
 * cannot see this defect at all, and that is the shape a first attempt at a regression test naturally takes. The
 * {@code 20 shards} row is the other end — a request that names no rescore window at all, on an ordinary sharded index,
 * losing four of five page slots to documents fusion never ranked.
 *
 * <p>{@link #testRescoreStillPromotesWhenTheFusedWindowCoversEverything} is the one test that <i>passed</i> in that run, and
 * is meant to: it pins the behaviour the fix must not take away.
 *
 * <p><b>Two hazards the grid above cannot reach</b>, each with its own measured run, because each one lets a confinement look
 * complete while shipping nothing:
 *
 * <ol>
 *   <li><b>The confinement has to reach the shards.</b> A rescore query that rewrites on the coordinator's first pass makes
 *       core replace the request's whole rescore list, so a confinement applied after the leg fan-out lands on an orphan. Every
 *       rescore query above rewrites to itself and therefore cannot see this — measured against a callback-time confinement,
 *       the entire grid passed and only {@link #testConfinementSurvivesARewritingRescoreQuery} and
 *       {@link #testConfinementSurvivesARewritingRescoreQueryInsideATopHitsBucket} failed.</li>
 *   <li><b>The floor has to survive attenuation.</b> The value that keeps a ranked document above the Tail is multiplied by
 *       {@code query_weight} and by any enclosing {@code boost} before it is compared to anything, so a subnormal floor is
 *       annihilated by a factor of {@code 0.5}. {@link #testFusedRankedDocumentAtZero_stillOutranksTheTail} checks the floor
 *       at {@code 1.0} only and passed against a subnormal one; {@link #testTheRankedFloorSurvivesRescoreAttenuation} and
 *       {@link #testTheRankedFloorSurvivesAnEnclosingBoost} are what catch the value being wrong.</li>
 * </ol>
 *
 * <p>{@link #testFusedRankedDocumentAtZero_stillOutranksTheTail} is a different defect with the same symptom and no rescore
 * involved at all, and {@link #testQueryWeightZero_collapsesTheScaleButStillCannotLiftAnUnrankedDocument} pins the one request
 * shape confinement cannot fully repair.
 *
 * <p>Run: {@code ./gradlew integTest --tests "*HybridQueryFusedModeRescoreIT*"}.
 */
public class HybridQueryFusedModeRescoreIT extends BaseNeuralSearchIT {

    /** One shard: the fused window's own tie-breaking is only deterministic when one collector sees every document. */
    private static final String INDEX_ONE_SHARD = "test-fused-rescore-1-shard";
    /** Three shards: the shape where a per-shard rescore window covers unranked documents on its own. */
    private static final String INDEX_THREE_SHARDS = "test-fused-rescore-3-shards";
    /** Twenty shards over 30 documents: the per-shard shortfall at its worst, where almost no shard holds a ranked document. */
    private static final String INDEX_MANY_SHARDS = "test-fused-rescore-20-shards";
    private static final String SCORE_FIELD = "s";
    private static final int TOTAL_DOCS = 30;
    /** {@code s = SCORE_BASE - id}, so ranking by {@code s} descending ranks by {@code _id} ascending. */
    private static final int SCORE_BASE = 1000;
    private static final int WINDOW_SIZE = 5;

    /**
     * The specification, verbatim: a hybrid returning {@code [1, 2, 3, 4, 5]} with a rescore matching {@code [5, 6]} must
     * return {@code [5, 1, 2, 3, 4]}. Document 5 is promoted to the top — the rescore has overridden the hybrid's scale
     * outright — and document 6 does not appear, because the hybrid never ranked it.
     *
     * <p>Asserted over both shard counts, both rescore windows and both shapes of rescore query, and the expected page is
     * the same one in all eight: the point of confining the rescore is that the answer no longer depends on any of them.
     */
    @SneakyThrows
    public void testRescoreOverridesTheScaleButOnlyBoostsDocumentsTheHybridRanked() {
        ensureDatasets();
        // Well above the top fused score of 1.0, so the promotion is unambiguous rather than a near-tie.
        float boost = 10.0f;

        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, String> shape : rescoreShapes(boost).entrySet()) {
            Map<String, Object> response = searchForHits(shape.getValue());
            collectMismatches(mismatches, shape.getKey(), List.of("5", "1", "2", "3", "4"), response);

            // Requirement 1, on the scores themselves: document 5 fused at 0.001 — last in the window — and comes back at
            // ~10.001. The rescore's contribution is what the page is now ordered by, not the fused score it replaced.
            List<Double> scores = hitScores(response);
            collect(mismatches, shape.getKey(), "the first hit (document 5) must carry the rescore's scale", 10.001, scores.get(0));
            collect(mismatches, shape.getKey(), "the second hit (document 1) must still carry the hybrid's", 1.0, scores.get(1));
        }
        assertEquals("every combination must produce the same page", List.of(), mismatches);
    }

    /**
     * The same contract with a boost small enough that the promotion lands mid-window: document 5 moves from last to third,
     * and every document it passes is one the hybrid ranked. A rescore reordering the hybrid's own hits is the whole point
     * of allowing it — this is the case that must keep working, not just the one that must be prevented.
     *
     * <p>Chosen against the fused scores, which are exact and shard-count independent here: the window is
     * {@code [1.0, 0.75, 0.5, 0.25, 0.001]}, so {@code 0.001 + 0.6} lands between documents 2 and 3 with room on both sides.
     */
    @SneakyThrows
    public void testRescoreReordersWithinTheWindow() {
        ensureDatasets();
        float boost = 0.6f;

        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, String> shape : rescoreShapes(boost).entrySet()) {
            Map<String, Object> response = searchForHits(shape.getValue());
            collectMismatches(mismatches, shape.getKey(), List.of("1", "2", "5", "3", "4"), response);
        }
        assertEquals("every combination must produce the same page", List.of(), mismatches);
    }

    /**
     * Records a wrong page rather than throwing, so one run reports <i>every</i> failing combination in the grid. Which
     * combinations fail is the diagnosis — a rescore window at the fused window failing only on the multi-shard index is
     * the per-shard shortfall, and one failing on both is the window inequality.
     */
    private void collectMismatches(List<String> mismatches, String shape, List<String> expected, Map<String, Object> response) {
        List<String> actual = hitIds(response);
        if (expected.equals(actual) == false) {
            mismatches.add(shape + ": expected " + expected + " but got " + actual);
        }
        if (totalHits(response) != TOTAL_DOCS) {
            mismatches.add(shape + ": the Tail must still match every document, but total_hits was " + totalHits(response));
        }
    }

    private void collect(List<String> mismatches, String shape, String what, double expected, double actual) {
        if (Math.abs(expected - actual) > 0.01) {
            mismatches.add(shape + ": " + what + " (~" + expected + ") but it was " + actual);
        }
    }

    /**
     * The stock case, and the one whose absence let an earlier fix pair look complete: an <b>unset</b>
     * {@code rescore.window_size} — so core's own default, which the request never mentions — on an index with far more
     * shards than the fused window has documents. 20 shards over 30 documents leaves most shards holding no ranked document
     * at all, so a per-shard rescore window is filled almost entirely from the Tail. Nothing here is unusual or adversarial:
     * a default rescore over a sharded index is what a user writes first.
     *
     * <p>The range query is used rather than {@code ids}, because it matches every document from 5 up: whatever shard layout
     * the {@code _id}s happen to route to, several unranked candidates are within reach on every shard.
     *
     * <p>Measured 2026-08-27 with the confinement disabled: {@code [5, 11, 13, 17, 19]} — one ranked document left on a
     * five-document page.
     */
    @SneakyThrows
    public void testDefaultRescoreWindowAtHighShardCount_admitsNothingUnranked() {
        ensureDatasets();

        Map<String, Object> response = searchForHits(
            INDEX_MANY_SHARDS,
            rescoredSearch("{\"range\":{\"" + SCORE_FIELD + "\":{\"lte\":" + (SCORE_BASE - WINDOW_SIZE) + "}}}", null, 10.0f)
        );

        assertEquals(List.of("5", "1", "2", "3", "4"), hitIds(response));
        assertEquals("the Tail still matches every document", TOTAL_DOCS, totalHits(response));
    }

    /**
     * The other direction, and the reason this is confinement rather than a ban: when the fused window covers the whole
     * corpus, <i>every</i> document is a ranked document, so a rescore may legitimately promote any of them — including the
     * one fusion put last. Document 30 fuses to min_max's floor of {@code 0.001} and comes back first.
     *
     * <p>This is the test that fails if the window filter is ever built from something narrower than what fusion actually
     * ranked: it is only satisfied by a filter that admits all 30 documents. {@code rescore.window_size} has to be set to 30
     * as well, since core rescores by round-2 score and document 30 is the last of them — an unset window would stop at 10
     * and never reach it. That is core's own behaviour, unchanged here.
     *
     * <p>It is also the only test in this class that passes on unfixed code, and deliberately so: this is the direction the
     * fix must not take away, not a defect it closes.
     */
    @SneakyThrows
    public void testRescoreStillPromotesWhenTheFusedWindowCoversEverything() {
        ensureDatasets();

        String body = "{\"size\":"
            + WINDOW_SIZE
            + ",\"query\":"
            + fusedHybrid(TOTAL_DOCS)
            + ",\"rescore\":{\"window_size\":"
            + TOTAL_DOCS
            + ",\"query\":{\"rescore_query\":{\"ids\":{\"values\":[\"30\"]}},"
            + "\"query_weight\":1.0,\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}}}";
        Map<String, Object> response = searchForHits(INDEX_ONE_SHARD, body);

        assertEquals(List.of("30", "1", "2", "3", "4"), hitIds(response));
        // 0.001 (fusion's floor for the lowest of 30 documents) + 10 x the ids query's 1.0.
        assertEquals(10.001, hitScores(response).get(0), 0.01);
        assertEquals(1.0, hitScores(response).get(1), 0.01);
    }

    /**
     * The same contract on the path where the Tail is opened for a reason other than counting hits. {@code track_total_hits}
     * off would let fused mode drop the Tail — and with no Tail there are no unranked candidates for a rescore to reach at
     * all — but an aggregation re-opens it, because an aggregation has to see every document any leg matched. So this is a
     * request that asks for no totals and still carries the full candidate set into round 2, and the confinement has to hold
     * there too.
     *
     * <p>Measured 2026-08-27 with the confinement disabled: {@code [5, 7, 13, 22, 24]}.
     */
    @SneakyThrows
    public void testConfinementHoldsWhenTheTailIsOpenedForAnAggregation() {
        ensureDatasets();

        String body = "{\"size\":"
            + WINDOW_SIZE
            + ",\"track_total_hits\":false,\"aggs\":{\"scored\":{\"value_count\":{\"field\":\""
            + SCORE_FIELD
            + "\"}}},\"query\":"
            + fusedHybrid(WINDOW_SIZE)
            + ",\"rescore\":{\"window_size\":20,\"query\":{\"rescore_query\":{\"range\":{\""
            + SCORE_FIELD
            + "\":{\"lte\":"
            + (SCORE_BASE - WINDOW_SIZE)
            + "}}},\"query_weight\":1.0,\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}}}";
        Map<String, Object> response = searchForHits(INDEX_THREE_SHARDS, body);

        assertEquals(List.of("5", "1", "2", "3", "4"), hitIds(response));
        assertEquals("the aggregation still sees every document the Tail matched", 30, aggValue(response, "scored"));
    }

    /**
     * The same contract one level down, inside a {@code top_hits} bucket — a <b>second</b> place core applies the
     * request-level rescorers, and one that is easy to assume is out of reach. It is not:
     * {@code TopHitsAggregator} widens each bucket's collector to {@code max(rescore.window_size, from + size)} and then
     * rescores that bucket's {@code TopDocs} with the request's own rescore contexts, trimming back to {@code size}
     * afterwards. That is the identical widening the top-level page suffers, so a bucket asking for 5 hits collects 20
     * candidates here — 15 of them documents no leg ranked, sitting at {@code 0.0} — and a rescore matching them lifts
     * them into the 5 slots the bucket actually shows.
     *
     * <p>Confining the rescore query fixes this site for free, because it is the query that is rewritten, not the page:
     * whatever core hands the rescorer, the rescorer can only match inside the fused window. The bucket still <i>sees</i>
     * every document the Tail matched — that is what an aggregation is for, and documents 6-10 are present below at
     * {@code 0.0} — but the rescore no longer promotes any of them past a document fusion ranked.
     *
     * <p>Measured 2026-08-27 with the confinement disabled: {@code [5, 6, 7, 8, 9]}, a bucket whose five visible hits are
     * one ranked document and four that fusion never ranked.
     */
    @SneakyThrows
    public void testRescoreConfinementAlsoHoldsInsideTopHitsBuckets() {
        ensureDatasets();

        String body = "{\"size\":0,\"aggs\":{\"all\":{\"filter\":{\"match_all\":{}},\"aggs\":{\"top\":{\"top_hits\":{\"size\":"
            + WINDOW_SIZE
            + "}}}}},\"query\":"
            + fusedHybrid(WINDOW_SIZE)
            + ",\"rescore\":{\"window_size\":20,\"query\":{\"rescore_query\":{\"range\":{\""
            + SCORE_FIELD
            + "\":{\"lte\":"
            + (SCORE_BASE - WINDOW_SIZE)
            + "}}},\"query_weight\":1.0,\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}}}";
        List<Map<String, Object>> bucketHits = bucketTopHits(searchForHits(INDEX_ONE_SHARD, body), "all", "top");

        assertEquals(List.of("5", "1", "2", "3", "4"), idsOf(bucketHits));
        // Document 5 is the only document in both the fused window and the rescore query's range, so it is the only one
        // the confined rescore can reach: 0.001 + 10 x 1.0. Document 1 keeps the hybrid's own scale.
        assertEquals(10.001, scoresOf(bucketHits).get(0), 0.01);
        assertEquals(1.0, scoresOf(bucketHits).get(1), 0.01);
    }

    /**
     * A ranked document whose fused score is exactly {@code 0.0}, with no rescore anywhere in the request. The Top scores
     * and the Tail does not, which is the entire mechanism separating the fused window from everything else — and a ranked
     * document at {@code 0.0} ties every Tail-only document instead of outranking it. Lucene breaks that tie by ascending
     * doc id, and with {@code size} equal to the window the ranked document is not merely reordered but dropped: on unfixed
     * code this returns document 6, which fusion never ranked, in place of document 26, which it did.
     *
     * <p>Exactly {@code 0.0} is reachable from ordinary config, and this is one of the two ways: a {@code weights} entry of
     * {@code 0.0} zeroes its leg's contribution, so a document that matched only that leg fuses to {@code 0.0}. (The other
     * is {@code l2} over a leg whose scores are all {@code 0.0} — a zero norm, whose floor is {@code 0.0f} rather than
     * min_max's {@code 0.001f}.) Flooring the fused score to the smallest positive float restores the invariant and reorders
     * nothing, since every other positive score is already at or above that floor.
     *
     * <p>Single-shard on purpose: the eviction is a tie-break inside one collector, so it is only deterministic when one
     * collector sees all 30 documents. Measured 2026-08-27 with only the floor disabled, this returned
     * {@code [1, 2, 3, 4, 5, 6]}.
     */
    @SneakyThrows
    public void testFusedRankedDocumentAtZero_stillOutranksTheTail() {
        ensureDatasets();

        Map<String, Object> response = searchForHits(INDEX_ONE_SHARD, "{\"size\":6,\"query\":" + fusedWithAZeroWeightedLeg(6) + "}");

        // Documents 1-4 come from the weighted leg at [1.0, 0.667, 0.333, 0.001]; document 5 and document 26 both fused to
        // 0.0 and are floored, so they sort last, between them by doc id. Document 6 matched only the Tail.
        assertEquals(List.of("1", "2", "3", "4", "5", "26"), hitIds(response));
        assertEquals("the Tail still matches every document", TOTAL_DOCS, totalHits(response));
        assertTrue("a ranked document is never reported at 0.0", hitScores(response).get(5) > 0.0);
    }

    /**
     * The same grid with the rescore query wrapped in {@code wrapper}, which is the one variation that decides whether the
     * confinement reaches the shards <i>at all</i>.
     *
     * <p>A rescore query that rewrites on the coordinator's first pass — {@code wrapper} here, but equally {@code neural},
     * {@code neural_sparse} or a {@code terms} lookup — makes {@code QueryRescorerBuilder#rewrite} return a new builder,
     * which makes {@code Rewriteable#rewrite(List, ...)} return a new {@code ArrayList}, which
     * {@code SearchSourceBuilder#rewrite} then hands to {@code shallowCopy}. Any confinement written into the request's own
     * rescore list <i>after</i> that pass therefore lands on an orphan and ships nothing. Every other test in this class uses
     * a rescore query that rewrites to itself and so cannot see that: they all pass against an implementation that confines
     * the wrong list.
     *
     * <p>{@code wrapper} is used because it is the cheapest deterministic trigger — no model, no lookup index, no
     * cluster state — and its {@code doRewrite} unconditionally returns the query it parsed, so the identity always changes.
     *
     * <p>Measured 2026-08-27 against an implementation that confines the request's rescore list from the fusion callback
     * instead of installing a placeholder before the fan-out — i.e. the callback-time mutation this class's fix replaces. That
     * run left <b>every other test in this class passing</b> and failed only here and in
     * {@link #testConfinementSurvivesARewritingRescoreQueryInsideATopHitsBucket}:
     *
     * <pre>
     *   1 shard,  rescore_window=5   wrapper(ids[5,6])  [5, 1, 2, 3, 4]  passed — the benign row, as with a static query
     *   1 shard,  rescore_window=20  wrapper(ids[5,6])  [5, 6, 1, 2, 3]  unconfined: document 6 lifted, document 4 evicted
     *   3 shards, rescore_window=5   wrapper(ids[5,6])  [5, 6, 1, 2, 3]  unconfined
     *   3 shards, rescore_window=20  wrapper(ids[5,6])  [5, 6, 1, 2, 3]  unconfined
     * </pre>
     *
     * Those are the same pages the unfixed rows in the class-javadoc table return, which is the point: for a rewriting rescore
     * query the callback-time fix was indistinguishable from no fix, and nothing already in this class could tell.
     */
    @SneakyThrows
    public void testConfinementSurvivesARewritingRescoreQuery() {
        ensureDatasets();
        float boost = 10.0f;

        List<String> mismatches = new ArrayList<>();
        for (String index : List.of(INDEX_ONE_SHARD, INDEX_THREE_SHARDS)) {
            for (Integer rescoreWindow : List.of(WINDOW_SIZE, 20)) {
                String shape = index + " rescore_window=" + rescoreWindow + " wrapper(ids[5,6])";
                Map<String, Object> response = searchForHits(
                    index,
                    rescoredSearch(wrapped("{\"ids\":{\"values\":[\"5\",\"6\"]}}"), rescoreWindow, boost)
                );
                collectMismatches(mismatches, shape, List.of("5", "1", "2", "3", "4"), response);
                collect(
                    mismatches,
                    shape,
                    "the promoted document must still carry the rescore's scale",
                    10.001,
                    hitScores(response).get(0)
                );
            }
        }
        assertEquals("a rewriting rescore query must be confined exactly like a static one", List.of(), mismatches);
    }

    /**
     * Both hazards at once: a rescore query that rewrites, applied at the {@code top_hits} site. The bucket collector is
     * widened by {@code rescore.window_size} the same way the page is, so if the confinement was lost to the rescore-list
     * replacement there is nothing to stop the widening from filling the bucket with documents fusion never ranked.
     *
     * <p>Worth its own test rather than folding {@code wrapper} into
     * {@link #testRescoreConfinementAlsoHoldsInsideTopHitsBuckets}, because the two failure modes are independent: that test
     * covers a second <i>application</i> site with a static query, this one covers a second application site reached through
     * the rewrite path.
     *
     * <p>Measured 2026-08-27 against the callback-time confinement described on
     * {@link #testConfinementSurvivesARewritingRescoreQuery}: {@code [5, 6, 7, 8, 9]} — byte for byte the page that site
     * returns with no confinement at all, while {@link #testRescoreConfinementAlsoHoldsInsideTopHitsBuckets} passed.
     */
    @SneakyThrows
    public void testConfinementSurvivesARewritingRescoreQueryInsideATopHitsBucket() {
        ensureDatasets();

        String body = "{\"size\":0,\"aggs\":{\"all\":{\"filter\":{\"match_all\":{}},\"aggs\":{\"top\":{\"top_hits\":{\"size\":"
            + WINDOW_SIZE
            + "}}}}},\"query\":"
            + fusedHybrid(WINDOW_SIZE)
            + ",\"rescore\":{\"window_size\":20,\"query\":{\"rescore_query\":"
            + wrapped("{\"range\":{\"" + SCORE_FIELD + "\":{\"lte\":" + (SCORE_BASE - WINDOW_SIZE) + "}}}")
            + ",\"query_weight\":1.0,\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}}}";
        List<Map<String, Object>> bucketHits = bucketTopHits(searchForHits(INDEX_ONE_SHARD, body), "all", "top");

        assertEquals(List.of("5", "1", "2", "3", "4"), idsOf(bucketHits));
        assertEquals(10.001, scoresOf(bucketHits).get(0), 0.01);
    }

    /**
     * The floor that keeps a ranked document above the Tail has to stay above it <i>after</i> everything downstream
     * multiplies it, which is what makes the floor's actual value load-bearing rather than arbitrary.
     *
     * <p>{@code query_weight} multiplies the first-pass score of every document in the rescore window — core's
     * {@code QueryRescorer.combine} applies it whether or not the rescore query matched — so it attenuates the floor by an
     * amount the user chooses. A floor of {@code Float.MIN_VALUE} is subnormal: {@code 1.4e-45 * 0.5} is exactly {@code 0.0}
     * in float32, so the smallest attenuation a user can write annihilates it and re-creates the tie the floor exists to
     * break. {@code MIN_RANKED_SCORE} is a normal float chosen to survive factors down to ~{@code 1.5e-15}.
     *
     * <p>Three attenuations, which between them cover both ways a rescore reaches the floor:
     *
     * <ul>
     *   <li>{@code query_weight} at {@code 0.5} and at {@code 0.001}, with the rescore query pointed at document 6 — a document
     *       <i>outside</i> the fused window, so the confinement reduces it to matching nothing and this is pure attenuation.
     *       That isolates the floor from the rescore's own contribution. Document 6 staying off the page is asserted too, since
     *       the same request would show it with no confinement;</li>
     *   <li>{@code score_mode: multiply} with a rescore contribution of {@code 0.5}, pointed at document 26 — the floored
     *       document itself, which <i>is</i> in the window. Multiplying is the other route to a sub-1 factor, and it reaches
     *       documents the rescore query matched rather than the ones it missed.</li>
     * </ul>
     *
     * <p>Measured 2026-08-27 with the floor set back to {@code Float.MIN_VALUE}: {@code [1, 2, 3, 4, 5, 6]} on all three —
     * document 26, which fusion ranked, evicted by document 6, which it did not — and the last ranked hit reported at exactly
     * {@code 0.0} on all three as well. In that same run
     * {@link #testFusedRankedDocumentAtZero_stillOutranksTheTail} <i>passed</i>, which is why this test has to exist
     * separately: an unattenuated subnormal floor does hold, so a test that only checks the floor at {@code query_weight: 1.0}
     * cannot see the value being wrong.
     */
    @SneakyThrows
    public void testTheRankedFloorSurvivesRescoreAttenuation() {
        ensureDatasets();

        Map<String, String> rescorers = new LinkedHashMap<>();
        // query_weight attenuates every document in the window, matched or not — core's QueryRescorer.combine.
        rescorers.put(
            "query_weight=0.5",
            "{\"rescore_query\":{\"ids\":{\"values\":[\"6\"]}},\"query_weight\":0.5,"
                + "\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}"
        );
        rescorers.put(
            "query_weight=0.001",
            "{\"rescore_query\":{\"ids\":{\"values\":[\"6\"]}},\"query_weight\":0.001,"
                + "\"rescore_query_weight\":10.0,\"score_mode\":\"total\"}"
        );
        // multiply reaches the floored document through the rescore's own contribution: ids scores 1.0, weighted to 0.5.
        rescorers.put(
            "score_mode=multiply rescore_query_weight=0.5",
            "{\"rescore_query\":{\"ids\":{\"values\":[\"26\"]}},\"query_weight\":1.0,"
                + "\"rescore_query_weight\":0.5,\"score_mode\":\"multiply\"}"
        );

        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, String> rescorer : rescorers.entrySet()) {
            String body = "{\"size\":6,\"query\":" + fusedWithAZeroWeightedLeg(6) + ",\"rescore\":{\"query\":" + rescorer.getValue() + "}}";
            Map<String, Object> response = searchForHits(INDEX_ONE_SHARD, body);

            collectMismatches(mismatches, rescorer.getKey(), List.of("1", "2", "3", "4", "5", "26"), response);
            if (hitScores(response).get(5) <= 0.0) {
                mismatches.add(rescorer.getKey() + ": the attenuated floor must stay above the Tail, but the last ranked hit scored 0.0");
            }
        }
        assertEquals("the floor must survive every attenuation a request can apply", List.of(), mismatches);
    }

    /**
     * The same attenuation with no rescore in the request at all, so it pins the floor independently of anything in this
     * class's main subject. An enclosing {@code bool} carrying {@code boost: 0.5} multiplies the whole fused query's score on
     * the shard — {@code AbstractQueryBuilder#toQuery} wraps it in a {@code BoostQuery} — which reaches the floor before the
     * page is even collected, not during a second pass.
     *
     * <p>Kept separate because it is the cheaper and more general statement of the same requirement: a floor that only holds
     * at {@code boost: 1.0} is not a floor. A ranked document sitting at the floor is nothing exotic either — a
     * {@code weights} entry of {@code 0.0} produces one, as does {@code l2} over a leg whose scores are all {@code 0.0}.
     *
     * <p>Measured 2026-08-27 with the floor set back to {@code Float.MIN_VALUE}: {@code [1, 2, 3, 4, 5, 6]}.
     */
    @SneakyThrows
    public void testTheRankedFloorSurvivesAnEnclosingBoost() {
        ensureDatasets();

        String body = "{\"size\":6,\"query\":{\"bool\":{\"must\":[" + fusedWithAZeroWeightedLeg(6) + "],\"boost\":0.5}}}";
        Map<String, Object> response = searchForHits(INDEX_ONE_SHARD, body);

        assertEquals(List.of("1", "2", "3", "4", "5", "26"), hitIds(response));
        assertTrue("a ranked document is never reported at 0.0, whatever the enclosing boost", hitScores(response).get(5) > 0.0);
        assertEquals("the Tail still matches every document", TOTAL_DOCS, totalHits(response));
    }

    /**
     * {@code query_weight: 0} and a negative {@code query_weight} — the two shapes that multiply the ranked/Tail
     * separation away — now return only documents fusion ranked, because the rescore pool is limited to them before the
     * rescore runs.
     *
     * <p>History, because this test has asserted three different things and the reason matters. Limiting the rescore
     * <i>query</i> makes a Tail-only document unmatchable, which was originally taken to mean a rescore could not return
     * one. It does not: core's {@code QueryRescorer} multiplies the first-pass score of every document in the rescore
     * window by {@code query_weight} whether the rescore query matched it or not, then breaks score ties by ascending
     * Lucene doc id — so any weighting that lands a ranked document on exactly {@code 0.0f} makes it indistinguishable
     * from a Tail-only one. MEASURED, on a 300-document index at 20 shards with no window named anywhere in the request:
     * {@code query_weight: 0} returned two documents from outside a default 100-document window, and
     * {@code query_weight: -1} returned a page of which none were ranked.
     *
     * <p>Fixed by demoting, not by refusing: {@link org.opensearch.neuralsearch.query.FusedWindowGuardRescorer} records
     * which documents fusion ranked from the pool as it arrives — {@code score <= 0.0f} means Tail-only — then applies the
     * request's own rescorers and afterwards separates the two groups into disjoint score bands, so an unranked document
     * cannot be ordered above a ranked one however the request's arithmetic scored it. The weights stay legal — they are
     * core's own parameters and they wreck a plain {@code function_score} identically — and {@code total_hits} and
     * aggregations are untouched because nothing ever leaves the pool, which the totals assertion below pins.
     *
     * <p>It demotes rather than prunes because core's {@code RescoreProcessor} reads {@code scoreDocs[0].score} on the
     * rescorer's output with no length check, so returning a shorter array is an {@code ArrayIndexOutOfBoundsException}
     * shard failure. The demoted band's sentinel is mapped back to {@code 0.0} on the coordinator by
     * {@code FusedRescoreScoreNormalizer} — the score those documents carry without a rescore — so the scores asserted
     * below are the ones a user sees, not the internal band.
     *
     * <p><b>Both weight parameters are covered, and neither is validated by core.</b> {@code query_weight} and
     * {@code rescore_query_weight} are each parsed with a bare {@code declareFloat} and set by bare assignment — the only
     * validations anywhere in core's rescore package are a null {@code rescore_query}, an unknown field, and an illegal
     * {@code score_mode} — so zero and negative values for both are legal input that has to be answered, not rejected.
     * They reach the ranked documents differently, which is why the rows differ: {@code query_weight} multiplies the
     * first-pass score of every document in the rescore window whether the rescore query matched it or not, while a zero
     * or negative {@code rescore_query_weight} only reaches the documents it did match. MEASURED across all five score
     * modes on a 300-document, 20-shard index: with the guard in place none of the 20 combinations returned a document
     * outside the fused window.
     *
     * <p>The fixture matters. {@code fusedHybrid}'s window is documents 1-5, the five lowest doc ids in the index, so it
     * wins every {@code 0.0} tie and cannot show the effect at any shard count — which is also why
     * {@link #testDefaultRescoreWindowAtHighShardCount_admitsNothingUnranked} passed while the defect was live.
     * {@code fusedWithAZeroWeightedLeg}'s window is {@code [1, 2, 3, 4, 5, 26]}: document 26 is ranked with a HIGH doc
     * id and documents 6-25 are Tail-only with lower ones, so any assertion about membership has to use that one.
     */
    @SneakyThrows
    public void testFlatteningWeightsCannotAdmitAnUnrankedDocument() {
        ensureDatasets();

        // The top of the page is still exactly right: only a ranked document can be lifted.
        Map<String, Object> lifted = searchForHits(
            INDEX_ONE_SHARD,
            rescoredSearch("{\"ids\":{\"values\":[\"5\",\"6\"]}}", 20, 10.0f).replace("\"query_weight\":1.0", "\"query_weight\":0.0")
        );
        assertEquals("document 6 cannot be lifted", List.of("5", "1", "2", "3", "4"), hitIds(lifted));
        assertEquals("the only document both in the window and matched by the rescore query", 10.0, hitScores(lifted).get(0), 0.01);

        // Membership now holds over a window whose ranked documents are NOT doc-id-minimal. Each row below is chosen so
        // that it individually flattens or inverts document 26 — the one ranked document with a HIGH doc id — because a
        // row that only flattens document 1 is not discriminating: document 1 holds the lowest doc id in the index and
        // wins every 0.0 tie whether the guard demoted anything or not. Both the `query_weight` and the `rescore_query_weight`
        // sides are covered; the expected page for every row is the window and nothing else.
        List<String> window = List.of("1", "2", "3", "4", "5", "26");
        Map<String, String> flattening = new LinkedHashMap<>();
        // query_weight side: zero and negative multiply away the first-pass score of EVERY document in the window,
        // matched or not, so they need no particular rescore target.
        flattening.put(
            "query_weight=0",
            "{\"ids\":{\"values\":[\"1\"]}}|\"query_weight\":0.0,\"rescore_query_weight\":10.0,\"score_mode\":\"total\""
        );
        flattening.put(
            "query_weight=-1",
            "{\"ids\":{\"values\":[\"999\"]}}|\"query_weight\":-1.0,\"rescore_query_weight\":10.0,\"score_mode\":\"total\""
        );
        // rescore_query_weight side: a zero or negative secondary only reaches the documents the rescore query MATCHED,
        // so each of these has to point at document 26 to be discriminating.
        flattening.put(
            "multiply, rescore_query_weight=0",
            "{\"ids\":{\"values\":[\"26\"]}}|\"query_weight\":1.0,\"rescore_query_weight\":0.0,\"score_mode\":\"multiply\""
        );
        flattening.put(
            "min, rescore_query_weight=0",
            "{\"ids\":{\"values\":[\"26\"]}}|\"query_weight\":1.0,\"rescore_query_weight\":0.0,\"score_mode\":\"min\""
        );
        flattening.put(
            "total, both weights negative",
            "{\"ids\":{\"values\":[\"26\"]}}|\"query_weight\":-1.0,\"rescore_query_weight\":-5.0,\"score_mode\":\"total\""
        );
        flattening.put(
            "multiply, both weights negative",
            "{\"ids\":{\"values\":[\"26\"]}}|\"query_weight\":-1.0,\"rescore_query_weight\":-5.0,\"score_mode\":\"multiply\""
        );

        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, String> shape : flattening.entrySet()) {
            String[] parts = shape.getValue().split("\\|", 2);
            Map<String, Object> response = searchForHits(
                INDEX_ONE_SHARD,
                "{\"size\":6,\"query\":"
                    + fusedWithAZeroWeightedLeg(6)
                    + ",\"rescore\":{\"window_size\":20,\"query\":{\"rescore_query\":"
                    + parts[0]
                    + ","
                    + parts[1]
                    + "}}}"
            );
            List<String> ids = hitIds(response);
            for (String id : ids) {
                if (window.contains(id) == false) {
                    mismatches.add(shape.getKey() + ": returned unranked document " + id + " (page " + ids + ")");
                }
            }
            if (ids.contains("26") == false) {
                mismatches.add(shape.getKey() + ": ranked document 26 lost its slot (page " + ids + ")");
            }
            if (totalHits(response) != TOTAL_DOCS) {
                mismatches.add(shape.getKey() + ": total_hits must still count the whole leg union, was " + totalHits(response));
            }
        }
        assertEquals("no weighting may admit a document fusion did not rank", List.of(), mismatches);
    }

    // ------------------------------------------------ bodies ------------------------------------------------

    /**
     * The full grid: two shard counts x two rescore windows x two rescore-query shapes, all expected to produce one page.
     *
     * <p>The two windows are the two sides of multiplier 1 — {@code WINDOW_SIZE} is the benign case on one shard (the
     * rescore reaches exactly the ranked documents) and {@code 20} is the case that reaches deep into the Tail. The two
     * query shapes matter for the same reason: {@code ids} is the specification's own {@code [5, 6]}, while the range
     * matches every document from 5 up, so on <i>any</i> shard layout several unranked candidates are within reach and
     * multiplier 2 fires without depending on how {@code _id}s happened to route.
     */
    private Map<String, String> rescoreShapes(float boost) {
        Map<String, String> bodies = new LinkedHashMap<>();
        Map<String, String> rescoreQueries = new LinkedHashMap<>();
        rescoreQueries.put("ids[5,6]", "{\"ids\":{\"values\":[\"5\",\"6\"]}}");
        rescoreQueries.put("range[id>=5]", "{\"range\":{\"" + SCORE_FIELD + "\":{\"lte\":" + (SCORE_BASE - WINDOW_SIZE) + "}}}");

        for (String index : List.of(INDEX_ONE_SHARD, INDEX_THREE_SHARDS)) {
            for (int rescoreWindow : List.of(WINDOW_SIZE, 20)) {
                for (Map.Entry<String, String> rescoreQuery : rescoreQueries.entrySet()) {
                    bodies.put(
                        index + " rescore_window=" + rescoreWindow + " " + rescoreQuery.getKey(),
                        index + "|" + rescoredSearch(rescoreQuery.getValue(), rescoreWindow, boost)
                    );
                }
            }
        }
        return bodies;
    }

    /**
     * {@code size} is the fused window: an unranked document can only appear here by evicting a ranked one.
     *
     * @param rescoreWindow {@code null} leaves {@code window_size} out of the request entirely, which is core's default and
     *                      the shape a user writes first
     */
    private String rescoredSearch(String rescoreQuery, Integer rescoreWindow, float boost) {
        return "{\"size\":"
            + WINDOW_SIZE
            + ",\"query\":"
            + fusedHybrid(WINDOW_SIZE)
            + ",\"rescore\":{"
            + (rescoreWindow == null ? "" : "\"window_size\":" + rescoreWindow + ",")
            + "\"query\":{\"rescore_query\":"
            + rescoreQuery
            + ",\"query_weight\":1.0,\"rescore_query_weight\":"
            + boost
            + ",\"score_mode\":\"total\"}}}";
    }

    /**
     * Two identical legs under an inline {@code fusion} config, so the fused score of each document is its own min_max
     * normalized score and the window is exactly {@code [1.0, 0.75, 0.5, 0.25, 0.001]} for documents 1-5 — the same
     * numbers on any shard count, because fused mode normalizes on the coordinator over the merged leg results.
     */
    private String fusedHybrid(int windowSize) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.5,0.5]}}},"
            + "\"queries\":["
            + scoreByField("{\"match_all\":{}}")
            + ","
            + scoreByField("{\"match_all\":{}}")
            + "]}}";
    }

    /**
     * Three legs whose weights are {@code [1.0, 0.0, 0.0]}, built so that a document fuses to exactly {@code 0.0} and still
     * makes the window:
     *
     * <ul>
     *   <li>the weighted leg matches only documents 1-4, so it cannot fill a window of 6 on its own — which is what leaves
     *       room for a zero-scored document, since a zero always sorts last and would otherwise be the one truncated;</li>
     *   <li>{@code ids[26]} contributes document 26 at weight {@code 0.0}, so it fuses to {@code 0.0}. Its {@code _id} sorts
     *       ahead of 5 and 6 on the fused window's tie-break (a string comparison: {@code "26" < "5"}) and its doc id is
     *       higher than theirs, which is precisely the combination that survives the window and then loses the shard-side
     *       tie to a Tail-only document;</li>
     *   <li>the third leg matches everything, at weight {@code 0.0}, purely so the Tail covers documents 6-25 — the
     *       lower-doc-id, never-ranked documents that win the {@code 0.0} tie.</li>
     * </ul>
     */
    private String fusedWithAZeroWeightedLeg(int windowSize) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[1.0,0.0,0.0]}}},"
            + "\"queries\":["
            + scoreByField("{\"range\":{\"" + SCORE_FIELD + "\":{\"gte\":" + (SCORE_BASE - 4) + "}}}")
            + ",{\"ids\":{\"values\":[\"26\"]}},"
            + scoreByField("{\"match_all\":{}}")
            + "]}}";
    }

    /**
     * The same query behind a {@code wrapper}, whose {@code doRewrite} parses the base64 payload and returns what it parsed —
     * so the builder's identity always changes on the coordinator's first rewrite pass. That is the event that replaces the
     * request's rescore list, and the only reason this indirection is here.
     */
    private String wrapped(String query) {
        return "{\"wrapper\":{\"query\":\"" + Base64.getEncoder().encodeToString(query.getBytes(StandardCharsets.UTF_8)) + "\"}}";
    }

    /** A leg that scores by the numeric field, so its ranking is exact and independent of shard count and term stats. */
    private String scoreByField(String query) {
        return "{\"function_score\":{\"query\":"
            + query
            + ",\"field_value_factor\":{\"field\":\""
            + SCORE_FIELD
            + "\",\"modifier\":\"none\",\"missing\":1}}}";
    }

    // ------------------------------------------------ harness ------------------------------------------------

    /** Splits a grid entry's {@code index|body} back apart and runs it. */
    private Map<String, Object> searchForHits(String indexAndBody) {
        String[] parts = indexAndBody.split("\\|", 2);
        return searchForHits(parts[0], parts[1]);
    }

    @SneakyThrows
    private Map<String, Object> searchForHits(String index, String jsonBody) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(jsonBody);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hitList(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList == null ? List.of() : hitList;
    }

    /** The hits of a {@code top_hits} sub-aggregation, whose value has the same {@code hits.hits} shape as a response. */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> bucketTopHits(Map<String, Object> response, String bucketAgg, String topHitsAgg) {
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> bucket = (Map<String, Object>) aggregations.get(bucketAgg);
        return hitList((Map<String, Object>) bucket.get(topHitsAgg));
    }

    private List<String> hitIds(Map<String, Object> response) {
        return idsOf(hitList(response));
    }

    private List<String> idsOf(List<Map<String, Object>> hits) {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> hit : hits) {
            out.add((String) hit.get("_id"));
        }
        return out;
    }

    private List<Double> hitScores(Map<String, Object> response) {
        return scoresOf(hitList(response));
    }

    private List<Double> scoresOf(List<Map<String, Object>> hits) {
        List<Double> out = new ArrayList<>();
        for (Map<String, Object> hit : hits) {
            out.add(((Number) hit.get("_score")).doubleValue());
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private int aggValue(Map<String, Object> response, String name) {
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        return ((Number) ((Map<String, Object>) aggregations.get(name)).get("value")).intValue();
    }

    @SuppressWarnings("unchecked")
    private int totalHits(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return ((Number) ((Map<String, Object>) hits.get("total")).get("value")).intValue();
    }

    private String indexConfig(int shards) {
        return "{\"settings\":{\"number_of_shards\":"
            + shards
            + ",\"number_of_replicas\":0},\"mappings\":{\"properties\":{\""
            + SCORE_FIELD
            + "\":{\"type\":\"integer\"}}}}";
    }

    @SneakyThrows
    private void ensureDatasets() {
        // No index.search.default_pipeline: every query here carries its fusion config inline, which is what enables the
        // resolver, so a normalization pipeline would have nothing to contribute.
        ensureDataset(INDEX_ONE_SHARD, 1);
        ensureDataset(INDEX_THREE_SHARDS, 3);
        ensureDataset(INDEX_MANY_SHARDS, 20);
    }

    @SneakyThrows
    private void ensureDataset(String index, int shards) {
        if (indexExists(index)) {
            return;
        }
        createIndex(index, indexConfig(shards));
        for (int id = 1; id <= TOTAL_DOCS; id++) {
            Request request = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity("{\"" + SCORE_FIELD + "\":" + (SCORE_BASE - id) + "}");
            int code = client().performRequest(request).getStatusLine().getStatusCode();
            assertTrue(
                "indexing doc " + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }
}
