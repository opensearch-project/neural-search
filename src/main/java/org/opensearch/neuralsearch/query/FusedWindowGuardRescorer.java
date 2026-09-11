/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.Rescorer;

/**
 * Runs the request's own rescorers and then restores the one distinction their arithmetic can destroy: a document the
 * fusion ranked always outranks a document it did not.
 *
 * <p><b>What it is defending.</b> Fused mode's round 2 is a self-erased
 * {@code bool{should: constant_score(window ids)^fusedScore, filter: <the legs>}} with a null
 * {@code minimum_should_match}, so the non-scoring Tail filter decides matching and the Top only adds score: every
 * document any leg matched matches round 2, at exactly {@code 0.0f}, while only the window was ranked. Ranked documents
 * are held above them by {@link HybridFusionOrchestrator#MIN_RANKED_SCORE} alone. Core's {@code QueryRescorer} then
 * multiplies the first-pass score of every document in the rescore window by {@code query_weight} whether the rescore
 * query matched it or not, does the same beyond that window, and re-sorts with score ties broken by ascending Lucene
 * doc id — so {@code query_weight <= 0}, or {@code score_mode} {@code multiply}/{@code min} with a zero weighted rescore
 * term, collapses ranked and unranked onto the same number and lets doc id decide the page.
 *
 * <p><b>Why demote and not remove.</b> Removing the unranked documents here is not expressible: a shard holding Tail
 * matches but none of the coordinator-global window — routine once the shard count exceeds
 * {@code fusion.window_size / rescore window} — would have to return an empty array, and core's
 * {@code RescoreProcessor} length-guards only its INPUT before reading {@code scoreDocs[0].score} on the loop's output,
 * so an empty return is an {@code ArrayIndexOutOfBoundsException} escaping a {@code catch (IOException)} — a shard
 * failure. Demoting preserves the array length, so that path is never taken, and it also never shortens the page.
 *
 * <p><b>The guarantee this delivers.</b> A rescore may reorder the hybrid's hits; it may not change which documents are
 * eligible to be returned. That is exactly the ordering the same request has with no rescore at all — ranked documents
 * at or above the floor, Tail-only documents at {@code 0.0f} — which is also the contract
 * {@link HybridFusionQueryBuilder#buildSelfErasedQuery} states for {@code size <= window_size}.
 *
 * <p><b>Why one guard wraps the whole chain rather than one guard per rescorer.</b> Membership has to be read from the
 * PRISTINE pool, before any of the user's rescorers has touched a score. Wrapping each element separately would read it
 * again between elements, and by then a {@code query_weight: 0} element has set every genuinely-ranked document back to
 * exactly {@code 0.0f} — so the second wrapper would classify the ranked documents as unranked and demote them below
 * the real Tail, inverting the page. Recording membership once, ahead of the whole chain, makes that unreachable
 * without needing to carry the window itself to the shard.
 *
 * <p><b>The two-band clamp, and why a single fixed sentinel is not enough on its own.</b> Both rescore weights are
 * unvalidated floats, so a ranked document's post-rescore score is not bounded below: a large negative
 * {@code rescore_query_weight} can saturate it to {@code -Infinity}, and nothing sorts strictly below that. So the
 * demotion is a BAND, not a value: unranked documents are set to exactly {@code -Float.MAX_VALUE}, and ranked documents
 * are clamped UP to {@code Math.nextUp(-Float.MAX_VALUE)} if they landed at or below it (or became NaN). Nothing is
 * representable between those two floats, so the bands cannot meet; the clamp only touches ranked documents that had
 * already saturated, and it is the same two constants on every shard, so the coordinator's merge — which compares raw
 * floats and breaks ties by (shard, doc id) — sees one consistent band boundary rather than a per-shard one.
 *
 * <p>Both band values are an internal encoding, not scores the user asked for; {@code FusedRescoreScoreNormalizer}
 * decodes them on the coordinator — the unranked sentinel to {@code 0.0f} and the ranked floor to
 * {@link FusedWindowGuardRescorerBuilder#RANKED_FLOOR_NORMALIZED} — so the response reports what the same request
 * without a rescore would return.
 */
class FusedWindowGuardRescorer implements Rescorer {

    /** @see FusedWindowGuardRescorerBuilder#UNRANKED_SCORE */
    static final float UNRANKED = FusedWindowGuardRescorerBuilder.UNRANKED_SCORE;
    /** @see FusedWindowGuardRescorerBuilder#RANKED_FLOOR */
    static final float RANKED_FLOOR = FusedWindowGuardRescorerBuilder.RANKED_FLOOR;

    /**
     * Descending by score, ties by ascending doc id — the same order core's own {@code RescoreProcessor} asserts and
     * the coordinator's merge assumes.
     */
    private static final Comparator<ScoreDoc> BY_SCORE_THEN_DOC = Comparator.<ScoreDoc>comparingDouble(d -> -d.score)
        .thenComparingInt(d -> d.doc);

    private final List<RescoreContext> delegates;

    FusedWindowGuardRescorer(final List<RescoreContext> delegates) {
        this.delegates = delegates;
    }

    @Override
    public TopDocs rescore(final TopDocs topDocs, final IndexSearcher searcher, final RescoreContext rescoreContext) throws IOException {
        // Read membership from the pool as the collector produced it: a ranked document carries its fused score, floored
        // to MIN_RANKED_SCORE, and a Tail-only document carries exactly 0.0f. The test is exact rather than heuristic —
        // the hybrid query refuses any boost other than 1.0 and indices_boost is rejected for fused mode, so nothing
        // attenuates the floor before this point.
        //
        // One site hands over a pool round 2 did not score: a `global` aggregation's top_hits, which core collects in a
        // separate match_all pass, so every document arrives at 1.0f. The inference is vacuous there rather than wrong —
        // nothing reads as unranked, the separation below is skipped, and the bucket gets exactly what it would without
        // the guard, which is what `global` means (it is defined to ignore the query, so its documents are not the
        // hybrid's hits and there is no fused ranking to preserve).
        Set<Integer> unranked = new HashSet<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (scoreDoc.score <= 0.0f) {
                unranked.add(scoreDoc.doc);
            }
        }

        TopDocs rescored = topDocs;
        for (RescoreContext delegate : delegates) {
            rescored = delegate.rescorer().rescore(rescored, searcher, delegate);
        }

        if (unranked.isEmpty() && needsTheRankedFloor(rescored) == false) {
            // Nothing to separate: every document in the pool was ranked, so the user's rescore stands as it is.
            return rescored;
        }
        return separate(rescored, unranked);
    }

    /**
     * Whether any score still has to be clamped even though there is no band to separate. A pool of nothing but ranked
     * documents still reaches the coordinator through this method, and a delegate can have driven one of those scores to
     * NaN or past {@link #UNRANKED} — NaN would render as a null {@code _score} and sort to the top of the page, and a
     * saturated score would tie the sentinel a later shard is about to report. Checking is a scan of an array the
     * delegates just rewrote; the alternative is rebuilding and re-sorting it for nothing on the common path.
     */
    private static boolean needsTheRankedFloor(final TopDocs rescored) {
        for (ScoreDoc scoreDoc : rescored.scoreDocs) {
            if (Float.isNaN(scoreDoc.score) || scoreDoc.score <= UNRANKED) {
                return true;
            }
        }
        return false;
    }

    /**
     * Push every unranked document into the bottom band and lift any ranked document that saturated into it, then
     * re-sort. The array is rebuilt rather than sorted in place because the delegates may have returned the caller's own
     * array, and a caller that kept a reference to it should not see it reordered underneath them.
     */
    private TopDocs separate(final TopDocs rescored, final Set<Integer> unranked) {
        ScoreDoc[] separated = new ScoreDoc[rescored.scoreDocs.length];
        for (int i = 0; i < rescored.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = rescored.scoreDocs[i];
            float score = unranked.contains(scoreDoc.doc) ? UNRANKED : rankedScore(scoreDoc.score);
            separated[i] = new ScoreDoc(scoreDoc.doc, score, scoreDoc.shardIndex);
        }
        Arrays.sort(separated, BY_SCORE_THEN_DOC);
        return new TopDocs(rescored.totalHits, separated);
    }

    /**
     * A ranked document's score, clamped up into the ranked band. NaN is treated as saturated rather than propagated:
     * NaN is the largest value under {@code Float.compare}, so letting it through would float the document to the top of
     * the page and would also trip core's own sortedness assertion.
     */
    private static float rankedScore(final float score) {
        if (Float.isNaN(score) || score <= UNRANKED) {
            return RANKED_FLOOR;
        }
        return score;
    }

    /**
     * Chain the delegates' explanations in the order core would have applied them, so {@code explain: true} describes
     * the rescore the user asked for rather than this wrapper.
     *
     * <p>The value it reports is the delegates' own combined value, which for a demoted document is its score BEFORE
     * the band was applied. That is deliberate: the explanation explains the user's rescore, and the band is an internal
     * encoding the coordinator removes again. {@code FusedExplanationMerger} replaces the tree for every ranked hit
     * anyway.
     */
    @Override
    public Explanation explain(
        final int topLevelDocId,
        final IndexSearcher searcher,
        final RescoreContext rescoreContext,
        final Explanation sourceExplanation
    ) throws IOException {
        Explanation explanation = sourceExplanation;
        for (RescoreContext delegate : delegates) {
            explanation = delegate.rescorer().explain(topLevelDocId, searcher, delegate, explanation);
        }
        return explanation;
    }

    /** The delegate contexts, so the builder can expose the chain it wrapped for testing. */
    List<RescoreContext> delegates() {
        return new ArrayList<>(delegates);
    }
}
