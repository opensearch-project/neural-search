/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.Rescorer;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link FusedWindowGuardRescorer} — the guard that keeps a rescore from changing which documents a fused hybrid may
 * return.
 *
 * <p>The invariant every test here is about: after the guard runs, every document that arrived at exactly {@code 0.0f}
 * (Tail-only, i.e. matched by a leg but not ranked by the fusion) sorts below every document that arrived above it,
 * whatever the delegates did to the scores in between. The array length and the {@code TotalHits} must come back
 * unchanged, because core's {@code RescoreProcessor} reads {@code scoreDocs[0].score} after the loop without a length
 * check and the coordinator sums {@code totalHits} by value.
 */
public class FusedWindowGuardRescorerTests extends OpenSearchTestCase {

    private static final float UNRANKED = FusedWindowGuardRescorerBuilder.UNRANKED_SCORE;
    private static final float RANKED_FLOOR = FusedWindowGuardRescorerBuilder.RANKED_FLOOR;
    private static final TotalHits TOTAL = new TotalHits(500, TotalHits.Relation.EQUAL_TO);

    /** The defect, in one test: a delegate that flattens every score must not let a Tail-only document take a slot. */
    public void testRescore_whenADelegateFlattensEveryScore_thenUnrankedDocumentsSortLast() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(9, 0.7f), new ScoreDoc(1, 0.0f), new ScoreDoc(4, 0.2f), new ScoreDoc(2, 0.0f));

        TopDocs result = guard(flattenTo(0.0f)).rescore(pool, null, context());

        // Ranked 9 and 4 keep the band; Tail-only 1 and 2 are pushed under it, and within a band it is doc id order.
        assertEquals(List.of(4, 9, 1, 2), docsOf(result));
        // The delegate put the ranked documents at 0.0, which is already above the sentinel, so the clamp leaves them
        // alone — it only lifts a ranked score that landed AT or BELOW the sentinel. The band separates them regardless.
        assertEquals(0.0f, result.scoreDocs[0].score, 0.0f);
        assertEquals(UNRANKED, result.scoreDocs[2].score, 0.0f);
        assertTrue("every ranked document outranks every unranked one", result.scoreDocs[1].score > result.scoreDocs[2].score);
        assertEquals("the pool length is the contract with RescoreProcessor", 4, result.scoreDocs.length);
        assertSame("total_hits must be carried through untouched", TOTAL, result.totalHits);
    }

    /**
     * A negative {@code query_weight} is legal and drives ranked documents below zero. The band still has to hold, which
     * is why the guard clamps rather than trusting a fixed sentinel to be lower than everything.
     */
    public void testRescore_whenADelegateDrivesRankedScoresNegative_thenTheBandStillHolds() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(7, 1.0f), new ScoreDoc(3, 0.0f), new ScoreDoc(8, 0.5f));

        TopDocs result = guard(multiplyBy(-1.0f)).rescore(pool, null, context());

        assertEquals("both ranked documents stay above the Tail-only one", List.of(8, 7, 3), docsOf(result));
        assertTrue("a ranked document is never at or below the sentinel", result.scoreDocs[0].score > UNRANKED);
        assertTrue(result.scoreDocs[1].score > UNRANKED);
        assertEquals(UNRANKED, result.scoreDocs[2].score, 0.0f);
    }

    /**
     * The saturation case the clamp exists for: a large negative {@code rescore_query_weight} can take a ranked document
     * to {@code -Infinity}, and nothing is representable strictly below that — so the guard lifts it into the ranked band
     * instead of trying to demote the Tail below it.
     */
    public void testRescore_whenARankedScoreSaturates_thenItIsClampedIntoTheRankedBand() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(5, 0.9f), new ScoreDoc(6, 0.0f));

        TopDocs result = guard(flattenTo(Float.NEGATIVE_INFINITY)).rescore(pool, null, context());

        assertEquals(List.of(5, 6), docsOf(result));
        assertEquals("a saturated ranked score is clamped up, not left at -Infinity", RANKED_FLOOR, result.scoreDocs[0].score, 0.0f);
        assertEquals(UNRANKED, result.scoreDocs[1].score, 0.0f);
    }

    /**
     * NaN is treated as saturation rather than propagated. It is the LARGEST value under {@code Float.compare}, so
     * letting it through would float the document to the top of the page and would also trip core's own sortedness
     * assertion.
     */
    public void testRescore_whenADelegateProducesNaN_thenItIsClampedRatherThanPropagated() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(2, 0.4f), new ScoreDoc(3, 0.0f));

        TopDocs result = guard(flattenTo(Float.NaN)).rescore(pool, null, context());

        assertFalse("NaN must not reach the page", Float.isNaN(result.scoreDocs[0].score));
        assertEquals(RANKED_FLOOR, result.scoreDocs[0].score, 0.0f);
        assertEquals(List.of(2, 3), docsOf(result));
    }

    /**
     * The chain-inversion case, and the reason ONE guard wraps the whole chain rather than one guard per rescorer.
     * Membership is read once from the pristine pool; after the first delegate has flattened everything to {@code 0.0f} a
     * second reading would classify the genuinely-ranked documents as Tail and demote them below the real Tail.
     */
    public void testRescore_whenTheChainFlattensThenReorders_thenMembershipIsStillTheOriginalOne() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(1, 0.8f), new ScoreDoc(5, 0.0f), new ScoreDoc(2, 0.3f));

        TopDocs result = guard(flattenTo(0.0f), addTo(2.0f)).rescore(pool, null, context());

        assertEquals("document 5 was never ranked and cannot come first however the chain scored it", 5, docsOf(result).get(2).intValue());
        assertEquals(List.of(1, 2, 5), docsOf(result));
    }

    /** Delegates are applied in the order the user declared them. */
    public void testRescore_appliesDelegatesInOrder() throws IOException {
        List<String> calls = new ArrayList<>();
        TopDocs pool = poolOf(new ScoreDoc(1, 1.0f));

        guard(recording(calls, "first"), recording(calls, "second")).rescore(pool, null, context());

        assertEquals(List.of("first", "second"), calls);
    }

    /** With nothing unranked in the pool there is nothing to separate, so the delegates' own result is returned as is. */
    public void testRescore_whenEveryDocumentWasRanked_thenTheDelegateResultIsUntouched() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.4f));

        TopDocs result = guard(flattenTo(0.25f)).rescore(pool, null, context());

        assertEquals(0.25f, result.scoreDocs[0].score, 0.0f);
        assertEquals("no document is demoted when none was unranked", 0.25f, result.scoreDocs[1].score, 0.0f);
    }

    /** A pool that is entirely Tail-only still comes back at full length — the case that made pruning impossible. */
    public void testRescore_whenNothingWasRanked_thenTheLengthIsStillPreserved() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(4, 0.0f), new ScoreDoc(1, 0.0f));

        TopDocs result = guard(flattenTo(3.0f)).rescore(pool, null, context());

        assertEquals("an empty return would be an AIOOBE in RescoreProcessor", 2, result.scoreDocs.length);
        assertEquals(UNRANKED, result.scoreDocs[0].score, 0.0f);
        assertEquals(List.of(1, 4), docsOf(result));
        assertSame(TOTAL, result.totalHits);
    }

    /** The explanation describes the user's rescore, chained in the order core would have applied it. */
    public void testExplain_chainsTheDelegatesInOrder() throws IOException {
        FusedWindowGuardRescorer rescorer = guard(recording(new ArrayList<>(), "a"), recording(new ArrayList<>(), "b"));

        Explanation explanation = rescorer.explain(1, null, context(), Explanation.match(1.0f, "first pass"));

        assertEquals("b", explanation.getDescription());
        assertEquals("a", explanation.getDetails()[0].getDescription());
    }

    // ---- helpers ----

    private FusedWindowGuardRescorer guard(final Rescorer... delegates) {
        List<RescoreContext> contexts = new ArrayList<>();
        for (Rescorer delegate : delegates) {
            contexts.add(new RescoreContext(10, delegate));
        }
        return new FusedWindowGuardRescorer(contexts);
    }

    private RescoreContext context() {
        return new RescoreContext(10, flattenTo(0.0f));
    }

    private TopDocs poolOf(final ScoreDoc... docs) {
        return new TopDocs(TOTAL, docs);
    }

    private List<Integer> docsOf(final TopDocs topDocs) {
        List<Integer> docs = new ArrayList<>();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            docs.add(scoreDoc.doc);
        }
        return docs;
    }

    /** Mutates in place and returns the same instance, exactly as core's own QueryRescorer does. */
    private Rescorer flattenTo(final float score) {
        return new TestRescorer(topDocs -> {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                scoreDoc.score = score;
            }
            return topDocs;
        }, null);
    }

    private Rescorer multiplyBy(final float factor) {
        return new TestRescorer(topDocs -> {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                scoreDoc.score *= factor;
            }
            return topDocs;
        }, null);
    }

    private Rescorer addTo(final float delta) {
        return new TestRescorer(topDocs -> {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                scoreDoc.score += delta;
            }
            return topDocs;
        }, null);
    }

    private Rescorer recording(final List<String> calls, final String name) {
        return new TestRescorer(topDocs -> {
            calls.add(name);
            return topDocs;
        }, name);
    }

    private interface Rescore {
        TopDocs apply(TopDocs topDocs);
    }

    private static class TestRescorer implements Rescorer {
        private final Rescore rescore;
        private final String name;

        TestRescorer(final Rescore rescore, final String name) {
            this.rescore = rescore;
            this.name = name;
        }

        @Override
        public TopDocs rescore(final TopDocs topDocs, final IndexSearcher searcher, final RescoreContext rescoreContext) {
            return rescore.apply(topDocs);
        }

        @Override
        public Explanation explain(
            final int topLevelDocId,
            final IndexSearcher searcher,
            final RescoreContext rescoreContext,
            final Explanation sourceExplanation
        ) {
            return Explanation.match(sourceExplanation.getValue(), name, sourceExplanation);
        }
    }
}
