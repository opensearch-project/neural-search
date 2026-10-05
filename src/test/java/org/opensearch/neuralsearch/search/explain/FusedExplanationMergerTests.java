/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for the shapes the fused {@code explain} path has to survive without a cluster: nothing collected, a hit
 * fusion never ranked, a score a post-fusion step moved, and a leg that returned no explanation of its own. What the tree
 * looks like for a real query is pinned end-to-end in {@code HybridQueryFusedModeExplainIT}.
 */
public class FusedExplanationMergerTests extends OpenSearchTestCase {

    private static final String INDEX = "test-index";
    private static final String COMBINATION = "arithmetic_mean combination of:";
    private static final String NORMALIZATION = "min_max normalization of:";
    private static final String FINAL_SCORE = "score of the fused hybrid query as round 2 returned it, computed from:";

    public void testGetMergedResponse_whenNothingCollected_thenResponseReturnedUntouched() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        SearchResponse response = responseWithHits(hit("1", 0.5f));

        assertTrue("nothing was collected", merger.isEmpty());
        assertSame("an unexplained response must not be rebuilt", response, merger.getMergedResponse(response));
        assertNull("and no hit may gain an explanation", response.getHits().getHits()[0].getExplanation());
    }

    public void testGetMergedResponse_whenEmptyCollectionPublished_thenNothingIsAttached() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(new FusedDocExplanations());
        SearchResponse response = responseWithHits(hit("1", 0.5f));

        assertTrue("an explained request whose legs ranked nothing publishes an empty collection", merger.isEmpty());
        assertNull(merger.getMergedResponse(response).getHits().getHits()[0].getExplanation());
    }

    public void testGetMergedResponse_whenDocumentWasRanked_thenTheFusedTreeReplacesRoundTwos() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit ranked = hit("1", 0.6f);
        ranked.explanation(Explanation.match(0.6f, "ConstantScore(_id:[1])"));

        SearchResponse merged = merger.getMergedResponse(responseWithHits(ranked));
        Explanation explanation = merged.getHits().getHits()[0].getExplanation();

        assertEquals(COMBINATION, explanation.getDescription());
        assertEquals(0.6f, explanation.getValue().floatValue(), 0.0f);
        assertEquals("one node per leg", 2, explanation.getDetails().length);
        assertEquals(NORMALIZATION, explanation.getDetails()[0].getDescription());
        assertEquals(0.4f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("the leg's own explanation is kept under it", 1, explanation.getDetails()[0].getDetails().length);
        assertEquals("leg 0 raw", explanation.getDetails()[0].getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenDocumentWasNotRanked_thenItsOwnExplanationIsKept() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit tailOnly = hit("2", 0.0f);
        tailOnly.explanation(Explanation.match(0.0f, "the Tail matched this document"));

        SearchResponse merged = merger.getMergedResponse(responseWithHits(tailOnly));

        assertEquals(
            "a document fusion never ranked has no fused breakdown to show",
            "the Tail matched this document",
            merged.getHits().getHits()[0].getExplanation().getDescription()
        );
    }

    public void testGetMergedResponse_whenScoreMovedAfterFusion_thenTheFusionIsNestedUnderTheFinalScore() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));

        // No explanation of its own on the hit: nothing says what moved the score, so the fusion is nested under it.
        SearchResponse merged = merger.getMergedResponse(responseWithHits(hit("1", 1.9f)));
        Explanation explanation = merged.getHits().getHits()[0].getExplanation();

        assertEquals("the top node must describe the score the hit has", FINAL_SCORE, explanation.getDescription());
        assertEquals(1.9f, explanation.getValue().floatValue(), 0.0f);
        assertEquals(1, explanation.getDetails().length);
        assertEquals("and the fusion keeps the number it actually produced", COMBINATION, explanation.getDetails()[0].getDescription());
        assertEquals(0.6f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenARescoreMovedTheScore_thenTheFusionReplacesTheFirstPassOfCoresRescoreExplanation() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5334f, 0.4f, 0.8f));
        // Round 2's own tree for a rescored window document, as core's QueryRescorer#explain writes it over the
        // self-erased query (captured live: Top clause `_id:(…)^0.5334` plus the non-scoring Tail, then
        // query_weight 1.0 and rescore_query_weight 2.0 under score_mode total).
        SearchHit rescored = hit("1", 5.829916f);
        rescored.explanation(rescoreTree(5.829916f, "sum of:", selfErasedQuery(0.5334f), 1.0f, 2.648258f, 2.0f));

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals("core's rescore node stays on top", "sum of:", explanation.getDescription());
        assertEquals(5.829916f, explanation.getValue().floatValue(), 0.0f);
        assertEquals(2, explanation.getDetails().length);
        Explanation primary = explanation.getDetails()[0];
        assertEquals("product of:", primary.getDescription());
        assertEquals(0.5334f, primary.getValue().floatValue(), 0.0f);
        assertEquals("the fusion replaces the self-erased first pass", COMBINATION, primary.getDetails()[0].getDescription());
        assertEquals(0.5334f, primary.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("one node per leg under it", 2, primary.getDetails()[0].getDetails().length);
        assertEquals("primaryWeight", primary.getDetails()[1].getDescription());
        Explanation secondary = explanation.getDetails()[1];
        assertEquals("and the rescore query's own explanation is kept verbatim", "product of:", secondary.getDescription());
        assertEquals("weight(text:sofa in 3080)", secondary.getDetails()[0].getDescription());
        assertEquals("secondaryWeight", secondary.getDetails()[1].getDescription());
        assertEquals(2.0f, secondary.getDetails()[1].getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenTheRescoreQueryDidNotMatch_thenTheFusionStillReplacesTheWeightedFirstPass() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // query_weight 0.5 and a rescore query that did not match this document: core returns the weighted first pass alone.
        SearchHit rescored = hit("1", 0.25f);
        rescored.explanation(weightedFirstPass(selfErasedQuery(0.5f), 0.5f));

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals("product of:", explanation.getDescription());
        assertEquals(0.25f, explanation.getValue().floatValue(), 0.0f);
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
        assertEquals(0.5f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("primaryWeight", explanation.getDetails()[1].getDescription());
        assertEquals(0.5f, explanation.getDetails()[1].getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenRescorersAreChained_thenTheFusionReplacesTheInnermostFirstPass() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // Two rescorers: the second one's first pass is the first one's whole tree.
        Explanation first = rescoreTree(2.5f, "sum of:", selfErasedQuery(0.5f), 1.0f, 2.0f, 1.0f);
        Explanation second = rescoreTree(7.5f, "product of:", first, 1.0f, 3.0f, 1.0f);
        SearchHit rescored = hit("1", 7.5f);
        rescored.explanation(second);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(7.5f, explanation.getValue().floatValue(), 0.0f);
        Explanation innerLayer = explanation.getDetails()[0].getDetails()[0];
        assertEquals("the outer layer still wraps the inner one", 2.5f, innerLayer.getValue().floatValue(), 0.0f);
        Explanation innermostFirstPass = innerLayer.getDetails()[0].getDetails()[0];
        assertEquals("and only the innermost first pass is the fusion", COMBINATION, innermostFirstPass.getDescription());
        assertEquals(0.5f, innermostFirstPass.getValue().floatValue(), 0.0f);
        assertEquals("the second rescore query is kept", 3.0f, explanation.getDetails()[1].getDetails()[0].getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenTheFirstOfTwoRescoreQueriesDidNotMatch_thenTheReplacementDescendsThroughItsWeightedFirstPass() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // The first rescorer's query did not match this document, so its layer is core's weighted first pass alone; the
        // second rescorer's matched, and its first pass is that whole layer.
        Explanation first = weightedFirstPass(selfErasedQuery(0.5f), 0.5f);
        Explanation second = rescoreTree(2.25f, "sum of:", first, 1.0f, 2.0f, 1.0f);
        SearchHit rescored = hit("1", 2.25f);
        rescored.explanation(second);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals("core's rescore node stays on top", "sum of:", explanation.getDescription());
        assertEquals(2.25f, explanation.getValue().floatValue(), 0.0f);
        Explanation firstLayer = explanation.getDetails()[0].getDetails()[0];
        assertEquals("the first rescorer's weighted first pass is kept", 0.25f, firstLayer.getValue().floatValue(), 0.0f);
        assertEquals("primaryWeight", firstLayer.getDetails()[1].getDescription());
        assertEquals("with the fusion as the first pass it weights", COMBINATION, firstLayer.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenRoundTwosTreeIsNotAMatch_thenTheFusionIsNestedUnderTheFinalScore() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit moved = hit("1", 1.9f);
        moved.explanation(Explanation.noMatch("First pass did not match", selfErasedQuery(0.6f)));

        Explanation explanation = merger.getMergedResponse(responseWithHits(moved)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenAProductHasMoreThanTwoFactors_thenItIsNotTakenForARescoreLayer() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        // Core's weighted first pass has exactly two factors; a product with a third is some other query's explanation.
        SearchHit moved = hit("1", 1.2f);
        moved.explanation(
            Explanation.match(
                1.2f,
                "product of:",
                selfErasedQuery(0.6f),
                Explanation.match(1.0f, "primaryWeight"),
                Explanation.match(2.0f, "boost")
            )
        );

        Explanation explanation = merger.getMergedResponse(responseWithHits(moved)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenTheSecondFactorIsNotAWeightedRescoreQuery_thenNothingIsReplaced() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // A weighted first pass beside something that is not core's weighted rescore query: not a rescore layer.
        SearchHit moved = hit("1", 1.9f);
        moved.explanation(
            Explanation.match(1.9f, "sum of:", weightedFirstPass(selfErasedQuery(0.5f), 1.0f), Explanation.match(1.4f, "weight(text:a)"))
        );

        Explanation explanation = merger.getMergedResponse(responseWithHits(moved)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(1, explanation.getDetails().length);
    }

    public void testGetMergedResponse_whenRoundTwosTreeIsNotARescore_thenTheFusionIsNestedUnderTheFinalScore() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        // A moved score whose tree is a plain bool: a `sum of:` with two children that are not core's weighted products. The
        // description alone must not be taken for a rescore layer.
        SearchHit moved = hit("1", 1.9f);
        moved.explanation(
            Explanation.match(1.9f, "sum of:", Explanation.match(0.6f, "ConstantScore(_id:[1])"), Explanation.match(1.3f, "weight(text:a)"))
        );

        Explanation explanation = merger.getMergedResponse(responseWithHits(moved)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(1.9f, explanation.getValue().floatValue(), 0.0f);
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenTheFirstPassDoesNotCarryTheFusedScore_thenNothingIsReplaced() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        // A rescore tree whose first pass is some other number: replacing it with the fusion would misattribute the score.
        SearchHit rescored = hit("1", 1.9f);
        rescored.explanation(rescoreTree(1.9f, "sum of:", selfErasedQuery(0.3f), 1.0f, 1.6f, 1.0f));

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(1, explanation.getDetails().length);
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenTheRebuiltTreeDoesNotDescribeTheHitScore_thenTheFinalScoreNodeIsUsed() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // A well-formed rescore tree whose value is not what the hit carries (something after the rescore moved it again):
        // the top node must still describe the score the hit has.
        SearchHit rescored = hit("1", 9.0f);
        rescored.explanation(rescoreTree(2.5f, "sum of:", selfErasedQuery(0.5f), 1.0f, 2.0f, 1.0f));

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(9.0f, explanation.getValue().floatValue(), 0.0f);
    }

    public void testReplaceFirstPass_thenTheInputTreeIsNotMutated() {
        Explanation roundTwo = rescoreTree(5.829916f, "sum of:", selfErasedQuery(0.5334f), 1.0f, 2.648258f, 2.0f);
        Explanation combination = Explanation.match(0.5334f, COMBINATION);

        Explanation rebuilt = FusedDocExplanations.replaceFirstPass(roundTwo, combination, 0.5334f);

        assertNotSame(rebuilt, roundTwo);
        assertEquals(
            "the input still has the self-erased first pass",
            "sum of:",
            roundTwo.getDetails()[0].getDetails()[0].getDescription()
        );
        assertEquals(COMBINATION, rebuilt.getDetails()[0].getDetails()[0].getDescription());
        assertNull("a leaf is not a rescore layer", FusedDocExplanations.replaceFirstPass(Explanation.match(1f, "leaf"), combination, 1f));
    }

    /** Round 2's own explanation of a window document: the Top clause at the fused score plus the non-scoring Tail. */
    private static Explanation selfErasedQuery(final float fusedScore) {
        return Explanation.match(
            fusedScore,
            "sum of:",
            Explanation.match(fusedScore, "_id:([fe 17 78 4f])^" + fusedScore),
            Explanation.match(
                0f,
                "match on required clause, product of:",
                Explanation.match(0f, "# clause"),
                Explanation.match(1f, "text:sofa")
            )
        );
    }

    /** Core's {@code prim}: the first pass times {@code query_weight}. */
    private static Explanation weightedFirstPass(final Explanation firstPass, final float queryWeight) {
        return Explanation.match(
            firstPass.getValue().floatValue() * queryWeight,
            "product of:",
            firstPass,
            Explanation.match(queryWeight, "primaryWeight")
        );
    }

    /** Core's whole rescore layer for a document the rescore query matched. */
    private static Explanation rescoreTree(
        final float value,
        final String scoreModeDescription,
        final Explanation firstPass,
        final float queryWeight,
        final float rescoreQueryScore,
        final float rescoreQueryWeight
    ) {
        Explanation secondary = Explanation.match(
            rescoreQueryScore * rescoreQueryWeight,
            "product of:",
            Explanation.match(rescoreQueryScore, "weight(text:sofa in 3080)"),
            Explanation.match(rescoreQueryWeight, "secondaryWeight")
        );
        return Explanation.match(value, scoreModeDescription, weightedFirstPass(firstPass, queryWeight), secondary);
    }

    public void testGetMergedResponse_whenALegReturnedNoExplanation_thenItsNodeIsALeaf() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        FusedDocExplanations collected = new FusedDocExplanations().combinationDescription(COMBINATION)
            .normalizationDescription(NORMALIZATION);
        collected.addDocument(
            FusedDocExplanations.documentKey(INDEX, "1"),
            0.4f,
            List.of(new FusedDocExplanations.LegContribution(0, 0.4f, null))
        );
        merger.consumer().accept(collected);

        Explanation explanation = merger.getMergedResponse(responseWithHits(hit("1", 0.4f))).getHits().getHits()[0].getExplanation();

        assertEquals("the normalized value is still reported", 0.4f, explanation.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("with nothing invented under it", 0, explanation.getDetails()[0].getDetails().length);
    }

    public void testGetMergedResponse_whenScoresAreNotTracked_thenTheFusedScoreIsReported() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));

        // A hit of a request that did not track scores carries NaN, so there is no final score to describe — the fused
        // score is the only number there is, and comparing against NaN must not nest it under one that does not exist.
        Explanation explanation = merger.getMergedResponse(responseWithHits(hit("1", Float.NaN))).getHits().getHits()[0].getExplanation();

        assertEquals(COMBINATION, explanation.getDescription());
        assertEquals(0.6f, explanation.getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenTheResponseCarriesNoHitsArray_thenItIsReturnedUntouched() {
        // A response section with no hits array at all — the shape a request that asked for nothing back leaves behind.
        // There is nothing to correlate against, and reaching for the array would fail rather than report anything.
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHits noHits = new SearchHits(null, new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internal = new InternalSearchResponse(noHits, InternalAggregations.EMPTY, null, null, false, null, 1);
        SearchResponse response = new SearchResponse(
            internal,
            null,
            1,
            1,
            0,
            1L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        assertFalse("something was collected, so the guard is the array and not the collection", merger.isEmpty());
        assertSame(response, merger.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenAHitCannotBeCorrelated_thenItIsSkippedAndTheRestAreStillAttached() {
        // Correlation is by _index + _id, so a hit missing either cannot be looked up. It is skipped rather than keyed on
        // what is left, which would collide with a real document of another index that happens to share the _id.
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.6f, 0.4f, 0.8f));
        SearchHit indexless = new SearchHit(0, "1", null, null);
        indexless.score(0.6f);
        SearchHit idless = new SearchHit(0, null, null, null);
        idless.shard(new SearchShardTarget("node", new ShardId(INDEX, INDEX + "-uuid", 0), null, OriginalIndices.NONE));
        idless.score(0.6f);

        SearchResponse merged = merger.getMergedResponse(responseWithHits(indexless, idless, hit("1", 0.6f)));

        assertNull("a hit with no _index is left exactly as it came back", merged.getHits().getHits()[0].getExplanation());
        assertNull("and so is one with no _id", merged.getHits().getHits()[1].getExplanation());
        assertEquals(
            "and skipping them does not stop the hits that can be correlated",
            COMBINATION,
            merged.getHits().getHits()[2].getExplanation().getDescription()
        );
    }

    public void testDocumentKey_thenDistinctDocumentsGetDistinctKeys() {
        // The key is never parsed back, so it only has to separate documents and be built the same way in the rewrite and
        // on the response. An _id may contain the separator; an index name may not (OpenSearch rejects '#' in one), so the
        // pair cannot be re-split ambiguously into a different (index, id) that is also a real document.
        assertEquals(FusedDocExplanations.documentKey(INDEX, "a#b"), FusedDocExplanations.documentKey(INDEX, "a#b"));
        assertNotEquals(FusedDocExplanations.documentKey(INDEX, "1"), FusedDocExplanations.documentKey(INDEX, "2"));
        assertNotEquals(FusedDocExplanations.documentKey(INDEX, "1"), FusedDocExplanations.documentKey("other-index", "1"));
    }

    /** One document, two legs, with a raw explanation under each. */
    private FusedDocExplanations collected(final String id, final float fusedScore, final float... normalizedScores) {
        FusedDocExplanations collected = new FusedDocExplanations().combinationDescription(COMBINATION)
            .normalizationDescription(NORMALIZATION);
        List<FusedDocExplanations.LegContribution> contributions = new ArrayList<>();
        for (int leg = 0; leg < normalizedScores.length; leg++) {
            contributions.add(
                new FusedDocExplanations.LegContribution(
                    leg,
                    normalizedScores[leg],
                    Explanation.match(normalizedScores[leg], "leg " + leg + " raw")
                )
            );
        }
        collected.addDocument(FusedDocExplanations.documentKey(INDEX, id), fusedScore, contributions);
        return collected;
    }

    /** A response hit as the fetch phase leaves it: the shard target is what gives it an {@code _index}. */
    private SearchHit hit(final String id, final float score) {
        SearchHit hit = new SearchHit(0, id, null, null);
        hit.shard(new SearchShardTarget("node", new ShardId(INDEX, INDEX + "-uuid", 0), null, OriginalIndices.NONE));
        hit.score(score);
        return hit;
    }

    private SearchResponse responseWithHits(final SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internal = new InternalSearchResponse(searchHits, InternalAggregations.EMPTY, null, null, false, null, 1);
        return new SearchResponse(internal, null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
