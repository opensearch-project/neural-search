/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.rescore.QueryRescoreMode;
import org.opensearch.search.rescore.QueryRescorer;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

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
    /** Where a rescore tree that was not kept is reported. */
    private static final String LOGGER_NAME = FusedDocExplanations.class.getCanonicalName();
    /** Matches the one document {@link #roundTwo} explains over. */
    private static final Query MATCHES = new TermQuery(new Term("text", "lamp"));
    /** Matches nothing there. */
    private static final Query MISSES = new TermQuery(new Term("text", "sofa"));

    /** One rescorer: its query, its weights and its score mode. Core rescored the document. */
    private record Layer(Query query, float queryWeight, float rescoreQueryWeight, QueryRescoreMode scoreMode) {
    }

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

    public void testGetMergedResponse_whenARescoreMovedTheScore_thenTheFusionReplacesTheMarkedFirstPass() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5334f, 0.4f, 0.8f));
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5334f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals("core's own rescore node stays on top", roundTwo.getDescription(), explanation.getDescription());
        assertEquals(roundTwo.getValue(), explanation.getValue());
        Explanation weightedFirstPass = explanation.getDetails()[0];
        assertEquals("the fusion stands where the marked first pass was", COMBINATION, weightedFirstPass.getDetails()[0].getDescription());
        assertEquals(0.5334f, weightedFirstPass.getDetails()[0].getValue().floatValue(), 0.0f);
        assertEquals("one node per leg under it", 2, weightedFirstPass.getDetails()[0].getDetails().length);
        assertSame("core's weight node is kept as it was", roundTwo.getDetails()[0].getDetails()[1], weightedFirstPass.getDetails()[1]);
        assertSame("and so is the rescore query's whole explanation", roundTwo.getDetails()[1], explanation.getDetails()[1]);
        assertEquals("no marker is left behind", 0, count(explanation, FusedFirstPassMarker.DESCRIPTION));
    }

    public void testGetMergedResponse_whenTheRescoreLeftTheScoreUnchanged_thenCoresLayerIsShownAroundTheFusion() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // A rescore query that did not match the document and query_weight 1.0: the score stays the fused score, and core
        // still explains the rescore it ran — the weighted first pass alone.
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MISSES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", 0.5f);
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals("the layer core wrote, as for any rescored query", roundTwo.getDescription(), explanation.getDescription());
        assertEquals(0.5f, explanation.getValue().floatValue(), 0.0f);
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
        assertSame(roundTwo.getDetails()[1], explanation.getDetails()[1]);
    }

    public void testGetMergedResponse_whenRescorersAreChained_thenTheFusionReplacesTheFirstPassWhereverCoreNestedIt() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // Three rescorers: the first one's query misses, the next two match under different score modes.
        Explanation roundTwo = roundTwo(
            selfErasedQuery(0.5f),
            layer(MISSES, 0.5f, 1.0f, QueryRescoreMode.Total),
            layer(MATCHES, 1.0f, 3.0f, QueryRescoreMode.Multiply),
            layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total)
        );
        SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(roundTwo.getValue(), explanation.getValue());
        assertEquals("exactly one fused breakdown", 1, count(explanation, COMBINATION));
        assertEquals("and no marker", 0, count(explanation, FusedFirstPassMarker.DESCRIPTION));
        assertEquals("every layer core wrote is still there", count(roundTwo, "primaryWeight"), count(explanation, "primaryWeight"));
        assertEquals(count(roundTwo, "secondaryWeight"), count(explanation, "secondaryWeight"));
    }

    public void testGetMergedResponse_forEveryScoreMode_thenCoresRescoreTreeIsKept() throws IOException {
        for (QueryRescoreMode mode : QueryRescoreMode.values()) {
            FusedExplanationMerger merger = new FusedExplanationMerger();
            merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
            Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MATCHES, 0.7f, 1.3f, mode));
            SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
            rescored.explanation(roundTwo);

            Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

            assertEquals(mode + ": core's node stays on top", roundTwo.getDescription(), explanation.getDescription());
            assertEquals(mode.toString(), 1, count(explanation, COMBINATION));
        }
    }

    public void testGetMergedResponse_whenRoundTwoRanAFlooredScore_thenTheFloorStandsInForTheFirstPass() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        // Fusion computed 0.0; round 2 ran with the floored score, which is what the marked first pass carries.
        merger.consumer().accept(collectedRunAt("1", 0.0f, 1e-30f, 0.0f, 0.0f));
        Explanation roundTwo = roundTwo(selfErasedQuery(1e-30f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals("the rescore tree is kept", roundTwo.getDescription(), explanation.getDescription());
        Explanation firstPass = explanation.getDetails()[0].getDetails()[0];
        assertEquals("the floored value round 2 ran with", FINAL_SCORE, firstPass.getDescription());
        assertEquals(1e-30f, firstPass.getValue().floatValue(), 0.0f);
        assertEquals(
            "over the fusion, which keeps the number it actually produced",
            COMBINATION,
            firstPass.getDetails()[0].getDescription()
        );
        assertEquals(0.0f, firstPass.getDetails()[0].getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenTheMarkedFirstPassIsNotTheScoreRoundTwoRan_thenTheFinalScoreNodeIsUsed() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // Something added to round 2's query on the shard (a scoring wrapper, +1.0): the first pass is no longer the fused
        // score, so putting the fusion there would misattribute the number.
        Explanation roundTwo = roundTwo(selfErasedQuery(1.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(roundTwo.getValue().floatValue(), explanation.getValue().floatValue(), 0.0f);
        assertEquals(1, explanation.getDetails().length);
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenTheRebuiltTreeDoesNotDescribeTheHitScore_thenTheFinalScoreNodeIsUsed() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // A well-formed rescore tree whose value is not what the hit carries: the top node must describe the hit's score.
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", 9.0f);
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(9.0f, explanation.getValue().floatValue(), 0.0f);
    }

    @TestLogging(value = "org.opensearch.neuralsearch.search.explain.FusedDocExplanations:DEBUG", reason = "a tree that is not kept is reported at debug")
    public void testGetMergedResponse_whenTheRebuiltTreeIsNotKept_thenTheDisagreeingNumbersAreLoggedAtDebug() throws Exception {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", 9.0f);
        rescored.explanation(roundTwo);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(LOGGER_NAME))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "the rebuilt value, the hit's score and their distance",
                    LOGGER_NAME,
                    Level.DEBUG,
                    "*rescore explanation of ["
                        + INDEX
                        + "#1] rebuilds to "
                        + roundTwo.getValue().floatValue()
                        + " but the hit scored 9.0 (*ulps apart, tolerance 4)*"
                )
            );
            merger.getMergedResponse(responseWithHits(rescored));
            appender.assertAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.opensearch.neuralsearch.search.explain.FusedDocExplanations:DEBUG", reason = "a marker that is not the first pass is reported at debug")
    public void testGetMergedResponse_whenTheMarkedFirstPassIsNotTheScoreRoundTwoRan_thenItIsLoggedAtDebug() throws Exception {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        Explanation roundTwo = roundTwo(selfErasedQuery(1.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
        rescored.explanation(roundTwo);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(LOGGER_NAME))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "the score round 2 ran with and the hit's score",
                    LOGGER_NAME,
                    Level.DEBUG,
                    "*marked first pass of ["
                        + INDEX
                        + "#1] does not carry the score round 2 ran with (0.5); naming the hit's score "
                        + roundTwo.getValue().floatValue()
                        + "*"
                )
            );
            merger.getMergedResponse(responseWithHits(rescored));
            appender.assertAllExpectationsMatched();
        }
    }

    /** The negative control the two above need: a tree that is kept, and a hit with no rescore at all, say nothing. */
    @TestLogging(value = "org.opensearch.neuralsearch.search.explain.FusedDocExplanations:DEBUG", reason = "nothing to report when the tree is kept")
    public void testGetMergedResponse_whenTheRebuiltTreeIsKept_thenNothingIsLogged() throws Exception {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit kept = hit("1", roundTwo.getValue().floatValue());
        kept.explanation(roundTwo);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(LOGGER_NAME))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "a kept tree is not reported",
                    LOGGER_NAME,
                    Level.DEBUG,
                    "fused hybrid explain:*"
                )
            );
            merger.getMergedResponse(responseWithHits(kept));
            merger.getMergedResponse(responseWithHits(hit("1", 0.5f)));
            appender.assertAllExpectationsMatched();
        }
    }

    public void testGetMergedResponse_whenTheRootIsAFewUlpsOffTheHitScore_thenCoresTreeIsStillKept() throws IOException {
        // function_score and script_score explain in float what they score in double, so a correct tree can sit an ulp or
        // two away from the hit's score; ten ulps is beyond what that produces and is answered as a mismatch.
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        float value = roundTwo.getValue().floatValue();
        for (float hitScore : new float[] { Math.nextUp(value), Math.nextDown(Math.nextDown(value)) }) {
            FusedExplanationMerger merger = new FusedExplanationMerger();
            merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
            SearchHit rescored = hit("1", hitScore);
            rescored.explanation(roundTwo);

            Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

            assertEquals("kept at " + hitScore, roundTwo.getDescription(), explanation.getDescription());
        }
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        SearchHit offByTen = hit("1", value + 10 * Math.ulp(value));
        offByTen.explanation(roundTwo);

        assertEquals(
            FINAL_SCORE,
            merger.getMergedResponse(responseWithHits(offByTen)).getHits().getHits()[0].getExplanation().getDescription()
        );
    }

    public void testGetMergedResponse_whenTheRootOrTheHitScoreIsNotFinite_thenOnlyAnExactMatchIsKept() {
        // Saturating weights can drive core's arithmetic to infinity; ulps say nothing there, so only equality counts.
        Explanation infinite = Explanation.match(
            Float.POSITIVE_INFINITY,
            "sum of:",
            Explanation.match(
                0.5f,
                "product of:",
                FusedFirstPassMarker.mark(selfErasedQuery(0.5f)),
                Explanation.match(1.0f, "primaryWeight")
            ),
            Explanation.match(
                Float.POSITIVE_INFINITY,
                "product of:",
                Explanation.match(1.0f, "weight(text:lamp)"),
                Explanation.match(Float.MAX_VALUE, "secondaryWeight")
            )
        );
        Explanation finite = Explanation.match(
            2.5f,
            "sum of:",
            Explanation.match(
                0.5f,
                "product of:",
                FusedFirstPassMarker.mark(selfErasedQuery(0.5f)),
                Explanation.match(1.0f, "primaryWeight")
            ),
            Explanation.match(2.0f, "product of:", Explanation.match(1.0f, "weight(text:lamp)"), Explanation.match(2.0f, "secondaryWeight"))
        );

        assertEquals("sum of:", explainedAs(infinite, Float.POSITIVE_INFINITY).getDescription());
        assertEquals(FINAL_SCORE, explainedAs(infinite, 2.5f).getDescription());
        assertEquals(FINAL_SCORE, explainedAs(finite, Float.POSITIVE_INFINITY).getDescription());
    }

    public void testGetMergedResponse_whenTheRebuiltTreeFailsAndTheScoreDidNotMove_thenTheFusionIsTheWholeTree() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // A layer the score never went through — a chain on several shards reports its rescored documents as one set —
        // moves the tree's value while the hit kept its fused score: the tree is rejected, and the hit reads as it would
        // with no rescore at all.
        Explanation roundTwo = roundTwo(selfErasedQuery(0.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit unmoved = hit("1", 0.5f);
        unmoved.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(unmoved)).getHits().getHits()[0].getExplanation();

        assertEquals(COMBINATION, explanation.getDescription());
        assertEquals(0.5f, explanation.getValue().floatValue(), 0.0f);
    }

    public void testGetMergedResponse_whenRoundTwoCarriesNoMarker_thenAMovedScoreIsNestedUnderTheFinalScore() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // Core's rescore tree without the marker, as a shard that does not mark returns it: nothing in it is recognised.
        Explanation roundTwo = unmarkedRoundTwo(selfErasedQuery(0.5f), layer(MATCHES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit rescored = hit("1", roundTwo.getValue().floatValue());
        rescored.explanation(roundTwo);

        Explanation explanation = merger.getMergedResponse(responseWithHits(rescored)).getHits().getHits()[0].getExplanation();

        assertEquals(FINAL_SCORE, explanation.getDescription());
        assertEquals(roundTwo.getValue().floatValue(), explanation.getValue().floatValue(), 0.0f);
        assertEquals(COMBINATION, explanation.getDetails()[0].getDescription());
    }

    public void testGetMergedResponse_whenARescoredDocumentWasNotRanked_thenItsMarkIsLeftForTheStrip() throws IOException {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        // A document only the Tail matched, rescored: there is no fused breakdown for it, so the merger leaves its tree
        // alone and the strip that follows restores exactly what core built.
        Explanation tailFirstPass = Explanation.match(0.0f, "the Tail matched this document");
        Explanation roundTwo = roundTwo(tailFirstPass, layer(MISSES, 1.0f, 2.0f, QueryRescoreMode.Total));
        SearchHit tailOnly = hit("2", 0.0f);
        tailOnly.explanation(roundTwo);

        SearchResponse merged = merger.getMergedResponse(responseWithHits(tailOnly));
        assertSame(roundTwo, merged.getHits().getHits()[0].getExplanation());
        FusedFirstPassMarker.stripAll(merged);

        assertEquals(
            unmarkedRoundTwo(tailFirstPass, layer(MISSES, 1.0f, 2.0f, QueryRescoreMode.Total)),
            merged.getHits().getHits()[0].getExplanation()
        );
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

    private static Layer layer(final Query query, final float queryWeight, final float rescoreQueryWeight, final QueryRescoreMode mode) {
        return new Layer(query, queryWeight, rescoreQueryWeight, mode);
    }

    /**
     * Round 2's explanation of a rescored hit as the shard produces it: the first pass marked, then folded through core's
     * own {@code QueryRescorer#explain} once per rescorer — the real producer of every layer around the marker, so
     * nothing here depends on how this test thinks core lays them out.
     */
    private Explanation roundTwo(final Explanation firstPass, final Layer... layers) throws IOException {
        return fold(FusedFirstPassMarker.mark(firstPass), layers);
    }

    /** The same, from a shard that does not mark. */
    private Explanation unmarkedRoundTwo(final Explanation firstPass, final Layer... layers) throws IOException {
        return fold(firstPass, layers);
    }

    private Explanation fold(final Explanation firstPass, final Layer... layers) throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                Document document = new Document();
                document.add(new TextField("text", "lamp desk", Field.Store.NO));
                writer.addDocument(document);
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Explanation explanation = firstPass;
                for (Layer layer : layers) {
                    QueryRescorer.QueryRescoreContext context = new QueryRescorer.QueryRescoreContext(10);
                    context.setParsedQuery(new ParsedQuery(layer.query()));
                    context.setQueryWeight(layer.queryWeight());
                    context.setRescoreQueryWeight(layer.rescoreQueryWeight());
                    context.setScoreMode(layer.scoreMode());
                    context.setRescoredDocs(Set.of(0));
                    explanation = QueryRescorer.INSTANCE.explain(0, searcher, context, explanation);
                }
                return explanation;
            }
        }
    }

    /** Hit "1", fused at 0.5, with round 2's tree and score as given, run through a fresh merger. */
    private Explanation explainedAs(final Explanation roundTwo, final float hitScore) {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        merger.consumer().accept(collected("1", 0.5f, 0.4f, 0.8f));
        SearchHit hit = hit("1", hitScore);
        hit.explanation(roundTwo);
        return merger.getMergedResponse(responseWithHits(hit)).getHits().getHits()[0].getExplanation();
    }

    /** How many nodes of the tree carry the description. */
    private static int count(final Explanation node, final String description) {
        int count = description.equals(node.getDescription()) ? 1 : 0;
        for (Explanation child : node.getDetails()) {
            count += count(child, description);
        }
        return count;
    }

    public void testGetMergedResponse_whenALegReturnedNoExplanation_thenItsNodeIsALeaf() {
        FusedExplanationMerger merger = new FusedExplanationMerger();
        FusedDocExplanations collected = new FusedDocExplanations().combinationDescription(COMBINATION)
            .normalizationDescription(NORMALIZATION);
        collected.addDocument(
            FusedDocExplanations.documentKey(INDEX, "1"),
            0.4f,
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
        return collectedRunAt(id, fusedScore, fusedScore, normalizedScores);
    }

    /** The same, for a document round 2 ran at a score other than its fused one (a floored one). */
    private FusedDocExplanations collectedRunAt(
        final String id,
        final float fusedScore,
        final float roundTwoScore,
        final float... normalizedScores
    ) {
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
        collected.addDocument(FusedDocExplanations.documentKey(INDEX, id), fusedScore, roundTwoScore, contributions);
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
