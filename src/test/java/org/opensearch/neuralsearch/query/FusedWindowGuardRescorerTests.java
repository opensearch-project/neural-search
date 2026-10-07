/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.lucene.search.function.ScoreFunction;
import org.opensearch.common.lucene.search.function.WeightFactorFunction;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.neuralsearch.search.explain.FusedDocExplanations;
import org.opensearch.neuralsearch.search.explain.FusedFirstPassMarker;
import org.opensearch.search.rescore.QueryRescorer;
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

    /**
     * The all-ranked pool still has to be clamped. There is no band to separate, but the delegates can have driven a
     * ranked score past {@code -Float.MAX_VALUE} or to NaN, and returning that verbatim reaches the coordinator raw: NaN
     * is the largest value under {@code Float.compare} so it would float to rank 1 and render as a null {@code _score},
     * and a saturated score would tie the sentinel a shard holding Tail matches is about to report for a document this
     * one outranks.
     */
    public void testRescore_whenEverythingWasRankedButAScoreSaturated_thenItIsStillClamped() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.4f));

        TopDocs result = guard(flattenTo(Float.NEGATIVE_INFINITY)).rescore(pool, null, context());

        assertEquals(RANKED_FLOOR, result.scoreDocs[0].score, 0.0f);
        assertEquals(RANKED_FLOOR, result.scoreDocs[1].score, 0.0f);
    }

    /** Same path for NaN, which is the value that would otherwise be sorted to the top of the page. */
    public void testRescore_whenEverythingWasRankedButAScoreWentNaN_thenItIsStillClamped() throws IOException {
        TopDocs pool = poolOf(new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.4f));

        TopDocs result = guard(flattenTo(Float.NaN)).rescore(pool, null, context());

        assertFalse("NaN must not reach the coordinator", Float.isNaN(result.scoreDocs[0].score));
        assertEquals(RANKED_FLOOR, result.scoreDocs[0].score, 0.0f);
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

    /**
     * The first pass is wrapped once, at its own value, before any delegate sees it, so the coordinator can find it again
     * wherever the delegates nested it without reading their layers.
     */
    public void testExplain_marksTheFirstPassOnceBeforeAnyDelegate() throws IOException {
        Explanation firstPass = Explanation.match(0.5f, "first pass");

        Explanation explanation = guard(recording(new ArrayList<>(), "a"), recording(new ArrayList<>(), "b")).explain(
            1,
            null,
            context(),
            firstPass
        );

        Explanation marker = explanation.getDetails()[0].getDetails()[0];
        assertEquals(FusedFirstPassMarker.DESCRIPTION, marker.getDescription());
        assertSame(firstPass.getValue(), marker.getValue());
        assertSame(firstPass, marker.getDetails()[0]);
    }

    /** A first pass that did not match has no value for core to build on, and nothing for the coordinator to replace. */
    public void testExplain_whenTheFirstPassDidNotMatch_thenNothingIsMarked() throws IOException {
        Explanation firstPass = Explanation.noMatch("first pass did not match");

        Explanation explanation = guard(recording(new ArrayList<>(), "a")).explain(1, null, context(), firstPass);

        assertSame(firstPass, explanation.getDetails()[0]);
    }

    /**
     * A ranked document the rescore moved is explained the way core explains any rescore, with the rescore query as the
     * user wrote it: the fused window the query ran intersected with appears nowhere in the tree, and the arithmetic is
     * the one core computes over the query that ran.
     */
    public void testExplain_whenTheConfinedQueryRescoredTheDocument_thenTheRescoreQueryIsExplainedAsDeclared() throws IOException {
        try (Directory directory = newDirectory(); IndexReader reader = corpus(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            int ranked = docId(searcher, "a");
            QueryRescorer.QueryRescoreContext confined = confined(Set.of(ranked));

            Explanation explanation = guardOver(confined).explain(ranked, searcher, null, RANKED_FIRST_PASS);

            Explanation asItRan = QueryRescorer.INSTANCE.explain(ranked, searcher, confined, FusedFirstPassMarker.mark(RANKED_FIRST_PASS));
            assertEquals("the score the rescore produced", asItRan.getValue().floatValue(), explanation.getValue().floatValue(), 0.0f);
            assertEquals("core's combination of the weighted first pass and rescore query", 2, explanation.getDetails().length);
            assertEquals(
                "the rescore query, exactly as core explains it on its own",
                searcher.explain(DECLARED, ranked),
                explanation.getDetails()[1].getDetails()[0]
            );
            assertFalse("nothing of the window: " + explanation, explanation.toString().contains(WINDOW_FIELD));
            assertTrue("which explaining the query as it ran would describe", asItRan.toString().contains(WINDOW_FIELD));
        }
    }

    /**
     * A document only the Tail matched can sit in the shard's rescore window, and the rescore query alone matches it — but
     * the query that ran did not, so no rescore may be claimed for it: the tree is exactly what core says over the
     * query that ran.
     */
    public void testExplain_whenTheRescoreWindowReachedADocumentOutsideTheFusedWindow_thenNoRescoreIsClaimed() throws IOException {
        try (Directory directory = newDirectory(); IndexReader reader = corpus(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            int tailOnly = docId(searcher, "c");
            assertTrue("the declared query alone matches it", searcher.explain(DECLARED, tailOnly).isMatch());
            QueryRescorer.QueryRescoreContext confined = confined(Set.of(tailOnly));

            Explanation explanation = guardOver(confined).explain(tailOnly, searcher, null, TAIL_FIRST_PASS);

            assertEquals(
                QueryRescorer.INSTANCE.explain(tailOnly, searcher, confined, FusedFirstPassMarker.mark(TAIL_FIRST_PASS)),
                explanation
            );
            assertEquals("the weighted first pass alone", "product of:", explanation.getDescription());
        }
    }

    /**
     * A segment can hold none of the fused window — the window is coordinator-global — and then the confined query has no
     * scorer there at all: nothing in the segment was rescored by it, whatever the rescore query alone says.
     */
    public void testExplain_whenTheSegmentHoldsNoDocumentOfTheWindow_thenNoRescoreIsClaimed() throws IOException {
        try (Directory directory = newDirectory(); IndexReader reader = corpus(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            int matching = docId(searcher, "a");
            QueryRescorer.QueryRescoreContext confined = confined(Set.of(matching), "elsewhere");

            Explanation explanation = guardOver(confined).explain(matching, searcher, null, RANKED_FIRST_PASS);

            assertEquals(
                QueryRescorer.INSTANCE.explain(matching, searcher, confined, FusedFirstPassMarker.mark(RANKED_FIRST_PASS)),
                explanation
            );
            assertEquals("the weighted first pass alone", "product of:", explanation.getDescription());
        }
    }

    /** A document beyond the rescore window was never rescored, whatever the queries say about it. */
    public void testExplain_whenTheDocumentWasNotRescored_thenNoRescoreIsClaimed() throws IOException {
        try (Directory directory = newDirectory(); IndexReader reader = corpus(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            int ranked = docId(searcher, "a");
            QueryRescorer.QueryRescoreContext confined = confined(Set.of());

            Explanation explanation = guardOver(confined).explain(ranked, searcher, null, RANKED_FIRST_PASS);

            assertEquals(
                QueryRescorer.INSTANCE.explain(ranked, searcher, confined, FusedFirstPassMarker.mark(RANKED_FIRST_PASS)),
                explanation
            );
        }
    }

    /** A rescore query that is not the confinement's shape is explained exactly as it ran. */
    public void testExplain_whenTheQueryIsNotTheConfinement_thenItIsExplainedAsItRan() throws IOException {
        try (Directory directory = newDirectory(); IndexReader reader = corpus(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            int ranked = docId(searcher, "a");
            QueryRescorer.QueryRescoreContext plain = new QueryRescorer.QueryRescoreContext(10);
            plain.setParsedQuery(new ParsedQuery(DECLARED));
            plain.setRescoredDocs(Set.of(ranked));

            Explanation explanation = guardOver(plain).explain(ranked, searcher, null, RANKED_FIRST_PASS);

            assertEquals(
                QueryRescorer.INSTANCE.explain(ranked, searcher, plain, FusedFirstPassMarker.mark(RANKED_FIRST_PASS)),
                explanation
            );
        }
    }

    /**
     * A two-phase rescore query — a phrase, whose approximation is every document holding both terms — claims a rescore
     * only for the documents it really matches: the guard asks the confined query's scorer to advance to the document,
     * which for a two-phase scorer confirms the match, exactly as Lucene's own {@code QueryRescorer} decides one.
     */
    public void testExplain_whenTheRescoreQueryIsTwoPhase_thenOnlyItsConfirmedMatchesClaimARescore() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                for (String[] doc : new String[][] { { "phrase", "lamp desk" }, { "terms", "desk lamp" } }) {
                    Document document = new Document();
                    document.add(new StringField(WINDOW_FIELD, doc[0], Field.Store.NO));
                    document.add(new TextField(TEXT_FIELD, doc[1], Field.Store.NO));
                    writer.addDocument(document);
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                int phrase = docId(searcher, "phrase");
                int terms = docId(searcher, "terms");
                Query window = new TermInSetQuery(WINDOW_FIELD, List.of(new BytesRef("phrase"), new BytesRef("terms")));
                Query confinedQuery = new BooleanQuery.Builder().add(new PhraseQuery(TEXT_FIELD, "lamp", "desk"), BooleanClause.Occur.MUST)
                    .add(window, BooleanClause.Occur.FILTER)
                    .build();
                QueryRescorer.QueryRescoreContext confined = new QueryRescorer.QueryRescoreContext(10);
                confined.setParsedQuery(new ParsedQuery(confinedQuery));
                confined.setRescoreQueryWeight(2.0f);
                confined.setRescoredDocs(Set.of(phrase, terms));

                Explanation matched = guardOver(confined).explain(phrase, searcher, null, RANKED_FIRST_PASS);
                Explanation approximationOnly = guardOver(confined).explain(terms, searcher, null, RANKED_FIRST_PASS);

                assertTrue("the phrase is in the first document: " + matched, matched.toString().contains("secondaryWeight"));
                assertEquals(
                    "the second holds both terms in the wrong order, so no rescore is claimed",
                    QueryRescorer.INSTANCE.explain(terms, searcher, confined, FusedFirstPassMarker.mark(RANKED_FIRST_PASS)),
                    approximationOnly
                );
                assertFalse(approximationOnly.toString().contains("secondaryWeight"));
            }
        }
    }

    /**
     * A {@code function_score} rescore query explains in float what it scores in double, so the tree the guard explains can
     * land an ulp or two off the hit's score. The coordinator must keep every one of those trees — run end to end here: the
     * guard rescores and explains over core's own {@code QueryRescorer}, and the coordinator's {@link FusedDocExplanations}
     * rebuilds each hit.
     */
    public void testExplain_whenAFunctionScoreRescoreExplainsOffItsScore_thenTheCoordinatorStillKeepsEveryTree() throws IOException {
        Random random = new Random(42);
        String[] vocabulary = { "lamp", "desk", "chair", "sofa", "table" };
        int documents = 200;
        try (Directory directory = newDirectory()) {
            List<BytesRef> ids = new ArrayList<>();
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                for (int i = 0; i < documents; i++) {
                    StringBuilder text = new StringBuilder();
                    for (int word = 0, words = 1 + random.nextInt(6); word < words; word++) {
                        text.append(vocabulary[random.nextInt(vocabulary.length)]).append(' ');
                    }
                    Document document = new Document();
                    document.add(new StringField(WINDOW_FIELD, "d" + i, Field.Store.NO));
                    document.add(new TextField(TEXT_FIELD, text.toString(), Field.Store.NO));
                    writer.addDocument(document);
                    ids.add(new BytesRef("d" + i));
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                ScoreFunction[] functions = {
                    new WeightFactorFunction(1.3f),
                    new WeightFactorFunction(0.7f),
                    new WeightFactorFunction(2.1f) };
                Query declared = new FunctionScoreQuery(
                    new TermQuery(new Term(TEXT_FIELD, "desk")),
                    FunctionScoreQuery.ScoreMode.SUM,
                    functions,
                    CombineFunction.MULTIPLY,
                    null,
                    Float.MAX_VALUE
                );
                Query confinedQuery = new BooleanQuery.Builder().add(declared, BooleanClause.Occur.MUST)
                    .add(new TermInSetQuery(WINDOW_FIELD, ids), BooleanClause.Occur.FILTER)
                    .build();
                QueryRescorer.QueryRescoreContext confined = new QueryRescorer.QueryRescoreContext(documents);
                confined.setParsedQuery(new ParsedQuery(confinedQuery));
                confined.setQueryWeight(0.7f);
                confined.setRescoreQueryWeight(1.2f);
                FusedWindowGuardRescorer guard = guardOver(confined);
                float[] firstPass = new float[documents];
                ScoreDoc[] pool = new ScoreDoc[documents];
                FusedDocExplanations collected = new FusedDocExplanations().combinationDescription("arithmetic_mean combination of:")
                    .normalizationDescription("min_max normalization of:");
                for (int doc = 0; doc < documents; doc++) {
                    firstPass[doc] = 1e-30f + random.nextFloat();
                    pool[doc] = new ScoreDoc(doc, firstPass[doc]);
                    collected.addDocument(
                        FusedDocExplanations.documentKey("index", String.valueOf(doc)),
                        firstPass[doc],
                        firstPass[doc],
                        List.of()
                    );
                }
                Arrays.sort(pool, (left, right) -> Float.compare(right.score, left.score));

                TopDocs rescored = guard.rescore(new TopDocs(TOTAL, pool), searcher, null);

                int offTheScore = 0;
                for (ScoreDoc hit : rescored.scoreDocs) {
                    Explanation roundTwo = guard.explain(hit.doc, searcher, null, Explanation.match(firstPass[hit.doc], "first pass"));
                    if (Float.compare(roundTwo.getValue().floatValue(), hit.score) != 0) {
                        offTheScore++;
                    }
                    Explanation explained = collected.explain(
                        FusedDocExplanations.documentKey("index", String.valueOf(hit.doc)),
                        hit.score,
                        roundTwo
                    );
                    assertEquals("doc " + hit.doc + " keeps core's rescore tree", roundTwo.getDescription(), explained.getDescription());
                }
                assertTrue("the case this pins: some trees are explained off their score", offTheScore > 0);
            }
        }
    }

    /** Only {@code bool{must: <query>, filter: <window>}}, in that order and with nothing else, is read as the confinement. */
    public void testDeclaredQuery_recognisesOnlyTheConfinementShape() {
        Query window = new TermInSetQuery(WINDOW_FIELD, List.of(new BytesRef("a")));
        assertSame(DECLARED, FusedWindowGuardRescorer.declaredQuery(confinedQuery()));
        assertNull("not a bool", FusedWindowGuardRescorer.declaredQuery(DECLARED));
        assertNull(
            "the clauses the other way round",
            FusedWindowGuardRescorer.declaredQuery(
                new BooleanQuery.Builder().add(window, BooleanClause.Occur.FILTER).add(DECLARED, BooleanClause.Occur.MUST).build()
            )
        );
        assertNull(
            "a scoring second clause",
            FusedWindowGuardRescorer.declaredQuery(
                new BooleanQuery.Builder().add(DECLARED, BooleanClause.Occur.MUST).add(window, BooleanClause.Occur.SHOULD).build()
            )
        );
        assertNull(
            "a minimum_should_match",
            FusedWindowGuardRescorer.declaredQuery(
                new BooleanQuery.Builder().setMinimumNumberShouldMatch(1)
                    .add(DECLARED, BooleanClause.Occur.MUST)
                    .add(window, BooleanClause.Occur.FILTER)
                    .build()
            )
        );
        assertNull(
            "a third clause",
            FusedWindowGuardRescorer.declaredQuery(
                new BooleanQuery.Builder().add(DECLARED, BooleanClause.Occur.MUST)
                    .add(window, BooleanClause.Occur.FILTER)
                    .add(window, BooleanClause.Occur.FILTER)
                    .build()
            )
        );
    }

    // ---- helpers ----

    /** The field the stand-in fused window filters on — named so that any description mentioning it is recognisable. */
    private static final String WINDOW_FIELD = "fused_window_member";
    private static final String TEXT_FIELD = "text";
    /** The user's rescore query, as {@link FusedRescoreScope} finds it before confining it. */
    private static final Query DECLARED = new TermQuery(new Term(TEXT_FIELD, "lamp"));
    private static final Explanation RANKED_FIRST_PASS = Explanation.match(0.5f, "first pass");
    private static final Explanation TAIL_FIRST_PASS = Explanation.match(0.0f, "first pass");

    /** "a" and "b" are in the fused window; "a" and "c" match the rescore query, so "c" is the Tail-only match. */
    private IndexReader corpus(final Directory directory) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
            for (String[] doc : new String[][] { { "a", "lamp desk" }, { "b", "sofa table" }, { "c", "lamp chair" } }) {
                Document document = new Document();
                document.add(new StringField(WINDOW_FIELD, doc[0], Field.Store.NO));
                document.add(new TextField(TEXT_FIELD, doc[1], Field.Store.NO));
                writer.addDocument(document);
            }
        }
        return DirectoryReader.open(directory);
    }

    private int docId(final IndexSearcher searcher, final String id) throws IOException {
        return searcher.search(new TermQuery(new Term(WINDOW_FIELD, id)), 1).scoreDocs[0].doc;
    }

    /**
     * The shape {@code BoolQueryBuilder#doToQuery} compiles the confinement to: the user's query, then the window — by
     * default "a" and "b".
     */
    private Query confinedQuery(final String... window) {
        List<BytesRef> ids = new ArrayList<>();
        for (String id : window.length == 0 ? new String[] { "a", "b" } : window) {
            ids.add(new BytesRef(id));
        }
        Query windowFilter = new TermInSetQuery(WINDOW_FIELD, ids);
        return new BooleanQuery.Builder().add(DECLARED, BooleanClause.Occur.MUST).add(windowFilter, BooleanClause.Occur.FILTER).build();
    }

    private QueryRescorer.QueryRescoreContext confined(final Set<Integer> rescored, final String... window) {
        QueryRescorer.QueryRescoreContext context = new QueryRescorer.QueryRescoreContext(10);
        context.setParsedQuery(new ParsedQuery(confinedQuery(window)));
        context.setRescoreQueryWeight(2.0f);
        context.setRescoredDocs(rescored);
        return context;
    }

    private FusedWindowGuardRescorer guardOver(final RescoreContext... delegates) {
        return new FusedWindowGuardRescorer(List.of(delegates));
    }

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
