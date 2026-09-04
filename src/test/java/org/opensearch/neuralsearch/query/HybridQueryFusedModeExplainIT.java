/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import lombok.SneakyThrows;

/**
 * End-to-end coverage of what {@code explain: true} reports for a fused ({@code fusion}) {@code hybrid} query.
 *
 * <p>A fused hybrid normalizes and combines on the coordinator and replaces itself with a {@code bool} over the fused
 * window's {@code _id}s, so round 2's own explanation of a ranked hit is a childless {@code constant_score} carrying the
 * fused score — the right number with nothing under it, and describing the query the rewrite substituted rather than the
 * hybrid the user wrote. {@code FusedDocExplanations} keeps each leg's own round-1 explanation and the normalized value
 * fusion derived from it, and {@code FusedExplanationMerger} rebuilds the tree on the response, correlating by
 * {@code _index} plus {@code _id}.
 *
 * <p>Needs a live cluster for all of it: which nodes the tree has, what they are called, that the numbers agree with the
 * scores the same request returns, that a document fusion never ranked is left alone, and that asking to be explained does
 * not change the answer. The wording is asserted verbatim against classic hybrid's, since matching classic is the point —
 * see the parity test.
 *
 * <p>Dataset: 6 documents, ids 1..6, each with the same text and a 2-dimensional vector, queried by two legs — a
 * {@code knn} leg and a {@code term} leg that matches all six.
 *
 * <p>Run: {@code ./gradlew integTest --tests "*HybridQueryFusedModeExplainIT*"}.
 */
public class HybridQueryFusedModeExplainIT extends BaseNeuralSearchIT {

    private static final String INDEX = "test-fused-explain";
    private static final String NORM_PIPELINE = "fused-explain-norm-pipeline";
    private static final String TEXT_FIELD = "text";
    private static final String VECTOR_FIELD = "vec";
    private static final int TOTAL_DOCS = 6;
    private static final int WINDOW_SIZE = 10;

    /** What the combination node is called, unweighted: classic's {@code ScoreCombiner} format over the technique's describe(). */
    private static final String COMBINATION_DESCRIPTION = "arithmetic_mean combination of:";
    /** What each per-leg node is called: classic's {@code ExplanationUtils} format over the normalization's describe(). */
    private static final String NORMALIZATION_DESCRIPTION = "min_max normalization of:";
    /** The same node under rrf, where describe() and the technique name diverge — the case that made describe() necessary. */
    private static final String RANK_CONSTANT_PARAM = "rank_constant";
    private static final int RRF_RANK_CONSTANT = 42;
    private static final String RRF_EXPLAIN_PIPELINE = "fused-explain-classic-rrf-pipeline";
    private static final String RRF_NORMALIZATION_DESCRIPTION = "rrf, rank_constant [" + RRF_RANK_CONSTANT + "] normalization of:";
    /** The node inserted above the combination when the score round 2 returned is not the fused score. */
    private static final String FINAL_SCORE_DESCRIPTION = "score of the fused hybrid query as round 2 returned it, computed from:";
    /** Floats survive one JSON round trip exactly; the tolerance is for the arithmetic asserted across nodes. */
    private static final double DELTA = 1e-5;

    /**
     * The shape of the tree on the flat case: one node per leg that matched, under one combination node whose value is the
     * hit's score, with each leg's own round-1 explanation kept underneath it.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_thenEveryLegIsDescribedUnderTheFusedScore() {
        ensureDataset();

        Map<String, Object> response = search(explained(fusedHybrid(knnLeg(WINDOW_SIZE), termLeg())));
        List<Map<String, Object>> hits = hits(response);

        assertEquals("both legs match every document", TOTAL_DOCS, hits.size());
        for (Map<String, Object> hit : hits) {
            Map<String, Object> explanation = explanationOf(hit);
            assertEquals("the top node describes the fusion, not the substituted query", COMBINATION_DESCRIPTION, description(explanation));
            assertEquals("and it carries the score the same request returned for this hit", score(hit), value(explanation), DELTA);
            List<Map<String, Object>> legs = details(explanation);
            assertEquals("one node per leg that matched: " + descriptions(legs), 2, legs.size());
            for (Map<String, Object> leg : legs) {
                assertEquals("each leg reports what normalization made of its score", NORMALIZATION_DESCRIPTION, description(leg));
                assertTrue("min_max normalizes into [0, 1]: " + value(leg), value(leg) >= 0.0 && value(leg) <= 1.0);
                List<Map<String, Object>> raw = details(leg);
                assertEquals("and keeps the leg's own explanation of the raw score under it: " + descriptions(raw), 1, raw.size());
            }
            // The fused score is the mean of the two normalized values, which is what makes the tree an account of the
            // number above it rather than a list of unrelated scores.
            double mean = legs.stream().mapToDouble(this::value).average().orElseThrow();
            assertEquals("the combination node is the combination of its children", mean, value(explanation), DELTA);
        }
    }

    /**
     * The one node of the tree the legs cannot describe: each leg's own explanation is the query the user wrote. A
     * {@code term} leg names the field and term it scored, which is what the classic path shows too.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_thenALegsOwnExplanationIsTheQueryTheUserWrote() {
        ensureDataset();

        Map<String, Object> response = search(explained(fusedHybrid(knnLeg(WINDOW_SIZE), termLeg())));
        List<String> rawDescriptions = new ArrayList<>();
        for (Map<String, Object> leg : details(explanationOf(hits(response).get(0)))) {
            rawDescriptions.add(description(details(leg).get(0)));
        }

        assertEquals("one raw explanation per leg: " + rawDescriptions, 2, rawDescriptions.size());
        assertTrue(
            "one leg must be explained as the term query it is: " + rawDescriptions,
            rawDescriptions.stream().anyMatch(description -> description.contains(TEXT_FIELD + ":hello"))
        );
        // Lucene explains an ANN match as the candidate set it came from ("within top N docs") rather than as a scored
        // clause — a vector query has no term to attribute the score to. Asserted loosely for that reason: what matters is
        // that the leg's own account of its raw score is what the tree carries, not which words Lucene chose for it.
        assertTrue(
            "and the other as the vector-candidate set it came from: " + rawDescriptions,
            rawDescriptions.stream().anyMatch(description -> description.contains("docs"))
        );
    }

    /**
     * Weights are part of how the score was reached, so they belong in the description — same as classic, which renders them
     * from the combination technique's own {@code describe()}.
     */
    @SneakyThrows
    public void testExplainedWeightedFusedHybrid_thenTheWeightsAreNamed() {
        ensureDataset();

        Map<String, Object> response = search(explained(weightedFusedHybrid(knnLeg(WINDOW_SIZE), termLeg())));
        Map<String, Object> explanation = explanationOf(hits(response).get(0));

        assertEquals("arithmetic_mean, weights [0.7, 0.3] combination of:", description(explanation));
        List<Map<String, Object>> legs = details(explanation);
        assertEquals(
            "the weights are applied to the values the children report, not folded into them",
            0.7 * value(legs.get(0)) + 0.3 * value(legs.get(1)),
            value(explanation),
            DELTA
        );
    }

    /**
     * A weight of {@code 0.0} zeroes its leg's contribution, so a document that reached fusion through that leg alone fuses
     * to exactly {@code 0.0} and is floored away from the Tail before round 2 ranks it. The tree must then name what round 2
     * returned and keep the score fusion computed underneath it — labelling the combination node with the floor would claim
     * a number its own children do not produce.
     *
     * <p>{@code k=2} is load-bearing: on a dataset where every leg matches every document, min_max maps a zero-range leg to
     * {@code 1.0} and no document fuses to {@code 0.0} at all, so the case would not arise. Bounding the ANN leg leaves four
     * documents reaching fusion through the zero-weighted {@code term} leg alone. {@code size} covers them because they sort
     * last, being the lowest-scored documents in the window.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_whenALegWeightIsZero_thenTheFlooredScoreIsNamedAboveTheComputedOne() {
        ensureDataset();

        Map<String, Object> response = search(explained(zeroWeightedFusedHybrid(knnLeg(2), termLeg())));

        List<Map<String, Object>> floored = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            Map<String, Object> explanation = explanationOf(hit);
            if (FINAL_SCORE_DESCRIPTION.equals(description(explanation))) {
                floored.add(explanation);
            }
        }

        assertEquals("the four documents the zero-weighted leg alone surfaced fused to 0.0 and were floored", 4, floored.size());
        for (Map<String, Object> explanation : floored) {
            List<Map<String, Object>> children = details(explanation);
            assertEquals("the combination is the only child", 1, children.size());
            assertEquals("the child reports what fusion computed, which is exactly zero", 0.0, value(children.get(0)), DELTA);
            assertTrue("and the parent reports the floored score round 2 ranked by, which is above it", value(explanation) > 0.0);
        }
    }

    /**
     * A document only one leg matched. {@code k=2} bounds the ANN leg to two candidates, so the other four documents reach
     * fusion through the {@code term} leg alone — and the tree names one leg, exactly as classic does for a document only
     * one sub-query matched.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_whenALegDidNotMatch_thenOnlyTheMatchingLegsAreNamed() {
        ensureDataset();

        Map<String, Object> response = search(explained(fusedHybrid(knnLeg(2), termLeg())));
        List<Integer> legCounts = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            legCounts.add(details(explanationOf(hit)).size());
        }

        assertEquals("every document is still ranked and described", TOTAL_DOCS, legCounts.size());
        assertEquals("the two ANN candidates are described by both legs", 2, legCounts.stream().filter(count -> count == 2).count());
        assertEquals("the other four by the term leg alone", 4, legCounts.stream().filter(count -> count == 1).count());
    }

    /**
     * A {@code rescore} moves the score after fusion — the one way a fused hit's score can differ from its fused score, since
     * the {@code hybrid} query rejects {@code boost} at parse time. The fused combination can then no longer be the top node:
     * relabelling it with the final score would claim the fusion produced a number it did not. It becomes a child of a node
     * that names the final score instead.
     */
    @SneakyThrows
    public void testExplainedRescoredFusedHybrid_thenTheFusedScoreIsNestedUnderTheFinalScore() {
        ensureDataset();

        Map<String, Object> response = search(rescored(fusedHybrid(knnLeg(WINDOW_SIZE), termLeg())));
        Map<String, Object> hit = hits(response).get(0);
        Map<String, Object> explanation = explanationOf(hit);

        assertEquals("the top node describes the score the hit actually has", FINAL_SCORE_DESCRIPTION, description(explanation));
        assertEquals(score(hit), value(explanation), DELTA);
        List<Map<String, Object>> children = details(explanation);
        assertEquals("with the fusion underneath it: " + descriptions(children), 1, children.size());
        assertEquals(COMBINATION_DESCRIPTION, description(children.get(0)));
        assertTrue(
            "the rescore is what stands between them: " + value(children.get(0)) + " -> " + value(explanation),
            value(explanation) > value(children.get(0))
        );
    }

    /**
     * A request that sorts without tracking scores: the hits come back with no score at all, so there is no final score for
     * the tree to describe and the fusion is reported on its own. The fused score is still the fused score — a node claiming
     * a final score of zero would describe a number nothing computed.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_whenScoresAreNotTracked_thenTheFusionIsReportedOnItsOwn() {
        ensureDataset();

        Map<String, Object> response = search(
            "{\"query\":"
                + fusedHybrid(knnLeg(WINDOW_SIZE), termLeg())
                + ",\"explain\":true,\"sort\":[\"_doc\"],\"size\":"
                + (TOTAL_DOCS + 1)
                + "}"
        );
        List<Map<String, Object>> hits = hits(response);

        assertEquals(TOTAL_DOCS, hits.size());
        for (Map<String, Object> hit : hits) {
            assertNull("sorting without track_scores leaves the hit unscored", hit.get("_score"));
            Map<String, Object> explanation = explanationOf(hit);
            assertEquals(
                "the fusion is the whole tree, with nothing above it: " + description(explanation),
                COMBINATION_DESCRIPTION,
                description(explanation)
            );
            assertTrue("and it still reports what fusion computed", value(explanation) > 0.0);
        }
    }

    /**
     * A document round 2 returned that fusion never ranked — one the Tail surfaced beyond the window. There is no fused
     * breakdown to put there, and round 2's own explanation is truthful: the document matched a non-scoring clause. So it is
     * left alone, which is also what makes "explained" a property of the window rather than of the response.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_whenTheTailSurfacesADocumentBeyondTheWindow_thenItKeepsItsOwnExplanation() {
        ensureDataset();

        Map<String, Object> response = search(explained(fusedHybridWithWindow(2, knnLeg(WINDOW_SIZE), termLeg())));
        List<Map<String, Object>> hits = hits(response);

        assertEquals("the Tail surfaces the whole match set", TOTAL_DOCS, hits.size());
        List<String> fused = new ArrayList<>();
        List<String> untouched = new ArrayList<>();
        for (Map<String, Object> hit : hits) {
            String id = String.valueOf(hit.get("_id"));
            if (COMBINATION_DESCRIPTION.equals(description(explanationOf(hit)))) {
                fused.add(id);
            } else {
                untouched.add(id);
            }
        }

        assertEquals("only the window is described as fused: " + fused, 2, fused.size());
        assertEquals("and the rest keep round 2's own explanation: " + untouched, TOTAL_DOCS - 2, untouched.size());
        for (Map<String, Object> hit : hits) {
            if (untouched.contains(String.valueOf(hit.get("_id")))) {
                assertEquals("a document the Tail alone surfaced scores zero", 0.0, score(hit), DELTA);
            }
        }
    }

    /**
     * Parity with classic hybrid, which is the reason the wording is not ours to choose: a fused hybrid and a classic hybrid
     * over the same two sub-queries, with the same normalization and combination, must describe the fusion in the same words.
     * Classic renders it from a search pipeline ({@code hybrid_score_explanation}); fused renders it with no pipeline at all.
     */
    @SneakyThrows
    public void testExplainedFusedHybrid_thenTheWordingMatchesClassicHybrid() {
        ensureDataset();

        Map<String, Object> fused = search(explained(fusedHybrid(knnLeg(WINDOW_SIZE), termLeg())));
        Map<String, Object> classic = search(explained(classicHybrid()), EXPLAIN_PIPELINE);

        List<String> classicDescriptions = new ArrayList<>();
        collectDescriptions(explanationOf(hits(classic).get(0)), classicDescriptions);
        assertTrue(
            "classic must describe the combination the same way: " + classicDescriptions,
            classicDescriptions.contains(COMBINATION_DESCRIPTION)
        );
        assertTrue("and the normalization the same way: " + classicDescriptions, classicDescriptions.contains(NORMALIZATION_DESCRIPTION));
        assertEquals("which is what the fused tree is built from", COMBINATION_DESCRIPTION, description(explanationOf(hits(fused).get(0))));
    }

    /**
     * The same parity claim on the one technique that can break it. min_max, z_score and l2 all describe themselves with just
     * their name, so the test above passes whether the fused side renders the technique's description or merely its name —
     * which is how a fused rrf came to explain itself as a bare {@code rrf}, dropping the rank constant classic names and
     * every score in the tree below depends on. A non-default constant is used deliberately: 60 would pass against a
     * hardcoded default.
     */
    @SneakyThrows
    public void testExplainedRrfFusedHybrid_thenTheRankConstantIsNamedJustAsClassicNamesIt() {
        ensureDataset();
        // The score-ranker-processor helper, not createSearchPipeline: that one understands only min_max's bounds under
        // normalization.parameters and emits an empty parameters object for anything else, so a rank_constant handed to it
        // is silently dropped and classic ranks at 60 — which is exactly the kind of quiet default this test is about.
        createRRFSearchPipeline(RRF_EXPLAIN_PIPELINE, List.of(), RRF_RANK_CONSTANT, true);

        Map<String, Object> fused = search(explained(rrfFusedHybrid(knnLeg(WINDOW_SIZE), termLeg())));
        Map<String, Object> classic = search(explained(classicHybrid()), RRF_EXPLAIN_PIPELINE);

        List<String> fusedDescriptions = new ArrayList<>();
        collectDescriptions(explanationOf(hits(fused).get(0)), fusedDescriptions);
        List<String> classicDescriptions = new ArrayList<>();
        collectDescriptions(explanationOf(hits(classic).get(0)), classicDescriptions);

        assertTrue("classic names the rank constant: " + classicDescriptions, classicDescriptions.contains(RRF_NORMALIZATION_DESCRIPTION));
        assertTrue("and so must fused: " + fusedDescriptions, fusedDescriptions.contains(RRF_NORMALIZATION_DESCRIPTION));
        assertFalse(
            "the bare technique name is what this used to render, and it describes a normalization the request did not ask "
                + "for: "
                + fusedDescriptions,
            fusedDescriptions.contains("rrf normalization of:")
        );
    }

    /**
     * A fused hybrid nested inside a container. The hit's score is the enclosing query's, not the hybrid's, so replacing the
     * whole tree with the fusion would describe the wrong number — the request keeps core's own explanation, which correctly
     * describes the query round 2 ran.
     */
    @SneakyThrows
    public void testExplainedNestedFusedHybrid_thenCoreExplanationIsKept() {
        ensureDataset();

        String nested = "{\"bool\":{\"must\":[" + fusedHybrid(knnLeg(WINDOW_SIZE), termLeg()) + "]}}";
        Map<String, Object> response = search(explained(nested));

        List<String> descriptions = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            collectDescriptions(explanationOf(hit), descriptions);
        }
        assertFalse("no hit may be described as if the hybrid were its whole score: " + descriptions, descriptions.isEmpty());
        assertFalse(
            "a nested fused hybrid contributes to the score rather than being it: " + descriptions,
            descriptions.contains(COMBINATION_DESCRIPTION)
        );
    }

    /**
     * The control, and the claim that matters most: explaining a fused hybrid runs its legs explained, which is a different
     * execution — so the answer is measured with and without it. An unexplained request also gets no explanations at all.
     */
    @SneakyThrows
    public void testFusedHybrid_whenExplained_thenRankingAndTotalsAreUnchanged() {
        ensureDataset();
        String query = fusedHybrid(knnLeg(WINDOW_SIZE), termLeg());

        Map<String, Object> plain = search("{\"query\":" + query + ",\"track_total_hits\":true}");
        Map<String, Object> explained = search(explained(query));

        assertTrue(
            "an unexplained request carries no explanations",
            hits(plain).stream().noneMatch(hit -> Objects.nonNull(hit.get("_explanation")))
        );
        assertTrue(
            "an explained one carries them on every ranked hit",
            hits(explained).stream().allMatch(hit -> Objects.nonNull(hit.get("_explanation")))
        );
        assertEquals("explaining the legs must not change the fused ranking or the scores", rankedHits(plain), rankedHits(explained));
        assertEquals("explaining the legs must not change the totals", totalHits(plain), totalHits(explained));
    }

    /**
     * Both reports at once, over the wire. The two are merged by one listener and by different means — {@code profile}
     * rebuilds the response around the hits while {@code explain} writes onto the hits themselves — so asking for both is
     * the case where one could drop the other. Covered only through mocked consumers in a unit test until now.
     */
    @SneakyThrows
    public void testFusedHybrid_whenExplainedAndProfiled_thenTheResponseCarriesBoth() {
        ensureDataset();
        String query = fusedHybrid(knnLeg(WINDOW_SIZE), termLeg());

        Map<String, Object> response = search(
            "{\"query\":" + query + ",\"explain\":true,\"profile\":true,\"track_total_hits\":true,\"size\":" + (TOTAL_DOCS + 1) + "}"
        );

        assertTrue(
            "every ranked hit still carries its fused explanation",
            hits(response).stream().allMatch(hit -> Objects.nonNull(hit.get("_explanation")))
        );
        assertEquals(
            "and the fusion is still what the top node describes",
            COMBINATION_DESCRIPTION,
            description(explanationOf(hits(response).get(0)))
        );
        assertNotNull("while the profile section the rebuild carried is present too", response.get("profile"));
    }

    // ------------------------------------------------ request bodies ------------------------------------------------

    private String knnLeg(final int k) {
        return "{\"knn\":{\"" + VECTOR_FIELD + "\":{\"vector\":[1.1,1.0],\"k\":" + k + "}}}";
    }

    private String termLeg() {
        return "{\"term\":{\"" + TEXT_FIELD + "\":\"hello\"}}";
    }

    private String fusedHybrid(final String... legs) {
        return fusedHybridWithWindow(WINDOW_SIZE, legs);
    }

    private String fusedHybridWithWindow(final int windowSize, final String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + windowSize
            + ",\"normalization\":{\"technique\":\"min_max\"},\"combination\":{\"technique\":\"arithmetic_mean\"}},"
            + "\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    /**
     * rrf with a rank constant that is not the default, so the description has to come from the config. Written in the
     * score-ranker-processor shape — {@code rank_constant} on the {@code combination} clause — which is where
     * {@code RRFProcessorFactory} reads it, so {@link #createRRFSearchPipeline} builds classic the same config.
     */
    private String rrfFusedHybrid(final String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"combination\":{\"technique\":\"rrf\",\""
            + RANK_CONSTANT_PARAM
            + "\":"
            + RRF_RANK_CONSTANT
            + "}},\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    /** Weights that zero the second leg entirely, so a document only that leg surfaced fuses to exactly {@code 0.0}. */
    private String zeroWeightedFusedHybrid(final String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[1.0,0.0]}}},"
            + "\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    private String weightedFusedHybrid(final String... legs) {
        return "{\"hybrid\":{\"fusion\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"normalization\":{\"technique\":\"min_max\"},"
            + "\"combination\":{\"technique\":\"arithmetic_mean\",\"parameters\":{\"weights\":[0.7,0.3]}}},"
            + "\"queries\":["
            + String.join(",", legs)
            + "]}}";
    }

    /** The fused hybrid, then a rescore that adds a term score on top of the fused one — so the hit's score is not the fused score. */
    private String rescored(final String query) {
        return "{\"query\":"
            + query
            + ",\"explain\":true,\"track_total_hits\":true,\"size\":"
            + (TOTAL_DOCS + 1)
            + ",\"rescore\":{\"window_size\":"
            + WINDOW_SIZE
            + ",\"query\":{\"rescore_query\":"
            + termLeg()
            + ",\"query_weight\":1.0,\"rescore_query_weight\":2.0}}}";
    }

    /** The same two legs with no {@code fusion} block: classic hybrid, normalized and explained by a search pipeline. */
    private String classicHybrid() {
        return "{\"hybrid\":{\"queries\":[" + knnLeg(WINDOW_SIZE) + "," + termLeg() + "]}}";
    }

    private String explained(final String query) {
        return "{\"query\":" + query + ",\"explain\":true,\"track_total_hits\":true,\"size\":" + (TOTAL_DOCS + 1) + "}";
    }

    // --------------------------------------------- explanation parsing ----------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> explanationOf(final Map<String, Object> hit) {
        Map<String, Object> explanation = (Map<String, Object>) hit.get("_explanation");
        assertNotNull("the request asked to be explained and hit " + hit.get("_id") + " carries no explanation", explanation);
        return explanation;
    }

    private String description(final Map<String, Object> node) {
        return String.valueOf(node.get("description"));
    }

    private double value(final Map<String, Object> node) {
        return ((Number) node.get("value")).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> details(final Map<String, Object> node) {
        List<Map<String, Object>> details = (List<Map<String, Object>>) node.get("details");
        return Objects.isNull(details) ? List.of() : details;
    }

    private List<String> descriptions(final List<Map<String, Object>> nodes) {
        return nodes.stream().map(this::description).toList();
    }

    /** Every description in a tree, depth first — for asserting that a wording appears (or does not) at any depth. */
    private void collectDescriptions(final Map<String, Object> node, final List<String> into) {
        into.add(description(node));
        for (Map<String, Object> child : details(node)) {
            collectDescriptions(child, into);
        }
    }

    // -------------------------------------------------- dataset -----------------------------------------------------

    /** The classic hybrid's pipeline: the same normalization and combination, plus the response processor that explains it. */
    private static final String EXPLAIN_PIPELINE = "fused-explain-classic-pipeline";

    private String indexConfig() {
        return "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":1,\"number_of_replicas\":0,"
            + "\"search.default_pipeline\":\""
            + NORM_PIPELINE
            + "\"}},\"mappings\":{\"properties\":{\""
            + TEXT_FIELD
            + "\":{\"type\":\"text\"},\""
            + VECTOR_FIELD
            + "\":{\"type\":\"knn_vector\",\"dimension\":2,"
            + "\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\"}}}}}";
    }

    @SneakyThrows
    private void ensureDataset() {
        createSearchPipeline(NORM_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        createSearchPipeline(EXPLAIN_PIPELINE, "min_max", Map.of(), "arithmetic_mean", Map.of(), true);
        if (indexExists(INDEX)) {
            return;
        }
        createIndex(INDEX, indexConfig());
        for (int id = 1; id <= TOTAL_DOCS; id++) {
            Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id + "?refresh=true");
            request.setJsonEntity(
                "{\"" + TEXT_FIELD + "\":\"hello world document " + id + "\",\"" + VECTOR_FIELD + "\":[1." + id + ",1.0]}"
            );
            Response response = client().performRequest(request);
            int code = response.getStatusLine().getStatusCode();
            assertTrue(
                "indexing " + INDEX + "/" + id + " failed: " + code,
                code == RestStatus.OK.getStatus() || code == RestStatus.CREATED.getStatus()
            );
        }
    }

    private Map<String, Object> search(final String jsonBody) {
        return search(jsonBody, null);
    }

    @SneakyThrows
    private Map<String, Object> search(final String jsonBody, final String pipeline) {
        String endpoint = "/" + INDEX + "/_search" + (Objects.isNull(pipeline) ? "" : "?search_pipeline=" + pipeline);
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(jsonBody);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return Objects.isNull(hitList) ? List.of() : hitList;
    }

    private double score(final Map<String, Object> hit) {
        return ((Number) hit.get("_score")).doubleValue();
    }

    /** The hit list as {@code _id@_score}, which is both the ranking and the scores in one comparable value. */
    private List<String> rankedHits(final Map<String, Object> response) {
        List<String> ranked = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            ranked.add(hit.get("_id") + "@" + hit.get("_score"));
        }
        return ranked;
    }

    @SuppressWarnings("unchecked")
    private long totalHits(final Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertNotNull("track_total_hits was asked for", total);
        return ((Number) total.get("value")).longValue();
    }
}
