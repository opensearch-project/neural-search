/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.dto.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The RRF counterpart of {@link CoordinatorScoreFusionDifferentialTests}: for the same hit set, the classic shard-side
 * path ({@link RRFNormalizationTechnique#normalize} + {@link ScoreCombiner} + {@link RRFScoreCombinationTechnique}) and
 * the coordinator path ({@link CoordinatorScoreFusion#fuse} with {@link RrfScalarNormalizer}) produce identical fused
 * scores. Both call the same
 * {@link RRFScoreNormalizer} arithmetic; only the data shape differs. A failure here means the two paths have diverged
 * into two implementations of RRF.
 *
 * <p>Parity is asserted over <em>tie-free</em> score sets, which is the whole contract. Classic breaks a within-leg
 * score tie by Lucene docId then shard id; the coordinator has neither and breaks it by ascending fusion key. That
 * divergence is deliberate — see {@link RrfScalarNormalizer} — and is pinned by
 * {@link #testTieOrder_isByKeyAscending_andIndependentOfInsertionOrder} rather than papered over.
 */
public class CoordinatorScoreFusionRrfDifferentialTests extends OpenSearchTestCase {

    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "uuid-1");
    // Zero, deliberately: both paths call the same scoreForRank and the same combine() over a float[], so the fused
    // scores must agree bit for bit. Any tolerance here would hide exactly the divergence this test exists to catch.
    private static final float DELTA = 0.0f;
    private static final int RANK_CONSTANT = 60;

    private ScoreCombinationTechnique rrfCombination() {
        return new RRFScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
    }

    /**
     * Run the classic path over a single-shard {@link CompoundTopDocs} whose sub-queries carry the given (docId ->
     * rawScore) maps, and return the classic fused score per docId. Each sub-query's {@code scoreDocs} are ordered by
     * descending score, as Lucene delivers them, so the {@code singleShard} positional path and the multi-shard
     * global-ranking path see the same order.
     *
     * @param singleShard drives which classic ranking path runs: {@code true} ranks by array position, {@code false}
     *                    builds the cross-shard priority queue keyed on {@code ScoreDoc.COMPARATOR}
     */
    private Map<Integer, Float> classicFused(
        List<Map<Integer, Float>> perSubQueryScores,
        int rankConstant,
        ScoreCombinationTechnique combination,
        boolean singleShard
    ) {
        List<TopDocs> topDocsPerSubQuery = new ArrayList<>();
        long unionCount = perSubQueryScores.stream().flatMap(m -> m.keySet().stream()).distinct().count();
        for (Map<Integer, Float> subQuery : perSubQueryScores) {
            ScoreDoc[] scoreDocs = subQuery.entrySet()
                .stream()
                .map(e -> new ScoreDoc(e.getKey(), e.getValue()))
                .sorted(Comparator.comparingDouble((ScoreDoc d) -> d.score).reversed().thenComparingInt(d -> d.doc))
                .toArray(ScoreDoc[]::new);
            topDocsPerSubQuery.add(new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs));
        }
        CompoundTopDocs compound = new CompoundTopDocs(
            new TotalHits(unionCount, TotalHits.Relation.EQUAL_TO),
            topDocsPerSubQuery,
            false,
            SEARCH_SHARD
        );

        // Built through the factory because RRFNormalizationTechnique's constructor takes a package-private collaborator.
        ScoreNormalizationTechnique normalization = new ScoreNormalizationFactory().createNormalization(
            RRFNormalizationTechnique.TECHNIQUE_NAME,
            Map.of(RRFScoreNormalizer.PARAM_NAME_RANK_CONSTANT, rankConstant)
        );
        normalization.normalize(
            NormalizeScoresDTO.builder()
                .queryTopDocs(List.of(compound))
                .normalizationTechnique(normalization)
                .singleShard(singleShard)
                .build()
        );

        // Combine exactly as ScoreCombiner does: per-doc float[] (0.0-filled) then combine().
        ScoreCombiner combiner = new ScoreCombiner();
        Map<Integer, float[]> perDoc = combiner.getNormalizedScoresPerDocument(compound.getTopDocs());
        Map<Integer, Float> fused = new LinkedHashMap<>();
        for (Map.Entry<Integer, float[]> e : perDoc.entrySet()) {
            fused.put(e.getKey(), combination.combine(e.getValue()));
        }
        return fused;
    }

    /**
     * Same logical hit set expressed the coordinator way ({@code _id}-keyed per-leg maps), fused via
     * {@link CoordinatorScoreFusion#fuse} with {@link RrfScalarNormalizer}. docId {@code n} maps to key {@code "n"}.
     */
    private Map<String, Float> coordinatorFused(
        List<Map<Integer, Float>> perLegScores,
        int rankConstant,
        ScoreCombinationTechnique combination
    ) {
        List<Map<String, Float>> legRawScores = new ArrayList<>();
        for (Map<Integer, Float> leg : perLegScores) {
            Map<String, Float> byId = new LinkedHashMap<>();
            leg.forEach((docId, score) -> byId.put(String.valueOf(docId), score));
            legRawScores.add(byId);
        }
        return CoordinatorScoreFusion.fuse(legRawScores, new RrfScalarNormalizer(rankConstant), combination);
    }

    /** Assert parity against both classic ranking paths — positional (single shard) and priority-queue (multi shard). */
    private void assertParity(List<Map<Integer, Float>> hitSet) {
        assertParity(hitSet, RANK_CONSTANT);
    }

    private void assertParity(List<Map<Integer, Float>> hitSet, int rankConstant) {
        for (boolean singleShard : new boolean[] { true, false }) {
            Map<Integer, Float> classic = classicFused(hitSet, rankConstant, rrfCombination(), singleShard);
            Map<String, Float> coordinator = coordinatorFused(hitSet, rankConstant, rrfCombination());

            assertEquals("same number of fused docs (singleShard=" + singleShard + ")", classic.size(), coordinator.size());
            for (Map.Entry<Integer, Float> e : classic.entrySet()) {
                String id = String.valueOf(e.getKey());
                assertTrue("coordinator missing doc " + id, coordinator.containsKey(id));
                assertEquals(
                    "fused score parity for doc " + id + " (singleShard=" + singleShard + ")",
                    e.getValue(),
                    coordinator.get(id),
                    DELTA
                );
            }
        }
    }

    public void testParity_singleSubQuery() {
        assertParity(List.of(Map.of(2, 0.5f, 4, 0.2f)));
    }

    public void testParity_multipleSubQueries_overlappingAndDisjointDocs() {
        // leg0 matches {2,4}; leg1 empty; leg2 matches {3,4,2} — doc 4 & 2 appear in multiple legs, doc 3 only in leg2.
        assertParity(List.of(Map.of(2, 0.5f, 4, 0.2f), Map.of(), Map.of(3, 0.9f, 4, 0.7f, 2, 0.1f)));
    }

    public void testParity_twoLegsPartialOverlap() {
        assertParity(List.of(Map.of(1, 5.0f, 2, 3.0f, 3, 1.0f), Map.of(2, 0.8f, 3, 0.6f, 4, 0.4f)));
    }

    public void testParity_rawScoreMagnitudeIsIrrelevant_onlyOrderMatters() {
        // RRF reads rank, not score, so two legs whose scores are orders of magnitude apart still fuse identically as
        // long as the ordering matches. This is what distinguishes it from the min_max path.
        assertParity(List.of(Map.of(1, 900.0f, 2, 800.0f, 3, 700.0f), Map.of(1, 0.003f, 2, 0.002f, 3, 0.001f)));
    }

    public void testParity_atRankConstantBounds() {
        List<Map<Integer, Float>> hitSet = List.of(Map.of(1, 5.0f, 2, 3.0f, 3, 1.0f), Map.of(3, 0.6f, 4, 0.4f));
        assertParity(hitSet, RRFScoreNormalizer.MIN_RANK_CONSTANT);
        assertParity(hitSet, RRFScoreNormalizer.MAX_RANK_CONSTANT);
    }

    public void testParity_withWeights() {
        // Weights are applied by the shared RRFScoreCombinationTechnique on both paths, so parity must survive them.
        ScoreCombinationTechnique weighted = new RRFScoreCombinationTechnique(
            Map.of(ScoreCombinationUtil.PARAM_NAME_WEIGHTS, List.of(0.3, 0.7)),
            new ScoreCombinationUtil()
        );
        List<Map<Integer, Float>> hitSet = List.of(Map.of(1, 5.0f, 2, 3.0f), Map.of(2, 0.8f, 3, 0.6f));
        Map<Integer, Float> classic = classicFused(hitSet, RANK_CONSTANT, weighted, true);
        Map<String, Float> coordinator = coordinatorFused(
            hitSet,
            RANK_CONSTANT,
            new RRFScoreCombinationTechnique(Map.of(ScoreCombinationUtil.PARAM_NAME_WEIGHTS, List.of(0.3, 0.7)), new ScoreCombinationUtil())
        );
        assertEquals(classic.size(), coordinator.size());
        for (Map.Entry<Integer, Float> e : classic.entrySet()) {
            assertEquals("weighted parity for doc " + e.getKey(), e.getValue(), coordinator.get(String.valueOf(e.getKey())), DELTA);
        }
    }

    public void testFuseWithRrf_unmatchedLegContributesNothing() {
        // A doc present in only one of two legs gets that leg's rank score and nothing for the other — the 0.0 slot
        // adds nothing because RRF combines by weighted sum.
        Map<String, Float> fused = CoordinatorScoreFusion.fuse(
            List.of(Map.of("a", 5.0f), Map.of("b", 5.0f)),
            new RrfScalarNormalizer(RANK_CONSTANT),
            rrfCombination()
        );
        float rank0 = RRFScoreNormalizer.scoreForRank(0, RANK_CONSTANT);
        assertEquals(rank0, fused.get("a"), 0.0f);
        assertEquals(rank0, fused.get("b"), 0.0f);
    }

    /**
     * Pins the deliberate divergence from classic: within a leg, documents tied on score are ranked by ascending fusion
     * key. Classic would order the same tie by Lucene docId then shard id, both of which are physical storage artifacts
     * absent on the coordinator, so this is a different — and layout-independent — order, not a bug.
     */
    public void testTieOrder_isByKeyAscending_andIndependentOfInsertionOrder() {
        float rank0 = RRFScoreNormalizer.scoreForRank(0, RANK_CONSTANT);
        float rank1 = RRFScoreNormalizer.scoreForRank(1, RANK_CONSTANT);
        float rank2 = RRFScoreNormalizer.scoreForRank(2, RANK_CONSTANT);

        // Three docs all tied at 1.0 (what a constant_score / match_all / filter-context leg produces for every doc),
        // fed in an order that is neither ascending nor descending by _id.
        Map<String, Float> scrambled = new LinkedHashMap<>();
        scrambled.put("c", 1.0f);
        scrambled.put("a", 1.0f);
        scrambled.put("b", 1.0f);
        Map<String, Float> fused = CoordinatorScoreFusion.fuse(
            List.of(scrambled),
            new RrfScalarNormalizer(RANK_CONSTANT),
            rrfCombination()
        );

        assertEquals("lowest key takes rank 0", rank0, fused.get("a"), 0.0f);
        assertEquals(rank1, fused.get("b"), 0.0f);
        assertEquals(rank2, fused.get("c"), 0.0f);

        // Same docs in a different insertion order must fuse to the same scores — the order is a property of the keys,
        // not of how the leg happened to enumerate its hits.
        Map<String, Float> reordered = new LinkedHashMap<>();
        reordered.put("b", 1.0f);
        reordered.put("c", 1.0f);
        reordered.put("a", 1.0f);
        assertEquals(fused, CoordinatorScoreFusion.fuse(List.of(reordered), new RrfScalarNormalizer(RANK_CONSTANT), rrfCombination()));
    }
}
