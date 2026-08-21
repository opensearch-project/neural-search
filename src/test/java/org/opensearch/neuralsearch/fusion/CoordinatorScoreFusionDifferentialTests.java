/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.SearchShard;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.dto.NormalizeScoresDTO;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The load-bearing guarantee for fused mode: for the SAME hit set, the classic shard-side hybrid path
 * ({@link ScoreNormalizationTechnique#normalize} + {@link ScoreCombiner}) and the resolver coordinator path
 * ({@link CoordinatorScoreFusion}) produce identical fused scores. Both call the same shared normalization math and the
 * same {@link ScoreCombinationTechnique#combine}; only the data shape differs. If this test ever fails, the two paths
 * have diverged into two implementations — which is exactly what the shared-core extraction exists to prevent.
 *
 * <p>Every case runs across the whole score-normalization family ({@code min_max}, {@code z_score}, {@code l2}), since a
 * divergence is a property of a technique's arithmetic rather than of the hit set. The hit sets are single-shard, which
 * is what makes the assertion exact rather than approximate: {@code l2} accumulates its sum of squares in {@code float}
 * and float addition is not associative, so classic's per-shard accumulation and the coordinator's accumulation over the
 * already-merged leg agree bit for bit on one shard and may differ in the last bit across shards.
 */
public class CoordinatorScoreFusionDifferentialTests extends OpenSearchTestCase {

    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "uuid-1");
    private static final List<String> NORMALIZATION_TECHNIQUES = List.of("min_max", "z_score", "l2");

    private ScoreCombinationTechnique arithmeticMean() {
        return new ArithmeticMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
    }

    /**
     * Run the classic path over a single-shard {@link CompoundTopDocs} whose sub-queries carry the given (docId ->
     * rawScore) maps, and return the classic fused score per docId.
     */
    private Map<Integer, Float> classicFused(
        List<Map<Integer, Float>> perSubQueryScores,
        String normalizationTechnique,
        ScoreCombinationTechnique combination
    ) {
        // Build CompoundTopDocs: one TopDocs per sub-query.
        java.util.List<TopDocs> topDocsPerSubQuery = new java.util.ArrayList<>();
        long unionCount = perSubQueryScores.stream().flatMap(m -> m.keySet().stream()).distinct().count();
        for (Map<Integer, Float> subQuery : perSubQueryScores) {
            ScoreDoc[] scoreDocs = subQuery.entrySet().stream().map(e -> new ScoreDoc(e.getKey(), e.getValue())).toArray(ScoreDoc[]::new);
            topDocsPerSubQuery.add(new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs));
        }
        CompoundTopDocs compound = new CompoundTopDocs(
            new TotalHits(unionCount, TotalHits.Relation.EQUAL_TO),
            topDocsPerSubQuery,
            false,
            SEARCH_SHARD
        );

        ScoreNormalizationTechnique normalization = new ScoreNormalizationFactory().createNormalization(normalizationTechnique);
        normalization.normalize(NormalizeScoresDTO.builder().queryTopDocs(List.of(compound)).normalizationTechnique(normalization).build());

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
     * {@link CoordinatorScoreFusion}. docId {@code n} maps to {@code _id} string {@code "n"}.
     */
    private Map<String, Float> coordinatorFused(
        List<Map<Integer, Float>> perLegScores,
        String normalizationTechnique,
        ScoreCombinationTechnique combination
    ) {
        java.util.List<Map<String, Float>> legRawScores = new java.util.ArrayList<>();
        for (Map<Integer, Float> leg : perLegScores) {
            Map<String, Float> byId = new LinkedHashMap<>();
            leg.forEach((docId, score) -> byId.put(String.valueOf(docId), score));
            legRawScores.add(byId);
        }
        return CoordinatorScoreFusion.fuse(legRawScores, ScalarNormalizers.forTechnique(normalizationTechnique), combination);
    }

    /** Assert classic and coordinator agree on the given hit set, for every wired normalization technique. */
    private void assertParity(List<Map<Integer, Float>> hitSet) {
        for (String technique : NORMALIZATION_TECHNIQUES) {
            Map<Integer, Float> classic = classicFused(hitSet, technique, arithmeticMean());
            Map<String, Float> coordinator = coordinatorFused(hitSet, technique, arithmeticMean());

            assertEquals("same number of fused docs for " + technique, classic.size(), coordinator.size());
            for (Map.Entry<Integer, Float> e : classic.entrySet()) {
                String id = String.valueOf(e.getKey());
                assertTrue("coordinator missing doc " + id + " for " + technique, coordinator.containsKey(id));
                // Bit equality, not a delta: on a single shard the two paths run the same arithmetic over the same
                // values in the same order, so anything less would let a real divergence hide inside the tolerance.
                assertEquals(
                    "fused score parity for doc " + id + " with " + technique,
                    Float.floatToIntBits(e.getValue()),
                    Float.floatToIntBits(coordinator.get(id))
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

    public void testParity_singleDocPerLeg_minEqualsMaxEdgeCase() {
        // One doc in each leg → min==max==score. Each technique has its own single-score edge case (min_max returns 1.0,
        // z_score returns the sub-query max, l2 divides by its own magnitude) and all three must match classic.
        assertParity(List.of(Map.of(7, 0.42f), Map.of(7, 9.9f)));
    }

    public void testParity_wideScoreRange() {
        // Scores spanning several orders of magnitude, where the three techniques disagree most with each other — and so
        // where a coordinator-side reimplementation of any one of them would show up.
        assertParity(List.of(Map.of(1, 1000.0f, 2, 0.001f, 3, 42.5f), Map.of(2, 7.0f, 3, 7.0f, 4, 0.5f)));
    }

    public void testParity_allZeroScoresInOneLeg() {
        // A leg whose scores are all 0.0 drives l2's zero-norm guard and min_max's min==max branch; classic and the
        // coordinator must take that branch identically rather than one of them emitting NaN.
        assertParity(List.of(Map.of(1, 0.0f, 2, 0.0f), Map.of(1, 0.6f, 2, 0.3f)));
    }
}
