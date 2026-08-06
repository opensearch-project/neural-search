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
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The load-bearing PR2 guarantee: for the SAME hit set, the classic shard-side hybrid path
 * ({@link MinMaxScoreNormalizationTechnique#normalize} + {@link ScoreCombiner}) and the resolver coordinator path
 * ({@link CoordinatorScoreFusion}) produce identical fused scores to float precision. Both call the same shared
 * min-max math and the same {@link ScoreCombinationTechnique#combine}; only the data shape differs. If this test ever
 * fails, the two paths have diverged into two implementations — which is exactly what PR2 exists to prevent.
 */
public class CoordinatorScoreFusionDifferentialTests extends OpenSearchTestCase {

    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "uuid-1");
    private static final float DELTA = 1e-6f;

    private ScoreCombinationTechnique arithmeticMean() {
        return new ArithmeticMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
    }

    /**
     * Run the classic path over a single-shard {@link CompoundTopDocs} whose sub-queries carry the given (docId ->
     * rawScore) maps, and return the classic fused score per docId.
     */
    private Map<Integer, Float> classicFused(List<Map<Integer, Float>> perSubQueryScores, ScoreCombinationTechnique combination) {
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

        MinMaxScoreNormalizationTechnique normalization = new MinMaxScoreNormalizationTechnique();
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
    private Map<String, Float> coordinatorFused(List<Map<Integer, Float>> perLegScores, ScoreCombinationTechnique combination) {
        java.util.List<Map<String, Float>> legRawScores = new java.util.ArrayList<>();
        for (Map<Integer, Float> leg : perLegScores) {
            Map<String, Float> byId = new LinkedHashMap<>();
            leg.forEach((docId, score) -> byId.put(String.valueOf(docId), score));
            legRawScores.add(byId);
        }
        return CoordinatorScoreFusion.fuseMinMax(legRawScores, combination);
    }

    private void assertParity(List<Map<Integer, Float>> hitSet) {
        ScoreCombinationTechnique combination = arithmeticMean();
        Map<Integer, Float> classic = classicFused(hitSet, combination);
        Map<String, Float> coordinator = coordinatorFused(hitSet, arithmeticMean());

        assertEquals("same number of fused docs", classic.size(), coordinator.size());
        for (Map.Entry<Integer, Float> e : classic.entrySet()) {
            String id = String.valueOf(e.getKey());
            assertTrue("coordinator missing doc " + id, coordinator.containsKey(id));
            assertEquals("fused score parity for doc " + id, e.getValue(), coordinator.get(id), DELTA);
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
        // One doc in each leg → min==max==score → classic's single-score edge case returns 1.0 on both paths.
        assertParity(List.of(Map.of(7, 0.42f), Map.of(7, 9.9f)));
    }
}
