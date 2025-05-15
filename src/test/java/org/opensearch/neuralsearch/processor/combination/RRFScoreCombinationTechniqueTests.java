/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;

public class RRFScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    private static final int RANK_CONSTANT = 60;
    private RRFScoreCombinationTechnique combinationTechnique;
    private ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public RRFScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = (scores, weights) -> RRF(scores, weights);
        combinationTechnique = new RRFScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, 0.0f, 0.6f);
        List<Double> weights = List.of(0.45, 0.15, 0.4);
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique(Map.of(PARAM_NAME_WEIGHTS, weights), scoreCombinationUtil);
        // 1 x 0.45 + 0 x 0.15 + 0.6 x 0.4 = 0.69
        float expectedScore = 0.69f;
        testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = IntStream.range(0, RANDOM_SCORES_SIZE).mapToObj(i -> 1.0 / RANDOM_SCORES_SIZE).collect(Collectors.toList());
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique(Map.of(PARAM_NAME_WEIGHTS, weights), scoreCombinationUtil);
        testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    public void testDescribe() {
        String description = combinationTechnique.describe();
        assertEquals("rrf", description);
    }

    public void testCombineWithEmptyInput() {
        float[] scores = new float[0];
        float result = combinationTechnique.combine(scores);
        assertEquals(0.0f, result, 0.001f);
    }

    public void testCombineWithSingleScore() {
        float[] scores = new float[] { 0.5f };
        float result = combinationTechnique.combine(scores);
        assertEquals(0.5f, result, 0.001f);
    }

    public void testCombineWithMultipleScores() {
        float[] scores = new float[] { 0.8f, 0.6f, 0.4f };
        float result = combinationTechnique.combine(scores);
        float expected = 0.8f + 0.6f + 0.4f;
        assertEquals(expected, result, 0.001f);
    }

    public void testCombineWithZeroScores() {
        float[] scores = new float[] { 0.0f, 0.0f };
        float result = combinationTechnique.combine(scores);
        assertEquals(0.0f, result, 0.001f);
    }

    public void testCombineWithNullInput() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> combinationTechnique.combine(null));
        assertEquals("scores array cannot be null", exception.getMessage());
    }

    private float RRF(List<Float> scores, List<Double> weights) {
        float sumScores = 0.0f;
        float sumOfWeights = 0;

        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.size(); indexOfSubQuery++) {
            float score = scores.get(indexOfSubQuery);
            float weight = weights.get(indexOfSubQuery).floatValue();
            if (score >= 0.0) {
                score = score * weight;
                sumScores += score;
                sumOfWeights += weight;
            }
        }
        if (sumOfWeights == 0.0f) {
            return 0.0f;
        }

        return sumScores;
    }
}
