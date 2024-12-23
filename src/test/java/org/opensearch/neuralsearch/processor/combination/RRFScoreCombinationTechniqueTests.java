/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;

public class RRFScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    private static final int RANK_CONSTANT = 60;
    private RRFScoreCombinationTechnique combinationTechnique;

    public RRFScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = (scores, weights) -> RRF(scores, weights);
        combinationTechnique = new RRFScoreCombinationTechnique();
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique();
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique();
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
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
        for (float score : scores) {
            sumScores += score;
        }
        return sumScores;
    }
}
