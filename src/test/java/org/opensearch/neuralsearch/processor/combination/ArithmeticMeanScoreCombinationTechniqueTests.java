/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import static org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique.PARAM_NAME_WEIGHTS;

import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class ArithmeticMeanScoreCombinationTechniqueTests extends OpenSearchTestCase {

    private static final float DELTA_FOR_ASSERTION = 0.0001f;

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ArithmeticMeanScoreCombinationTechnique combinationTechnique = new ArithmeticMeanScoreCombinationTechnique(Map.of());
        float[] scores = { 1.0f, 0.5f, 0.3f };
        float actualScore = combinationTechnique.combine(scores);
        assertEquals(0.6f, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ArithmeticMeanScoreCombinationTechnique combinationTechnique = new ArithmeticMeanScoreCombinationTechnique(Map.of());
        float[] scores = { 1.0f, -1.0f, 0.6f };
        float actualScore = combinationTechnique.combine(scores);
        assertEquals(0.8f, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        ArithmeticMeanScoreCombinationTechnique combinationTechnique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, List.of(0.9, 0.2, 0.7))
        );
        float[] scores = { 1.0f, 0.5f, 0.3f };
        float actualScore = combinationTechnique.combine(scores);
        assertEquals(0.6722f, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        ArithmeticMeanScoreCombinationTechnique combinationTechnique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, List.of(0.9, 0.15, 0.7))
        );
        float[] scores = { 1.0f, -1.0f, 0.6f };
        float actualScore = combinationTechnique.combine(scores);
        assertEquals(0.825f, actualScore, DELTA_FOR_ASSERTION);
    }
}
