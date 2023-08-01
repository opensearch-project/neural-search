/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import static org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique.PARAM_NAME_WEIGHTS;

import java.util.List;
import java.util.Map;

public class ArithmeticMeanScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    public ArithmeticMeanScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = this::arithmeticMean;
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = List.of(0.9, 0.2, 0.7);
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            new ScoreCombinationUtil()
        );
        testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = List.of(0.9, 0.2, 0.7);
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            new ScoreCombinationUtil()
        );
        testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    private float arithmeticMean(List<Float> scores, List<Double> weights) {
        assertEquals(scores.size(), weights.size());
        float sumOfWeightedScores = 0;
        float sumOfWeights = 0;
        for (int i = 0; i < scores.size(); i++) {
            float score = scores.get(i);
            float weight = weights.get(i).floatValue();
            if (score >= 0) {
                sumOfWeightedScores += score * weight;
                sumOfWeights += weight;
            }
        }
        return sumOfWeights == 0 ? 0f : sumOfWeightedScores / sumOfWeights;
    }
}
