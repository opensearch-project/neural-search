/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import static org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique.PARAM_NAME_WEIGHTS;

import java.util.List;
import java.util.Map;

public class HarmonicMeanScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    public HarmonicMeanScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = (scores, weights) -> harmonicMean(scores, weights);
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(Map.of(), new ScoreCombinationUtil());
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = List.of(0.9, 0.2, 0.7);
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            new ScoreCombinationUtil()
        );
        testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = List.of(0.9, 0.2, 0.7);
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            new ScoreCombinationUtil()
        );
        testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    private float harmonicMean(List<Float> scores, List<Double> weights) {
        assertEquals(scores.size(), weights.size());
        float w = 0, h = 0;
        for (int i = 0; i < scores.size(); i++) {
            float score = scores.get(i), weight = weights.get(i).floatValue();
            if (score > 0) {
                w += weight;
                h += weight / score;
            }
        }
        return h == 0 ? 0f : w / h;
    }
}
