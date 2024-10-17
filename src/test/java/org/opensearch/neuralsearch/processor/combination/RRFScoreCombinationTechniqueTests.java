/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;

public class RRFScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    private ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public RRFScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = (scores, weights) -> RRF(scores, weights);
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new RRFScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    private float RRF(List<Float> scores, List<Double> weights) {
        float sumScores = 0.0f;
        for (float score : scores) {
            sumScores += score;
        }
        return sumScores;
    }
}
