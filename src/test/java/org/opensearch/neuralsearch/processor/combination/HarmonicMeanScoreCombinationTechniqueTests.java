/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.processor.combination;

import static org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique.PARAM_NAME_WEIGHTS;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HarmonicMeanScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    private ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public HarmonicMeanScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = (scores, weights) -> harmonicMean(scores, weights);
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, 0.5f, 0.3f);
        List<Double> weights = List.of(0.45, 0.15, 0.4);
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        float expectedScore = 0.48f;
        testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, 0.0f, 0.6f);
        List<Double> weights = List.of(0.45, 0.15, 0.4);
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        float expectedScore = 0.7611f;
        testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = IntStream.range(0, RANDOM_SCORES_SIZE).mapToObj(i -> 1.0 / RANDOM_SCORES_SIZE).collect(Collectors.toList());
        ScoreCombinationTechnique technique = new HarmonicMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
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
