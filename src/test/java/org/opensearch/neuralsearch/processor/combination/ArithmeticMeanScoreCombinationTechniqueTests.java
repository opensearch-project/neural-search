/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import static org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique.PARAM_NAME_WEIGHTS;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class ArithmeticMeanScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    private ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public ArithmeticMeanScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = this::arithmeticMean;
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, 0.5f, 0.3f);
        List<Double> weights = List.of(0.9, 0.2, 0.7);
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        float expectedScore = 0.6722f;
        testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testRandomValues_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = IntStream.range(0, RANDOM_SCORES_SIZE)
            .mapToObj(i -> RandomizedTest.randomDouble())
            .collect(Collectors.toList());
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        testRandomValues_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, -1.0f, 0.6f);
        List<Double> weights = List.of(0.9, 0.2, 0.7);
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        float expectedScore = 0.825f;
        testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = IntStream.range(0, RANDOM_SCORES_SIZE)
            .mapToObj(i -> RandomizedTest.randomDouble())
            .collect(Collectors.toList());
        ScoreCombinationTechnique technique = new ArithmeticMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
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
