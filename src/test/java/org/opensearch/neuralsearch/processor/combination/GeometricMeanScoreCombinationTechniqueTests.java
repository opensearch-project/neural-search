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

public class GeometricMeanScoreCombinationTechniqueTests extends BaseScoreCombinationTechniqueTests {

    private ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

    public GeometricMeanScoreCombinationTechniqueTests() {
        this.expectedScoreFunction = this::geometricMean;
    }

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new GeometricMeanScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores() {
        ScoreCombinationTechnique technique = new GeometricMeanScoreCombinationTechnique(Map.of(), scoreCombinationUtil);
        testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(technique);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, 0.5f, 0.3f);
        List<Double> weights = List.of(0.45, 0.15, 0.4);
        ScoreCombinationTechnique technique = new GeometricMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        float expectedScore = 0.5567f;
        testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testRandomValues_whenAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = IntStream.range(0, RANDOM_SCORES_SIZE).mapToObj(i -> 1.0 / RANDOM_SCORES_SIZE).collect(Collectors.toList());
        ScoreCombinationTechnique technique = new GeometricMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        testRandomValues_whenAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Float> scores = List.of(1.0f, 0.0f, 0.6f);
        List<Double> weights = List.of(0.45, 0.15, 0.4);
        ScoreCombinationTechnique technique = new GeometricMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        float expectedScore = 0.7863f;
        testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, scores, expectedScore);
    }

    public void testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores() {
        List<Double> weights = IntStream.range(0, RANDOM_SCORES_SIZE).mapToObj(i -> 1.0 / RANDOM_SCORES_SIZE).collect(Collectors.toList());
        ScoreCombinationTechnique technique = new GeometricMeanScoreCombinationTechnique(
            Map.of(PARAM_NAME_WEIGHTS, weights),
            scoreCombinationUtil
        );
        testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores(technique, weights);
    }

    /**
     * Verify score correctness by using alternative formula for geometric mean as n-th root of product of weighted scores,
     * more details in here https://en.wikipedia.org/wiki/Weighted_geometric_mean
     */
    private float geometricMean(List<Float> scores, List<Double> weights) {
        float product = 1.0f;
        float sumOfWeights = 0.0f;
        for (int indexOfSubQuery = 0; indexOfSubQuery < scores.size(); indexOfSubQuery++) {
            float score = scores.get(indexOfSubQuery);
            if (score <= 0) {
                // scores 0.0 need to be skipped, ln() of 0 is not defined
                continue;
            }
            float weight = weights.get(indexOfSubQuery).floatValue();
            product *= Math.pow(score, weight);
            sumOfWeights += weight;
        }
        return sumOfWeights == 0 ? 0f : (float) Math.pow(product, (float) 1 / sumOfWeights);
    }
}
