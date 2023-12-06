/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.commons.lang.ArrayUtils;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class BaseScoreCombinationTechniqueTests extends OpenSearchTestCase {

    protected BiFunction<List<Float>, List<Double>, Float> expectedScoreFunction;
    protected static final int RANDOM_SCORES_SIZE = 100;

    private static final float DELTA_FOR_ASSERTION = 0.0001f;

    public void testLogic_whenAllScoresPresentAndNoWeights_thenCorrectScores(final ScoreCombinationTechnique technique) {
        float[] scores = { 1.0f, 0.5f, 0.3f };
        float actualScore = technique.combine(scores);
        float expectedScore = expectedScoreFunction.apply(Arrays.asList(ArrayUtils.toObject(scores)), List.of(1.0, 1.0, 1.0));
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenNotAllScoresPresentAndNoWeights_thenCorrectScores(final ScoreCombinationTechnique technique) {
        float[] scores = { 1.0f, -1.0f, 0.6f };
        float actualScore = technique.combine(scores);
        float expectedScore = expectedScoreFunction.apply(Arrays.asList(ArrayUtils.toObject(scores)), List.of(1.0, 1.0, 1.0));
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenAllScoresAndWeightsPresent_thenCorrectScores(
        final ScoreCombinationTechnique technique,
        List<Float> scores,
        float expectedScore
    ) {
        float[] scoresArray = new float[scores.size()];
        for (int i = 0; i < scoresArray.length; i++) {
            scoresArray[i] = scores.get(i);
        }
        float actualScore = technique.combine(scoresArray);
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testRandomValues_whenAllScoresAndWeightsPresent_thenCorrectScores(
        final ScoreCombinationTechnique technique,
        final List<Double> weights
    ) {
        float[] scores = new float[weights.size()];
        for (int i = 0; i < RANDOM_SCORES_SIZE; i++) {
            scores[i] = randomScore();
        }
        float actualScore = technique.combine(scores);
        float expectedScore = expectedScoreFunction.apply(Arrays.asList(ArrayUtils.toObject(scores)), weights);
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(
        final ScoreCombinationTechnique technique,
        List<Float> scores,
        float expectedScore
    ) {
        float[] scoresArray = new float[scores.size()];
        for (int i = 0; i < scoresArray.length; i++) {
            scoresArray[i] = scores.get(i);
        }
        float actualScore = technique.combine(scoresArray);
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testRandomValues_whenNotAllScoresAndWeightsPresent_thenCorrectScores(
        final ScoreCombinationTechnique technique,
        final List<Double> weights
    ) {
        float[] scores = new float[weights.size()];
        for (int i = 0; i < RANDOM_SCORES_SIZE; i++) {
            scores[i] = randomScore();
        }
        float actualScore = technique.combine(scores);
        float expectedScore = expectedScoreFunction.apply(Arrays.asList(ArrayUtils.toObject(scores)), weights);
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    private float randomScore() {
        return RandomizedTest.randomBoolean() ? -1.0f : RandomizedTest.randomFloat();
    }
}
