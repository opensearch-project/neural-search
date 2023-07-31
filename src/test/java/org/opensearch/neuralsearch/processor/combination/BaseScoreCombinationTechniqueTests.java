/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import lombok.NoArgsConstructor;

import org.apache.commons.lang.ArrayUtils;
import org.opensearch.test.OpenSearchTestCase;

@NoArgsConstructor
public class BaseScoreCombinationTechniqueTests extends OpenSearchTestCase {

    protected BiFunction<List<Float>, List<Double>, Float> expectedScoreFunction;

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
        List<Double> weights
    ) {
        float[] scores = { 1.0f, 0.5f, 0.3f };
        float actualScore = technique.combine(scores);
        float expectedScore = expectedScoreFunction.apply(Arrays.asList(ArrayUtils.toObject(scores)), weights);
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }

    public void testLogic_whenNotAllScoresAndWeightsPresent_thenCorrectScores(
        final ScoreCombinationTechnique technique,
        List<Double> weights
    ) {
        float[] scores = { 1.0f, -1.0f, 0.6f };
        float actualScore = technique.combine(scores);
        float expectedScore = expectedScoreFunction.apply(Arrays.asList(ArrayUtils.toObject(scores)), weights);
        assertEquals(expectedScore, actualScore, DELTA_FOR_ASSERTION);
    }
}
