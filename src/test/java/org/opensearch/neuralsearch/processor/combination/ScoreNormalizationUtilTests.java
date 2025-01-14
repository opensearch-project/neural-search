/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import java.util.Map;

import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class ScoreNormalizationUtilTests extends OpenSearchQueryTestCase {

    public void testCombinationWeights_whenEmptyInputPassed_thenCreateEmptyWeightCollection() {
        ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();
        List<Float> weights = scoreCombinationUtil.getWeights(Map.of());
        assertNotNull(weights);
        assertTrue(weights.isEmpty());
    }

    public void testCombinationWeights_whenWeightsArePassed_thenSuccessful() {
        ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();
        List<Float> weights = scoreCombinationUtil.getWeights(Map.of("weights", List.of(0.4, 0.6)));
        assertNotNull(weights);
        assertEquals(2, weights.size());
        assertTrue(weights.containsAll(List.of(0.4f, 0.6f)));
    }

    public void testCombinationWeights_whenInvalidWeightsArePassed_thenFail() {
        ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();

        IllegalArgumentException exception1 = expectThrows(
            IllegalArgumentException.class,
            () -> scoreCombinationUtil.getWeights(Map.of("weights", List.of(2.4)))
        );
        assertTrue(exception1.getMessage().contains("all weights must be in range"));

        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> scoreCombinationUtil.getWeights(Map.of("weights", List.of(0.4, 0.5, 0.6)))
        );
        assertTrue(exception2.getMessage().contains("sum of weights for combination must be equal to 1.0"));
    }

    public void testWeightsValidation_whenNumberOfScoresDifferentFromNumberOfWeights_thenFail() {
        ScoreCombinationUtil scoreCombinationUtil = new ScoreCombinationUtil();
        IllegalArgumentException exception1 = expectThrows(
            IllegalArgumentException.class,
            () -> scoreCombinationUtil.validateIfWeightsMatchScores(new float[] { 0.6f, 0.5f }, List.of(0.4f, 0.2f, 0.4f))
        );
        org.hamcrest.MatcherAssert.assertThat(
            exception1.getMessage(),
            allOf(
                containsString("number of weights"),
                containsString("must match number of sub-queries"),
                containsString("in hybrid query")
            )
        );
    }
}
