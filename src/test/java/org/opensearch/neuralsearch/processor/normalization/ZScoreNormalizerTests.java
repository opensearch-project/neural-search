/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.test.OpenSearchTestCase;

public class ZScoreNormalizerTests extends OpenSearchTestCase {

    private static final float DELTA = 1e-6f;

    public void testNormalizeSingleScore_plainZScore() {
        // Plain z_score: (score - mean) / standard_deviation.
        assertEquals(1.5f, ZScoreNormalizer.normalizeSingleScore(8.0f, 2.0f, 5.0f, 9.0f, 1.0f), DELTA);
    }

    public void testNormalizeSingleScore_scoreEqualsMean_returnsMax() {
        // mean == score → the sub-query max is returned, ahead of any division.
        assertEquals(9.0f, ZScoreNormalizer.normalizeSingleScore(5.0f, 2.0f, 5.0f, 9.0f, 1.0f), DELTA);
    }

    public void testNormalizeSingleScore_zeroStandardDeviation_returnsMin() {
        // Reachable only when the score differs from the mean while the standard deviation is 0, which no realistic score
        // distribution produces — asserted here so the branch is pinned regardless.
        assertEquals(1.0f, ZScoreNormalizer.normalizeSingleScore(8.0f, 0.0f, 5.0f, 9.0f, 1.0f), DELTA);
    }

    public void testNormalizeSingleScore_negativeZScoreFloorsToMinScore() {
        // A score below the mean normalizes negative, and is floored to MIN_SCORE (0.001) rather than kept negative.
        assertEquals(ZScoreNormalizer.MIN_SCORE, ZScoreNormalizer.normalizeSingleScore(2.0f, 2.0f, 5.0f, 9.0f, 1.0f), DELTA);
    }

    public void testStatsAccumulator_exposesMeanStandardDeviationMaxAndMin() {
        ZScoreNormalizer.StatsAccumulator accumulator = new ZScoreNormalizer.StatsAccumulator();
        for (float score : new float[] { 2.0f, 7.0f, 8.0f }) {
            accumulator.add(score);
        }

        // Mean 5.667. DescriptiveStatistics#getStandardDeviation is the sample (n-1, bias-corrected) standard deviation,
        // not the population (n) one: for {2, 7, 8} that is sqrt(20.667/2) = 3.215, where population would give
        // sqrt(20.667/3) = 2.625. z_score's behaviour inherits that convention, so it is pinned here.
        assertEquals(5.6666665f, accumulator.mean(), DELTA);
        assertEquals(3.2145503f, accumulator.standardDeviation(), DELTA);
        assertEquals(8.0f, accumulator.max(), DELTA);
        assertEquals(2.0f, accumulator.min(), DELTA);
    }

    public void testStatsAccumulator_singleValue_hasZeroStandardDeviation() {
        ZScoreNormalizer.StatsAccumulator accumulator = new ZScoreNormalizer.StatsAccumulator();
        accumulator.add(4.0f);

        assertEquals(4.0f, accumulator.mean(), DELTA);
        assertEquals(0.0f, accumulator.standardDeviation(), DELTA);
        assertEquals(4.0f, accumulator.max(), DELTA);
        assertEquals(4.0f, accumulator.min(), DELTA);
    }

    public void testStatsAccumulator_noValues_yieldsNaN() {
        // A sub-query with no hits on any shard: nothing downstream reads these, but they must not throw.
        ZScoreNormalizer.StatsAccumulator accumulator = new ZScoreNormalizer.StatsAccumulator();

        assertTrue(Float.isNaN(accumulator.mean()));
        assertTrue(Float.isNaN(accumulator.standardDeviation()));
        assertTrue(Float.isNaN(accumulator.max()));
        assertTrue(Float.isNaN(accumulator.min()));
    }
}
