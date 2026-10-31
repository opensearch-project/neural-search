/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.normalization.bounds.BoundMode;
import org.opensearch.neuralsearch.processor.normalization.bounds.LowerBound;
import org.opensearch.neuralsearch.processor.normalization.bounds.UpperBound;
import org.opensearch.test.OpenSearchTestCase;

public class MinMaxScoreNormalizerTests extends OpenSearchTestCase {

    private static final float DELTA = 1e-6f;

    public void testNormalizeSingleScore_unbounded() {
        // Plain min_max: (score - min) / (max - min).
        assertEquals(0.5f, MinMaxScoreNormalizer.normalizeSingleScore(1.5f, 1.0f, 2.0f), DELTA);
    }

    public void testNormalizeSingleScore_singleScoreEdgeCase_returnsOne() {
        // min == max == score → single-result edge case returns 1.0.
        assertEquals(1.0f, MinMaxScoreNormalizer.normalizeSingleScore(0.7f, 0.7f, 0.7f), DELTA);
    }

    public void testNormalizeSingleScore_zeroResultFloorsToMinScore() {
        // A normalized 0.0 is floored to MIN_SCORE (0.001) so a matched doc never scores exactly zero.
        assertEquals(MinMaxScoreNormalizer.MIN_SCORE, MinMaxScoreNormalizer.normalizeSingleScore(1.0f, 1.0f, 2.0f), DELTA);
    }

    public void testNormalizeSingleScore_lowerBoundClip_returnsMinScore() {
        // Enabled CLIP lower bound above the score → clipped to MIN_SCORE.
        LowerBound clipLower = new LowerBound(true, BoundMode.CLIP, 5.0f);
        UpperBound defaultUpper = new UpperBound();
        float result = MinMaxScoreNormalizer.normalizeSingleScore(1.0f, 0.0f, 10.0f, clipLower, defaultUpper);
        assertEquals(MinMaxScoreNormalizer.MIN_SCORE, result, DELTA);
    }

    public void testNormalizeSingleScore_upperBoundClip_returnsMaxScore() {
        // Enabled CLIP upper bound below the score → clipped to MAX_SCORE.
        LowerBound defaultLower = new LowerBound();
        UpperBound clipUpper = new UpperBound(true, BoundMode.CLIP, 5.0f);
        float result = MinMaxScoreNormalizer.normalizeSingleScore(9.0f, 0.0f, 10.0f, defaultLower, clipUpper);
        assertEquals(MinMaxScoreNormalizer.MAX_SCORE, result, DELTA);
    }

    public void testCalculateNormalizedScore_equalEffectiveBounds_returnsOne() {
        assertEquals(1.0f, MinMaxScoreNormalizer.calculateNormalizedScore(0.5f, 0.7f, 0.7f), DELTA);
    }
}
