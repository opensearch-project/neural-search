/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class L2ScoreNormalizerTests extends OpenSearchTestCase {

    private static final float DELTA = 1e-6f;

    public void testNormalizeSingleScore_dividesByNorm() {
        // Plain l2: score / norm.
        assertEquals(0.6f, L2ScoreNormalizer.normalizeSingleScore(3.0f, 5.0f), DELTA);
    }

    public void testNormalizeSingleScore_zeroNorm_returnsMinScore() {
        // A whole sub-query of zero scores has a zero norm; guard against the division rather than producing NaN.
        assertEquals(L2ScoreNormalizer.MIN_SCORE, L2ScoreNormalizer.normalizeSingleScore(3.0f, 0.0f), DELTA);
    }

    public void testL2Norm_isRootOfSumOfSquares() {
        // 3^2 + 4^2 = 25, sqrt(25) = 5.
        assertEquals(5.0f, L2ScoreNormalizer.l2Norm(List.of(3.0f, 4.0f)), DELTA);
    }

    public void testL2Norm_emptyInput_isZero() {
        assertEquals(0.0f, L2ScoreNormalizer.l2Norm(List.of()), DELTA);
    }

    public void testNormAccumulator_matchesL2NormConvenience() {
        // The incremental accumulator (classic path, feeding scores as it walks CompoundTopDocs) and the collection
        // convenience (coordinator path) must agree bit for bit, not merely within a delta.
        List<Float> scores = List.of(0.5f, 0.25f, 1.75f, 0.125f);
        L2ScoreNormalizer.NormAccumulator accumulator = new L2ScoreNormalizer.NormAccumulator();
        for (float score : scores) {
            accumulator.add(score);
        }

        assertEquals(Float.floatToIntBits(L2ScoreNormalizer.l2Norm(scores)), Float.floatToIntBits(accumulator.norm()));
    }

    public void testNormAccumulator_accumulatesInFloat() {
        // The running sum is a float, matching what the classic path has always done. Summing in double would round
        // differently and silently change existing l2 scores, so this pins the narrower arithmetic.
        float score = 1.0f;
        float tiny = 1e-4f;
        L2ScoreNormalizer.NormAccumulator accumulator = new L2ScoreNormalizer.NormAccumulator();
        accumulator.add(score);
        accumulator.add(tiny);

        float expectedFloatSum = score * score + tiny * tiny;
        assertEquals(Float.floatToIntBits((float) Math.sqrt(expectedFloatSum)), Float.floatToIntBits(accumulator.norm()));
    }
}
