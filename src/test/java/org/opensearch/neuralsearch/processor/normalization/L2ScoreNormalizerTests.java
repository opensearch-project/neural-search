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
        //
        // The fixture has to be one where float and double actually diverge in the final float norm, and the expectation
        // has to be a literal rather than the implementation's own expression, or the test asserts nothing. 2.0e-4f
        // squares to 4.0e-8, below half an ulp of 1.0f (5.96e-8), so a float sum swallows each of the 100 addends and
        // stays exactly 1.0f. A double sum keeps them, reaching 1.000004 and narrowing to 1.000002f, 17 bits away.
        L2ScoreNormalizer.NormAccumulator accumulator = new L2ScoreNormalizer.NormAccumulator();
        accumulator.add(1.0f);
        double sumOfSquaresInDouble = 1.0;
        for (int i = 0; i < 100; i++) {
            accumulator.add(2.0e-4f);
            sumOfSquaresInDouble += 2.0e-4f * 2.0e-4f;
        }

        assertEquals(Float.floatToIntBits(1.0f), Float.floatToIntBits(accumulator.norm()));
        // Guard the fixture itself: if this ever stops holding, the assertion above no longer tells float from double.
        assertNotEquals(Float.floatToIntBits(1.0f), Float.floatToIntBits((float) Math.sqrt(sumOfSquaresInDouble)));
    }
}
