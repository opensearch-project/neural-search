/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.Collection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Stateless L2 score-normalization arithmetic, shared by the classic shard-side hybrid path
 * ({@link L2ScoreNormalizationTechnique}, which iterates {@code CompoundTopDocs}) and the resolver (fused) mode's
 * coordinator path (which works on an {@code _id}-keyed per-leg score view). Extracting the scalar math here — rather
 * than reimplementing it per path — is what guarantees the two produce identical fused scores for the same hit set;
 * any classic-vs-resolver difference can then only be the hit set, never a second copy of the formula.
 *
 * <p>Formula is exactly the one classic hybrid uses:
 * {@code n_score_i = score_i / sqrt(score1^2 + score2^2 + ... + scoren^2)}, with the zero-norm case preserved bit for
 * bit.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class L2ScoreNormalizer {

    public static final float MIN_SCORE = 0.0f;

    /**
     * Accumulates the sum of squares of one sub-query's (classic) or one leg's (coordinator) raw scores and turns it into
     * an L2 norm.
     *
     * <p>The running sum is deliberately a {@code float}, matching what the classic path has always done. Widening it to
     * {@code double} would round differently and silently change every existing {@code l2} score. Note that {@code float}
     * addition is not associative, so the norm depends on accumulation order — classic accumulates per shard while the
     * coordinator accumulates over the merged leg, which can differ in the last bit once more than one shard is involved.
     */
    public static final class NormAccumulator {

        private float sumOfSquares;

        public void add(final float score) {
            sumOfSquares += score * score;
        }

        public float norm() {
            return (float) Math.sqrt(sumOfSquares);
        }
    }

    /**
     * Single-pass convenience for callers that already hold every score for a sub-query or leg, such as the coordinator
     * path's per-leg score map.
     */
    public static float l2Norm(final Collection<Float> scores) {
        NormAccumulator accumulator = new NormAccumulator();
        for (float score : scores) {
            accumulator.add(score);
        }
        return accumulator.norm();
    }

    /**
     * Normalize a single raw score against its sub-query's L2 norm. This is the exact per-score computation classic hybrid
     * applies in {@code L2ScoreNormalizationTechnique#normalizeSingleScore}.
     */
    public static float normalizeSingleScore(final float score, final float l2Norm) {
        return l2Norm == 0 ? MIN_SCORE : score / l2Norm;
    }
}
