/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.normalization.bounds.LowerBound;
import org.opensearch.neuralsearch.processor.normalization.bounds.UpperBound;

import com.google.common.primitives.Floats;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Stateless min-max score-normalization arithmetic, shared by the classic shard-side hybrid path
 * ({@link MinMaxScoreNormalizationTechnique}, which iterates {@code CompoundTopDocs}) and the resolver (fused) mode's
 * coordinator path (which works on an {@code _id}-keyed per-leg score view). Extracting the scalar math here — rather
 * than reimplementing it per path — is what guarantees the two produce identical fused scores for the same hit set;
 * any classic-vs-resolver difference can then only be the hit set, never a second copy of the formula.
 *
 * <p>Formula is exactly the one classic hybrid uses: {@code nscore = (score - min) / (max - min)}, with the
 * single-score edge case, lower/upper bound handling, and the {@code 0.001}/{@code 1.0} clipping preserved bit for bit.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MinMaxScoreNormalizer {

    public static final float MIN_SCORE = 0.001f;
    public static final float MAX_SCORE = 1.0f;
    private static final float SINGLE_RESULT_SCORE = 1.0f;

    /**
     * Normalize a single raw score against its sub-query's min/max, honoring the given bounds. This is the exact
     * per-score computation classic hybrid applies in {@code MinMaxScoreNormalizationTechnique#normalizeSingleScore}.
     */
    public static float normalizeSingleScore(
        final float score,
        final float minScore,
        final float maxScore,
        final LowerBound lowerBound,
        final UpperBound upperBound
    ) {
        // edge case when there is only one score and min and max scores are same
        if (isSingleScore(score, minScore, maxScore)) {
            return SINGLE_RESULT_SCORE;
        }

        float effectiveMinScore = lowerBound.determineEffectiveScore(score, minScore, maxScore);
        float effectiveMaxScore = upperBound.determineEffectiveScore(score, minScore, maxScore);

        if (lowerBound.shouldClipToBound(score, effectiveMinScore)) {
            return MIN_SCORE;
        }
        if (upperBound.shouldClipToBound(score, effectiveMaxScore)) {
            return MAX_SCORE;
        }

        return calculateNormalizedScore(score, effectiveMinScore, effectiveMaxScore);
    }

    /**
     * Unbounded convenience overload — normalize against a sub-query's min/max with default (disabled) bounds. Used by
     * the coordinator path, which does not (yet) carry per-leg bounds parameters.
     */
    public static float normalizeSingleScore(final float score, final float minScore, final float maxScore) {
        return normalizeSingleScore(score, minScore, maxScore, new LowerBound(), new UpperBound());
    }

    public static float calculateNormalizedScore(final float score, final float effectiveMinScore, final float effectiveMaxScore) {
        if (Floats.compare(effectiveMaxScore, effectiveMinScore) == 0) {
            return SINGLE_RESULT_SCORE;
        }

        float normalizedScore = (score - effectiveMinScore) / (effectiveMaxScore - effectiveMinScore);
        return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
    }

    private static boolean isSingleScore(final float score, final float minScore, final float maxScore) {
        return Floats.compare(maxScore, minScore) == 0 && Floats.compare(maxScore, score) == 0;
    }
}
