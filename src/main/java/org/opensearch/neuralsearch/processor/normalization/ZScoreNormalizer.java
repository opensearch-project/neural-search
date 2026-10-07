/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.primitives.Floats;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Stateless z-score normalization arithmetic, shared by the classic shard-side hybrid path
 * ({@link ZScoreNormalizationTechnique}, which iterates {@code CompoundTopDocs}) and the resolver (fused) mode's
 * coordinator path (which works on an {@code _id}-keyed per-leg score view). Extracting the scalar math here — rather
 * than reimplementing it per path — is what guarantees the two produce identical fused scores for the same hit set;
 * any classic-vs-resolver difference can then only be the hit set, never a second copy of the formula.
 *
 * <p>Formula is exactly the one classic hybrid uses: {@code nscore = (score - mean) / standard_deviation}, with the
 * equal-to-mean edge case, the zero-standard-deviation case, and the {@code 0.001} floor preserved bit for bit.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ZScoreNormalizer {

    public static final float MIN_SCORE = 0.001f;

    /**
     * Accumulates the raw scores of one sub-query (classic) or one leg (coordinator) and exposes the four statistics
     * z-score needs. Both paths feed values through this so they share not just the final formula but the <i>definition</i>
     * of mean/standard deviation/max/min, including the narrowing from {@code double} to {@code float}.
     *
     * <p>Values are accumulated one at a time so the classic path can keep feeding scores as it walks
     * {@code CompoundTopDocs}, without materializing an intermediate collection.
     */
    public static final class StatsAccumulator {

        private final DescriptiveStatistics statistics = new DescriptiveStatistics();

        public void add(final float score) {
            statistics.addValue(score);
        }

        public float mean() {
            return (float) statistics.getMean();
        }

        public float standardDeviation() {
            return (float) statistics.getStandardDeviation();
        }

        public float max() {
            return (float) statistics.getMax();
        }

        public float min() {
            return (float) statistics.getMin();
        }
    }

    /**
     * Normalize a single raw score against its sub-query's statistics. This is the exact per-score computation classic
     * hybrid applies in {@code ZScoreNormalizationTechnique#normalizeSingleScore}.
     */
    public static float normalizeSingleScore(
        final float score,
        final float standardDeviation,
        final float mean,
        final float maxScore,
        final float minScore
    ) {
        // edge case when there is only one score and z scores are same
        if (Floats.compare(mean, score) == 0) {
            return maxScore;
        }
        // Case when sd is 0
        if (Floats.compare(standardDeviation, 0.0f) == 0) {
            return minScore;
        }
        float normalizedScore = (score - mean) / standardDeviation;

        return normalizedScore <= 0.0f ? MIN_SCORE : normalizedScore;
    }
}
