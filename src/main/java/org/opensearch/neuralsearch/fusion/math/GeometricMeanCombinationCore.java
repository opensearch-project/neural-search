/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Objects;

/**
 * Weighted geometric mean of a document's positive normalized leg scores: {@code exp(sum(w_i * ln(s_i)) / sum(w_i))}.
 * Legs scoring zero or less are skipped because {@code ln} is undefined there, as in the classic technique.
 */
public final class GeometricMeanCombinationCore implements ScoreCombinationCore {

    private static final float ZERO_SCORE = 0.0f;

    private final CombinationWeights weights;

    public GeometricMeanCombinationCore(final CombinationWeights weights) {
        this.weights = Objects.requireNonNull(weights, "weights must not be null");
    }

    @Override
    public String techniqueName() {
        return FusionTechniqueNames.GEOMETRIC_MEAN;
    }

    @Override
    public CombinationWeights weights() {
        return weights;
    }

    @Override
    public float combine(final float[] normalizedScores) {
        Objects.requireNonNull(normalizedScores, "scores array cannot be null");
        weights.requireMatches(normalizedScores.length);
        float weightedLnSum = 0;
        float sumOfWeights = 0;
        for (int legIndex = 0; legIndex < normalizedScores.length; legIndex++) {
            float score = normalizedScores[legIndex];
            if (score <= 0) {
                continue;
            }
            float weight = weights.weightFor(legIndex);
            sumOfWeights += weight;
            weightedLnSum += weight * Math.log(score);
        }
        return sumOfWeights == 0 ? ZERO_SCORE : (float) Math.exp(weightedLnSum / sumOfWeights);
    }
}
