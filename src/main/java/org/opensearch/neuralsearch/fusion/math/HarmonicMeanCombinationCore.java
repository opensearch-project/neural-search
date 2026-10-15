/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Objects;

/**
 * Weighted harmonic mean of a document's positive normalized leg scores: {@code sum(w_i) / sum(w_i / s_i)}. Legs scoring
 * zero or less are skipped, as in the classic technique.
 */
public final class HarmonicMeanCombinationCore implements ScoreCombinationCore {

    private static final float ZERO_SCORE = 0.0f;

    private final CombinationWeights weights;

    public HarmonicMeanCombinationCore(final CombinationWeights weights) {
        this.weights = Objects.requireNonNull(weights, "weights must not be null");
    }

    @Override
    public String techniqueName() {
        return FusionTechniqueNames.HARMONIC_MEAN;
    }

    @Override
    public CombinationWeights weights() {
        return weights;
    }

    @Override
    public float combine(final float[] normalizedScores) {
        Objects.requireNonNull(normalizedScores, "scores array cannot be null");
        weights.requireMatches(normalizedScores.length);
        float sumOfWeights = 0;
        float sumOfHarmonics = 0;
        for (int legIndex = 0; legIndex < normalizedScores.length; legIndex++) {
            float score = normalizedScores[legIndex];
            if (score <= 0) {
                continue;
            }
            float weight = weights.weightFor(legIndex);
            sumOfWeights += weight;
            sumOfHarmonics += weight / score;
        }
        return sumOfHarmonics > 0 ? sumOfWeights / sumOfHarmonics : ZERO_SCORE;
    }
}
