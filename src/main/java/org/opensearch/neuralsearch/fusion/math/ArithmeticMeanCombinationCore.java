/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Objects;

/**
 * Weighted arithmetic mean of a document's normalized leg scores: {@code sum(w_i * s_i) / sum(w_i)} over the legs whose
 * score is non-negative. The arithmetic, including its single-precision accumulation, is the classic technique's.
 */
public final class ArithmeticMeanCombinationCore implements ScoreCombinationCore {

    private static final float ZERO_SCORE = 0.0f;

    private final CombinationWeights weights;

    public ArithmeticMeanCombinationCore(final CombinationWeights weights) {
        this.weights = Objects.requireNonNull(weights, "weights must not be null");
    }

    @Override
    public String techniqueName() {
        return FusionTechniqueNames.ARITHMETIC_MEAN;
    }

    @Override
    public CombinationWeights weights() {
        return weights;
    }

    @Override
    public float combine(final float[] normalizedScores) {
        Objects.requireNonNull(normalizedScores, "scores array cannot be null");
        weights.requireMatches(normalizedScores.length);
        float combinedScore = 0.0f;
        float sumOfWeights = 0;
        for (int legIndex = 0; legIndex < normalizedScores.length; legIndex++) {
            float score = normalizedScores[legIndex];
            if (score >= 0.0) {
                float weight = weights.weightFor(legIndex);
                score = score * weight;
                combinedScore += score;
                sumOfWeights += weight;
            }
        }
        if (sumOfWeights == 0.0f) {
            return ZERO_SCORE;
        }
        return combinedScore / sumOfWeights;
    }
}
