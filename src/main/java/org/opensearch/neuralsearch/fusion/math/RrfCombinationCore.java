/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Objects;

/**
 * Reciprocal rank fusion's combination step: the weighted sum {@code sum(w_i * s_i)} of the per-leg reciprocal-rank
 * contributions, without dividing by the weights. A document absent from every leg scores zero.
 */
public final class RrfCombinationCore implements ScoreCombinationCore {

    private static final float ZERO_SCORE = 0.0f;

    private final CombinationWeights weights;

    public RrfCombinationCore(final CombinationWeights weights) {
        this.weights = Objects.requireNonNull(weights, "weights must not be null");
    }

    @Override
    public String techniqueName() {
        return FusionTechniqueNames.RRF_COMBINATION;
    }

    @Override
    public CombinationWeights weights() {
        return weights;
    }

    @Override
    public float combine(final float[] normalizedScores) {
        if (Objects.isNull(normalizedScores)) {
            throw new IllegalArgumentException("scores array cannot be null");
        }
        weights.requireMatches(normalizedScores.length);
        float sumScores = 0.0f;
        float sumOfWeights = 0;
        for (int legIndex = 0; legIndex < normalizedScores.length; legIndex++) {
            float score = normalizedScores[legIndex];
            if (score >= 0.0) {
                float weight = weights.weightFor(legIndex);
                score = score * weight;
                sumScores += score;
                sumOfWeights += weight;
            }
        }
        if (sumOfWeights == 0.0f) {
            return ZERO_SCORE;
        }
        return sumScores;
    }
}
