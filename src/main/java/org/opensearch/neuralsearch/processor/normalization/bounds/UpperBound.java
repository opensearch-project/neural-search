/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

public class UpperBound extends ScoreBound {
    public static final float DEFAULT_UPPER_BOUND_SCORE = 1.0f;

    public UpperBound() {
        this(false, BoundMode.DEFAULT, DEFAULT_UPPER_BOUND_SCORE);
    }

    public UpperBound(boolean enabled, BoundMode mode, float maxScore) {
        super(enabled, mode, maxScore);
    }

    @Override
    public float determineEffectiveScore(float score, float minScore, float maxScore) {
        if (enabled == false) {
            return maxScore;
        }

        return switch (mode) {
            case APPLY -> (minScore < boundScore && score < boundScore) ? boundScore : maxScore;
            case CLIP -> minScore > boundScore ? maxScore : boundScore;
            case IGNORE -> maxScore;
        };
    }

    @Override
    public boolean shouldClipToBound(float score, float effectiveScore) {
        return enabled && mode == BoundMode.CLIP && score > effectiveScore;
    }
}
