/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

public class LowerBound extends ScoreBound {
    public static final float DEFAULT_LOWER_BOUND_SCORE = 0.0f;

    public LowerBound() {
        this(false, BoundMode.DEFAULT, DEFAULT_LOWER_BOUND_SCORE);
    }

    public LowerBound(boolean enabled, BoundMode mode, float minScore) {
        super(enabled, mode, minScore);
    }

    @Override
    public float determineEffectiveScore(float score, float minScore, float maxScore) {
        if (enabled == false) {
            return minScore;
        }

        return switch (mode) {
            case APPLY -> (maxScore > boundScore && score > boundScore) ? boundScore : minScore;
            case CLIP -> maxScore < boundScore ? minScore : boundScore;
            case IGNORE -> minScore;
        };
    }

    @Override
    public boolean shouldClipToBound(float score, float effectiveScore) {
        return enabled && mode == BoundMode.CLIP && score < effectiveScore;
    }
}
