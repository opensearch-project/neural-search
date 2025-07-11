/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

/**
 * Represents a lower boundary constraint for score normalization in the min-max score normalization technique.
 */
public class LowerBound extends ScoreBound {
    public static final float DEFAULT_LOWER_BOUND_SCORE = 0.0f;

    /**
     * Constructs a default LowerBound instance.
     * Initializes with disabled state, default bound mode, and default lower bound score.
     */
    public LowerBound() {
        this(false, BoundMode.DEFAULT, DEFAULT_LOWER_BOUND_SCORE);
    }

    /**
     * Constructs a LowerBound instance with specified parameters.
     *
     * @param enabled   whether the lower bound constraint is enabled
     * @param mode      the boundary mode to determine how the bound is applied
     * @param boundScore  the score value to use as the lower bound
     */
    public LowerBound(boolean enabled, BoundMode mode, float boundScore) {
        super(enabled, mode, boundScore);
    }

    /**
     * Determines the effective score based on the lower bound constraints.
     *
     * @param score    the current score to evaluate
     * @param minScore the minimum possible score in the range
     * @param maxScore the maximum possible score in the range
     * @return the effective score after applying lower bound constraints
     */
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

    /**
     * Determines whether the score should be clipped to the bound value.
     *
     * @param score           the current score to evaluate
     * @param effectiveScore  the calculated effective score
     * @return true if the score should be clipped to the bound value, false otherwise
     */
    @Override
    public boolean shouldClipToBound(float score, float effectiveScore) {
        return enabled && mode == BoundMode.CLIP && score < effectiveScore;
    }
}
