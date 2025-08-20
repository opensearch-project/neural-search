/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

import java.util.Map;

/**
 * Represents an upper boundary constraint for score normalization in the min-max score normalization technique.
 */
public class UpperBound extends ScoreBound {
    public static final float DEFAULT_UPPER_BOUND_SCORE = 1.0f;
    private static final String PARAM_NAME_UPPER_BOUND_MAX_SCORE = "max_score";

    /**
     * Constructs a default UpperBound instance.
     * Initializes with disabled state, default bound mode, and default upper bound score.
     */
    public UpperBound() {
        this(false, BoundMode.DEFAULT, DEFAULT_UPPER_BOUND_SCORE);
    }

    /**
     * Constructs an UpperBound instance with specified parameters.
     *
     * @param enabled   whether the upper bound constraint is enabled
     * @param mode      the boundary mode to determine how the bound is applied
     * @param boundScore  the score value to use as the upper bound
     */
    public UpperBound(boolean enabled, BoundMode mode, float boundScore) {
        super(enabled, mode, boundScore);
    }

    /**
     * Constructs an UpperBound instance from a map of parameters.
     *
     * @param upperBound the map containing the upper bound parameters
     */
    public UpperBound(Map<String, Object> upperBound) {
        this(true, parseBoundMode(upperBound), parseBoundScore(upperBound, PARAM_NAME_UPPER_BOUND_MAX_SCORE, DEFAULT_UPPER_BOUND_SCORE));
    }

    /**
     * Determines the effective score based on the upper bound constraints.
     *
     * @param score    the current score to evaluate
     * @param minScore the minimum possible score in the range
     * @param maxScore the maximum possible score in the range
     * @return the effective score after applying upper bound constraints
     */
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

    /**
     * Determines whether the score should be clipped to the bound value.
     *
     * @param score           the current score to evaluate
     * @param effectiveScore  the calculated effective score
     * @return true if the score should be clipped to the bound value, false otherwise
     */
    @Override
    public boolean shouldClipToBound(float score, float effectiveScore) {
        return enabled && mode == BoundMode.CLIP && score > effectiveScore;
    }
}
