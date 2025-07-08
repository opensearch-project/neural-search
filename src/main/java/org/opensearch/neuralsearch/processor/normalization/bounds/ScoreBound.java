/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

/**
 * Represents a boundary constraint for score normalization in the min-max score normalization technique.
 */
public abstract class ScoreBound {
    public static final float MIN_BOUND_SCORE = -10_000f;
    public static final float MAX_BOUND_SCORE = 10_000f;

    protected final boolean enabled;
    protected final BoundMode mode;
    protected final float boundScore;

    protected ScoreBound(boolean enabled, BoundMode mode, float boundScore) {
        this.enabled = enabled;
        this.mode = mode;
        this.boundScore = boundScore;
    }

    /**
     * Determines the effective score based on the upper bound constraints.
     *
     * @param score    the current score to evaluate
     * @param minScore the minimum possible score in the range
     * @param maxScore the maximum possible score in the range
     * @return the effective score after applying upper bound constraints
     */
    public abstract float determineEffectiveScore(float score, float minScore, float maxScore);

    /**
     * Determines whether the score should be clipped to the bound value.
     *
     * @param score           the current score to evaluate
     * @param effectiveScore  the calculated effective score
     * @return true if the score should be clipped to the bound value, false otherwise
     */
    public abstract boolean shouldClipToBound(float score, float effectiveScore);
}
