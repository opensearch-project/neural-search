/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

import lombok.Getter;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a boundary constraint for score normalization in the min-max score normalization technique.
 */
public abstract class ScoreBound {
    public static final float MIN_BOUND_SCORE = -10_000f;
    public static final float MAX_BOUND_SCORE = 10_000f;
    protected static final String PARAM_NAME_BOUND_MODE = "mode";

    protected final boolean enabled;
    @Getter
    protected final BoundMode mode;
    @Getter
    protected final float boundScore;

    protected ScoreBound(boolean enabled, BoundMode mode, float boundScore) {
        this.enabled = enabled;
        this.mode = mode;
        this.boundScore = boundScore;
    }

    /**
     * Determines the effective score based on the bound constraints.
     *
     * @param score    the current score to evaluate
     * @param minScore the minimum possible score in the range
     * @param maxScore the maximum possible score in the range
     * @return the effective score after applying bound constraints
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

    protected static BoundMode parseBoundMode(Map<String, Object> bound) {
        return BoundMode.fromString(Objects.toString(bound.get(PARAM_NAME_BOUND_MODE), ""));
    }

    protected static float parseBoundScore(Map<String, Object> bound, String scoreParamName, float defaultScore) {
        String scoreStr = Objects.toString(bound.get(scoreParamName), "");
        return scoreStr.isEmpty() ? defaultScore : Float.parseFloat(scoreStr);
    }
}
