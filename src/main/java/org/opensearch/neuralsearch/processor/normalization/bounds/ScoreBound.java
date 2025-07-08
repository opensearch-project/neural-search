/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

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

    public abstract float determineEffectiveScore(float score, float minScore, float maxScore);

    public abstract boolean shouldClipToBound(float score, float effectiveScore);
}
