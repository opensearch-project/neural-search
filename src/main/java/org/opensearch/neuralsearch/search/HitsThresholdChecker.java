/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.util.Locale;

import org.apache.lucene.search.ScoreMode;

/**
 *  Defines algorithm to allow searches to terminate early
 */
public class HitsThresholdChecker {
    private int hitCount;
    private final int totalHitsThreshold;

    private HitsThresholdChecker(int totalHitsThreshold) {
        if (totalHitsThreshold < 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "totalHitsThreshold must be >= 0, got %d", totalHitsThreshold));
        }
        assert totalHitsThreshold != Integer.MAX_VALUE;
        this.totalHitsThreshold = totalHitsThreshold;
    }

    int getHitsThreshold() {
        return totalHitsThreshold;
    }

    void incrementHitCount() {
        ++hitCount;
    }

    boolean isThresholdReached() {
        return hitCount > getHitsThreshold();
    }

    ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
    }

    public static HitsThresholdChecker create(final int totalHitsThreshold) {
        return new HitsThresholdChecker(totalHitsThreshold);
    }
}
