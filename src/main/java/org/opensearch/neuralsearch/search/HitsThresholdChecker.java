/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.util.Locale;

import lombok.Getter;

import org.apache.lucene.search.ScoreMode;

/**
 *  Abstracts algorithm that allows early termination for the search flow if number of hits reached
 *  certain treshold
 */
public class HitsThresholdChecker {
    private int hitCount;
    @Getter
    private final int totalHitsThreshold;

    private HitsThresholdChecker(int totalHitsThreshold) {
        if (totalHitsThreshold < 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "totalHitsThreshold must be >= 0, got %d", totalHitsThreshold));
        }
        if (totalHitsThreshold == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "totalHitsThreshold must be less than max integer value"));
        }
        this.totalHitsThreshold = totalHitsThreshold;
    }

    void incrementHitCount() {
        ++hitCount;
    }

    boolean isThresholdReached() {
        return hitCount > getTotalHitsThreshold();
    }

    ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
    }

    public static HitsThresholdChecker create(final int totalHitsThreshold) {
        return new HitsThresholdChecker(totalHitsThreshold);
    }
}
