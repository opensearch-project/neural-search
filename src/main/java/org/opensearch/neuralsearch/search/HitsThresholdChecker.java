/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Locale;

import org.apache.lucene.search.ScoreMode;

import lombok.Getter;

/**
 *  Abstracts algorithm that allows early termination for the search flow if number of hits reached
 *  certain treshold
 */
public class HitsThresholdChecker {
    private int hitCount;
    @Getter
    private final int totalHitsThreshold;

    public HitsThresholdChecker(int totalHitsThreshold) {
        if (totalHitsThreshold < 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "totalHitsThreshold must be >= 0, got %d", totalHitsThreshold));
        }
        this.totalHitsThreshold = totalHitsThreshold;
    }

    public void incrementHitCount() {
        ++hitCount;
    }

    public boolean isThresholdReached() {
        return hitCount >= getTotalHitsThreshold();
    }

    public ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
    }
}
