/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.stream.IntStream;

import org.apache.lucene.search.ScoreMode;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class HitsThresholdCheckerTests extends OpenSearchQueryTestCase {

    public void testThresholdReached_whenIncrementCount_thenThresholdReached() {
        HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(5);
        assertEquals(5, hitsThresholdChecker.getTotalHitsThreshold());
        assertEquals(ScoreMode.TOP_SCORES, hitsThresholdChecker.scoreMode());
        assertFalse(hitsThresholdChecker.isThresholdReached());
        hitsThresholdChecker.incrementHitCount();
        assertFalse(hitsThresholdChecker.isThresholdReached());
        IntStream.rangeClosed(1, 5).forEach((checker) -> hitsThresholdChecker.incrementHitCount());
        assertTrue(hitsThresholdChecker.isThresholdReached());
    }

    public void testThresholdReached_whenHitCountEqualsThreshold_thenNotReached() {
        HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(5);
        IntStream.rangeClosed(1, 5).forEach((checker) -> hitsThresholdChecker.incrementHitCount());
        // a count that lands exactly on the threshold is still exact
        assertFalse(hitsThresholdChecker.isThresholdReached());
        hitsThresholdChecker.incrementHitCount();
        // only a count above the threshold means more hits may exist than were counted
        assertTrue(hitsThresholdChecker.isThresholdReached());
    }

    public void testThresholdReached_whenThresholdZero_thenReachedAfterFirstHit() {
        HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(0);
        assertFalse(hitsThresholdChecker.isThresholdReached());
        hitsThresholdChecker.incrementHitCount();
        assertTrue(hitsThresholdChecker.isThresholdReached());
    }

    public void testThresholdLimit_whenThresholdNegative_thenFail() {
        expectThrows(IllegalArgumentException.class, () -> new HitsThresholdChecker(-1));
    }

    public void testTrackThreshold_whenTrackThresholdSet_thenSuccessful() {
        HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(Integer.MAX_VALUE);
        assertEquals(ScoreMode.TOP_SCORES, hitsThresholdChecker.scoreMode());
        assertFalse(hitsThresholdChecker.isThresholdReached());
        hitsThresholdChecker.incrementHitCount();
        assertFalse(hitsThresholdChecker.isThresholdReached());
        IntStream.rangeClosed(1, 5).forEach((checker) -> hitsThresholdChecker.incrementHitCount());
        assertFalse(hitsThresholdChecker.isThresholdReached());
    }
}
