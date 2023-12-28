/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.search;

import java.util.stream.IntStream;

import org.apache.lucene.search.ScoreMode;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class HitsTresholdCheckerTests extends OpenSearchQueryTestCase {

    public void testTresholdReached_whenIncrementCount_thenTresholdReached() {
        HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(5);
        assertEquals(5, hitsThresholdChecker.getTotalHitsThreshold());
        assertEquals(ScoreMode.TOP_SCORES, hitsThresholdChecker.scoreMode());
        assertFalse(hitsThresholdChecker.isThresholdReached());
        hitsThresholdChecker.incrementHitCount();
        assertFalse(hitsThresholdChecker.isThresholdReached());
        IntStream.rangeClosed(1, 5).forEach((checker) -> hitsThresholdChecker.incrementHitCount());
        assertTrue(hitsThresholdChecker.isThresholdReached());
    }

    public void testTresholdLimit_whenThresholdNegative_thenFail() {
        expectThrows(IllegalArgumentException.class, () -> new HitsThresholdChecker(-1));
    }

    public void testTresholdLimit_whenThresholdMaxValue_thenFail() {
        expectThrows(IllegalArgumentException.class, () -> new HitsThresholdChecker(Integer.MAX_VALUE));
    }
}
