/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.util.stream.IntStream;

import org.apache.lucene.search.ScoreMode;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class HitsTresholdCheckerTests extends OpenSearchQueryTestCase {

    public void testTresholdReached_whenIncrementCount_thenTresholdReached() {
        HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.create(5);
        assertEquals(5, hitsThresholdChecker.getHitsThreshold());
        assertEquals(ScoreMode.TOP_SCORES, hitsThresholdChecker.scoreMode());
        assertFalse(hitsThresholdChecker.isThresholdReached());
        hitsThresholdChecker.incrementHitCount();
        assertFalse(hitsThresholdChecker.isThresholdReached());
        IntStream.rangeClosed(1, 5).forEach((checker) -> hitsThresholdChecker.incrementHitCount());
        assertTrue(hitsThresholdChecker.isThresholdReached());
    }
}
