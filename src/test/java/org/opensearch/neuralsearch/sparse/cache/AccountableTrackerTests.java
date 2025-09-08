/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.opensearch.test.OpenSearchTestCase;

public class AccountableTrackerTests extends OpenSearchTestCase {

    private static class TestAccountableTracker extends AccountableTracker {
        // Concrete implementation for testing
    }

    public void testRecordUsedBytes() {
        TestAccountableTracker tracker = new TestAccountableTracker();

        tracker.recordUsedBytes(100L);
        assertEquals(100L, tracker.ramBytesUsed());

        tracker.recordUsedBytes(50L);
        assertEquals(150L, tracker.ramBytesUsed());
    }

    public void testRamBytesUsedInitiallyZero() {
        TestAccountableTracker tracker = new TestAccountableTracker();
        assertEquals(0L, tracker.ramBytesUsed());
    }

    public void testRecordNegativeBytes() {
        TestAccountableTracker tracker = new TestAccountableTracker();

        tracker.recordUsedBytes(100L);
        tracker.recordUsedBytes(-30L);
        assertEquals(70L, tracker.ramBytesUsed());
    }

    public void testRecordZeroBytes() {
        TestAccountableTracker tracker = new TestAccountableTracker();

        tracker.recordUsedBytes(0L);
        assertEquals(0L, tracker.ramBytesUsed());
    }
}
