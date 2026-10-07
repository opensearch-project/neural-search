/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for the one rule this merger has: across every leg of every fused hybrid in a request, the signal is a
 * logical OR that only ever sets. "Some part of this answer is incomplete" is not something a leg that finished can take
 * back, so a completed leg — or a whole completed hybrid — must not be able to clear what another one reported.
 */
public class FusedLegTimeoutMergerTests extends OpenSearchTestCase {

    public void testAnyLegTimedOut_whenNothingWasPublished_thenFalse() {
        assertFalse("a request whose legs were never reached says nothing about a timeout", new FusedLegTimeoutMerger().anyLegTimedOut());
    }

    public void testConsumer_whenNoLegTimedOut_thenNothingIsRecorded() {
        FusedLegTimeoutMerger merger = new FusedLegTimeoutMerger();
        merger.consumer().accept(false);
        assertFalse("the normal case must leave the response's own flag alone", merger.anyLegTimedOut());
    }

    public void testConsumer_whenALegTimedOut_thenItIsRecorded() {
        FusedLegTimeoutMerger merger = new FusedLegTimeoutMerger();
        merger.consumer().accept(true);
        assertTrue(merger.anyLegTimedOut());
    }

    /** Two fused hybrids in one request are two handles off one merger, and the truncated one has to win. */
    public void testConsumer_whenOneHybridTimedOutAndAnotherDidNot_thenTheTruncationStands() {
        FusedLegTimeoutMerger merger = new FusedLegTimeoutMerger();
        FusedLegTimeoutMerger.LegTimeoutConsumer first = merger.consumer();
        FusedLegTimeoutMerger.LegTimeoutConsumer second = merger.consumer();

        first.accept(true);
        second.accept(false);

        assertTrue("a hybrid whose legs all completed cannot undo a sibling hybrid's truncation", merger.anyLegTimedOut());
    }

    /** Order must not matter: the same two publications the other way round have to reach the same answer. */
    public void testConsumer_whenTheCompletedHybridPublishesFirst_thenTheTruncationStillStands() {
        FusedLegTimeoutMerger merger = new FusedLegTimeoutMerger();
        merger.consumer().accept(false);
        merger.consumer().accept(true);
        assertTrue(merger.anyLegTimedOut());
    }

    /** Reading it is not consuming it: the response listener reads once, but a re-read must not answer differently. */
    public void testAnyLegTimedOut_whenReadTwice_thenItIsStillSet() {
        FusedLegTimeoutMerger merger = new FusedLegTimeoutMerger();
        merger.consumer().accept(true);
        assertTrue(merger.anyLegTimedOut());
        assertTrue(merger.anyLegTimedOut());
    }
}
