/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.profile;

import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit coverage for the one value on this class that leaves the profile section.
 *
 * <p>Everything else here is rendered under the coordinator's profile entry and is pinned through that rendering in
 * {@link FusedLegProfileMergerTests}. {@code anyLegTimedOut()} is different: it sets the response's own {@code timed_out},
 * on unprofiled requests too, so it has to be readable without parsing the rendering — and it has to keep the same
 * only-ever-sets shape the response flag has.
 */
public class FusedCoordinatorTimingsTests extends OpenSearchTestCase {

    public void testAnyLegTimedOut_whenNoLegWasRecorded_thenFalse() {
        assertFalse(new FusedCoordinatorTimings().anyLegTimedOut());
    }

    public void testAnyLegTimedOut_whenEveryLegCompleted_thenFalse() {
        FusedCoordinatorTimings timings = new FusedCoordinatorTimings();
        timings.addLeg(0, 3L, 11, false);
        timings.addLeg(1, 5L, 11, false);
        assertFalse("a complete fan-out must not mark the answer incomplete", timings.anyLegTimedOut());
    }

    public void testAnyLegTimedOut_whenOneLegWasTruncated_thenTrue() {
        FusedCoordinatorTimings timings = new FusedCoordinatorTimings();
        timings.addLeg(0, 3L, 11, false);
        timings.addLeg(1, 5L, 4, true);
        assertTrue("one truncated leg narrows the window the whole ranking comes out of", timings.anyLegTimedOut());
    }

    /** A leg that finished cannot make an incomplete candidate set complete, whichever order the legs are recorded in. */
    public void testAnyLegTimedOut_whenACompletedLegFollowsATruncatedOne_thenItStaysTrue() {
        FusedCoordinatorTimings timings = new FusedCoordinatorTimings();
        timings.addLeg(0, 5L, 4, true);
        timings.addLeg(1, 3L, 11, false);
        assertTrue(timings.anyLegTimedOut());
    }

    /** The flag is derived from the same call that fills the rendering, so the two must never disagree. */
    public void testAnyLegTimedOut_whenALegIsRecorded_thenTheRenderingAgreesWithTheFlag() {
        FusedCoordinatorTimings timings = new FusedCoordinatorTimings();
        timings.addLeg(0, 5L, 4, true);

        assertEquals(List.of(Map.of("leg", 0, "took_in_millis", 5L, "hits", 4, "timed_out", true)), timings.legs());
        assertTrue(timings.anyLegTimedOut());
    }
}
