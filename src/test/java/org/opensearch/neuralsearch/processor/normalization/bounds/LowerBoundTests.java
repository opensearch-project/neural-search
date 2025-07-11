/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

public class LowerBoundTests extends OpenSearchTestCase {

    public void testConstructor_whenDefault_thenSuccessful() {
        LowerBound lowerBound = new LowerBound();
        assertFalse(lowerBound.enabled);
        assertEquals(BoundMode.DEFAULT, lowerBound.mode);
        assertEquals(LowerBound.DEFAULT_LOWER_BOUND_SCORE, lowerBound.boundScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testConstructor_whenNotDefault_thenSuccessful() {
        LowerBound lowerBound = new LowerBound(true, BoundMode.CLIP, 0.3f);
        assertTrue(lowerBound.enabled);
        assertEquals(BoundMode.CLIP, lowerBound.mode);
        assertEquals(0.3f, lowerBound.boundScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenLowerBoundDisabled_thenSuccessful() {
        LowerBound lowerBound = new LowerBound(false, BoundMode.APPLY, 0.3f);
        assertEquals(0.0f, lowerBound.determineEffectiveScore(0.5f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenApplyMode_thenSuccessful() {
        LowerBound lowerBound = new LowerBound(true, BoundMode.APPLY, 0.3f);

        // When score is above bound score and maxScore is above bound score
        assertEquals(0.3f, lowerBound.determineEffectiveScore(0.4f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);

        // When score is below bound score
        assertEquals(0.0f, lowerBound.determineEffectiveScore(0.2f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);

        // When maxScore is below bound score
        assertEquals(0.0f, lowerBound.determineEffectiveScore(0.4f, 0.0f, 0.2f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenClipMode_thenSuccessful() {
        LowerBound lowerBound = new LowerBound(true, BoundMode.CLIP, 0.3f);

        // When maxScore is above bound score
        assertEquals(0.3f, lowerBound.determineEffectiveScore(0.5f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);

        // When maxScore is below bound score
        assertEquals(0.0f, lowerBound.determineEffectiveScore(0.5f, 0.0f, 0.2f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenIgnoreMode_thenSuccessful() {
        LowerBound lowerBound = new LowerBound(true, BoundMode.IGNORE, 0.3f);
        assertEquals(0.0f, lowerBound.determineEffectiveScore(0.5f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testShouldClipToBound_whenClipMode_thenSuccessful() {
        LowerBound clipBound = new LowerBound(true, BoundMode.CLIP, 0.3f);
        assertTrue(clipBound.shouldClipToBound(0.2f, 0.3f));
        assertFalse(clipBound.shouldClipToBound(0.4f, 0.3f));
    }

    public void testShouldClipToBound_whenDisabled_thenSuccessful() {
        LowerBound disabledBound = new LowerBound(false, BoundMode.CLIP, 0.3f);
        assertFalse(disabledBound.shouldClipToBound(0.2f, 0.3f));
    }

    public void testShouldClipToBound_whenNonClipMode_thenSuccessful() {
        LowerBound applyBound = new LowerBound(true, BoundMode.APPLY, 0.3f);
        assertFalse(applyBound.shouldClipToBound(0.2f, 0.3f));
    }
}
