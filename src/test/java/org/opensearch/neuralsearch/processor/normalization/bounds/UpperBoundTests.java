/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization.bounds;

import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

public class UpperBoundTests extends OpenSearchTestCase {
    public void testConstructor_whenDefault_thenSuccessful() {
        UpperBound upperBound = new UpperBound();
        assertFalse(upperBound.enabled);
        assertEquals(BoundMode.DEFAULT, upperBound.mode);
        assertEquals(UpperBound.DEFAULT_UPPER_BOUND_SCORE, upperBound.boundScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testConstructor_whenNotDefault_thenSuccessful() {
        UpperBound upperBound = new UpperBound(true, BoundMode.CLIP, 0.7f);
        assertTrue(upperBound.enabled);
        assertEquals(BoundMode.CLIP, upperBound.mode);
        assertEquals(0.7f, upperBound.boundScore, DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenDisabled_thenSuccessful() {
        UpperBound upperBound = new UpperBound(false, BoundMode.APPLY, 0.7f);
        assertEquals(1.0f, upperBound.determineEffectiveScore(0.5f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenApplyMode_thenSuccessful() {
        UpperBound upperBound = new UpperBound(true, BoundMode.APPLY, 0.7f);

        // When score is below bound score and minScore is below bound score
        assertEquals(0.7f, upperBound.determineEffectiveScore(0.6f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);

        // When score is above bound score
        assertEquals(1.0f, upperBound.determineEffectiveScore(0.8f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);

        // When minScore is above bound score
        assertEquals(1.0f, upperBound.determineEffectiveScore(0.6f, 0.8f, 1.0f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenClipMode_thenSuccessful() {
        UpperBound upperBound = new UpperBound(true, BoundMode.CLIP, 0.7f);

        // When minScore is below bound score
        assertEquals(0.7f, upperBound.determineEffectiveScore(0.5f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);

        // When minScore is above bound score
        assertEquals(1.0f, upperBound.determineEffectiveScore(0.5f, 0.8f, 1.0f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testDetermineEffectiveScore_whenIgnoreMode_thenSuccessful() {
        UpperBound upperBound = new UpperBound(true, BoundMode.IGNORE, 0.7f);
        assertEquals(1.0f, upperBound.determineEffectiveScore(0.5f, 0.0f, 1.0f), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testShouldClipToBound_whenClipMode_thenSuccessful() {
        UpperBound clipBound = new UpperBound(true, BoundMode.CLIP, 0.7f);
        assertTrue(clipBound.shouldClipToBound(0.8f, 0.7f));
        assertFalse(clipBound.shouldClipToBound(0.6f, 0.7f));
    }

    public void testShouldClipToBound_whenDisabled_thenSuccessful() {
        UpperBound disabledBound = new UpperBound(false, BoundMode.CLIP, 0.7f);
        assertFalse(disabledBound.shouldClipToBound(0.8f, 0.7f));
    }

    public void testShouldClipToBound_whenNonClipMode_thenSuccessful() {
        UpperBound applyBound = new UpperBound(true, BoundMode.APPLY, 0.7f);
        assertFalse(applyBound.shouldClipToBound(0.8f, 0.7f));
    }
}
