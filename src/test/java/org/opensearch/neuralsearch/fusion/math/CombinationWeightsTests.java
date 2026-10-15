/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

public class CombinationWeightsTests extends OpenSearchTestCase {

    public void testUnweighted() {
        assertSame(CombinationWeights.UNWEIGHTED, CombinationWeights.of((List<Float>) null));
        assertSame(CombinationWeights.UNWEIGHTED, CombinationWeights.of(List.of()));
        assertSame(CombinationWeights.UNWEIGHTED, CombinationWeights.of((float[]) null));
        assertSame(CombinationWeights.UNWEIGHTED, CombinationWeights.of(new float[0]));
        assertTrue(CombinationWeights.UNWEIGHTED.isEmpty());
        assertEquals(0, CombinationWeights.UNWEIGHTED.size());
        assertEquals(CombinationWeights.DEFAULT_WEIGHT, CombinationWeights.UNWEIGHTED.weightFor(randomIntBetween(0, 10)), 0.0f);
        // any leg count matches no weights
        CombinationWeights.UNWEIGHTED.requireMatches(randomIntBetween(1, 10));
    }

    public void testWeighted() {
        CombinationWeights weights = CombinationWeights.of(new float[] { 0.25f, 0.75f });
        assertEquals(List.of(0.25f, 0.75f), weights.asList());
        assertEquals(2, weights.size());
        assertEquals(0.25f, weights.weightFor(0), 0.0f);
        assertEquals(0.75f, weights.weightFor(1), 0.0f);
        // beyond the given weights a leg weighs the default, as the classic helper always did
        assertEquals(CombinationWeights.DEFAULT_WEIGHT, weights.weightFor(2), 0.0f);
        weights.requireMatches(2);
        assertEquals(weights, CombinationWeights.of(List.of(0.25f, 0.75f)));
        assertFalse(weights.equals(CombinationWeights.of(List.of(0.75f, 0.25f))));
        assertFalse(weights.equals(List.of(0.25f, 0.75f)));
        assertEquals(weights.hashCode(), CombinationWeights.of(List.of(0.25f, 0.75f)).hashCode());
        assertEquals("[0.25, 0.75]", weights.toString());
    }

    public void testValidation_rangeMessage() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CombinationWeights.of(List.of(2.4f)));
        assertEquals("all weights must be in range [0.0 ... 1.0], submitted weights: [2.4]", e.getMessage());
    }

    public void testValidation_sumMessage() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CombinationWeights.of(List.of(0.4f, 0.5f, 0.6f)));
        assertEquals("sum of weights for combination must be equal to 1.0, submitted weights: [0.4, 0.5, 0.6]", e.getMessage());
        // within the classic tolerance passes
        CombinationWeights.of(List.of(0.33f, 0.33f, 0.33f));
    }

    public void testRequireMatches_message() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CombinationWeights.of(List.of(0.4f, 0.6f)).requireMatches(3)
        );
        assertEquals("number of weights [2] must match number of sub-queries [3] in hybrid query", e.getMessage());
        // the list helpers only check the count, never the range or the sum
        CombinationWeights.requireCountMatches(List.of(0.9f, 0.9f), 2);
        CombinationWeights.requireCountMatches(null, 5);
        assertEquals(0.9f, CombinationWeights.weightFor(List.of(0.9f, 0.9f), 1), 0.0f);
        assertEquals(CombinationWeights.DEFAULT_WEIGHT, CombinationWeights.weightFor(null, 0), 0.0f);
    }
}
