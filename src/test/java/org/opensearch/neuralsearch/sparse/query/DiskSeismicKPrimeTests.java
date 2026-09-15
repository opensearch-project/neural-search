/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.opensearch.neuralsearch.sparse.query.DiskSeismicKPrime.K_PRIME_AT_UNIT_HEAP_FACTOR;
import static org.opensearch.neuralsearch.sparse.query.DiskSeismicKPrime.MIN_K_PRIME;

public class DiskSeismicKPrimeTests extends AbstractSparseTestBase {

    public void testUnitHeapFactor_mapsToBaseBudget() {
        assertEquals(K_PRIME_AT_UNIT_HEAP_FACTOR, DiskSeismicKPrime.fromHeapFactor(1.0f));
    }

    public void testStepsAboveUnit_addOneBlockPerHundredth() {
        assertEquals(21, DiskSeismicKPrime.fromHeapFactor(1.01f));
        assertEquals(22, DiskSeismicKPrime.fromHeapFactor(1.02f));
        assertEquals(30, DiskSeismicKPrime.fromHeapFactor(1.1f));
        assertEquals(70, DiskSeismicKPrime.fromHeapFactor(1.5f));
        assertEquals(120, DiskSeismicKPrime.fromHeapFactor(2.0f));
    }

    public void testStepsBelowUnit_removeOneBlockPerHundredth() {
        assertEquals(19, DiskSeismicKPrime.fromHeapFactor(0.99f));
        assertEquals(15, DiskSeismicKPrime.fromHeapFactor(0.95f));
        assertEquals(12, DiskSeismicKPrime.fromHeapFactor(0.92f));
    }

    public void testFloor_clampsToMinKPrime() {
        // heap_factor <= 0.80 would compute k' <= 0; the floor protects the native "k' > 0" rule.
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(0.81f)); // exactly 1 at the boundary
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(0.8f));  // 0 -> clamp
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(0.5f));  // negative -> clamp
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(0.01f));
    }

    public void testDefensiveInputs_clampToMinKPrime() {
        // heap_factor is validated positive upstream; the converter must still never
        // return a non-positive or garbage budget that the native side would reject.
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(0.0f));
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(-3.0f));
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(Float.NaN));
        assertEquals(MIN_K_PRIME, DiskSeismicKPrime.fromHeapFactor(Float.NEGATIVE_INFINITY));
    }

    public void testNoCeiling_saturatesWithoutOverflow() {
        // A huge heap_factor must saturate to Integer.MAX_VALUE, never overflow to a
        // negative int (which the native side would reject). Native clamps k' to the pool.
        assertEquals(Integer.MAX_VALUE, DiskSeismicKPrime.fromHeapFactor(1.0e9f));
        assertEquals(Integer.MAX_VALUE, DiskSeismicKPrime.fromHeapFactor(Float.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, DiskSeismicKPrime.fromHeapFactor(Float.POSITIVE_INFINITY));
    }

    public void testMonotoneNonDecreasing_acrossOperatingRange() {
        int previous = Integer.MIN_VALUE;
        // 0.50 .. 3.00 in 0.01 steps, the region users actually operate in.
        for (int hundredths = 50; hundredths <= 300; hundredths++) {
            int current = DiskSeismicKPrime.fromHeapFactor(hundredths / 100.0f);
            assertTrue(
                "k' must be non-decreasing in heap_factor at hf=" + (hundredths / 100.0f) + " (prev=" + previous + ", cur=" + current + ")",
                current >= previous
            );
            assertTrue("k' must always be at least MIN_K_PRIME", current >= MIN_K_PRIME);
            previous = current;
        }
    }
}
