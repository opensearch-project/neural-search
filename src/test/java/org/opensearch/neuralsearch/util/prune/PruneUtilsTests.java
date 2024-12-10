/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util.prune;

import org.opensearch.common.collect.Tuple;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class PruneUtilsTests extends OpenSearchTestCase {

    public void testPruneByTopK() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", 5.0f);
        input.put("b", 3.0f);
        input.put("c", 4.0f);
        input.put("d", 1.0f);

        // Test prune
        Map<String, Float> result = PruneUtils.pruneSparseVector(PruneType.TOP_K, 2, input);

        assertEquals(2, result.size());
        assertEquals(5.0f, result.get("a"), 0.001);
        assertEquals(4.0f, result.get("c"), 0.001);

        // Test split
        Tuple<Map<String, Float>, Map<String, Float>> tupleResult = PruneUtils.splitSparseVector(PruneType.TOP_K, 2, input);

        assertEquals(2, tupleResult.v1().size());
        assertEquals(2, tupleResult.v2().size());
        assertEquals(5.0f, tupleResult.v1().get("a"), 0.001);
        assertEquals(4.0f, tupleResult.v1().get("c"), 0.001);
        assertEquals(3.0f, tupleResult.v2().get("b"), 0.001);
        assertEquals(1.0f, tupleResult.v2().get("d"), 0.001);
    }

    public void testPruneByMaxRatio() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", 10.0f);
        input.put("b", 8.0f);
        input.put("c", 5.0f);
        input.put("d", 2.0f);

        // Test prune
        Map<String, Float> result = PruneUtils.pruneSparseVector(PruneType.MAX_RATIO, 0.7f, input);

        assertEquals(2, result.size());
        assertEquals(10.0f, result.get("a"), 0.001);
        assertEquals(8.0f, result.get("b"), 0.001);

        // Test split
        Tuple<Map<String, Float>, Map<String, Float>> tupleResult = PruneUtils.splitSparseVector(PruneType.MAX_RATIO, 0.7f, input);

        assertEquals(2, tupleResult.v1().size());
        assertEquals(2, tupleResult.v2().size());
        assertEquals(10.0f, tupleResult.v1().get("a"), 0.001);
        assertEquals(8.0f, tupleResult.v1().get("b"), 0.001);
        assertEquals(5.0f, tupleResult.v2().get("c"), 0.001);
        assertEquals(2.0f, tupleResult.v2().get("d"), 0.001);
    }

    public void testPruneByValue() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", 5.0f);
        input.put("b", 3.0f);
        input.put("c", 2.0f);
        input.put("d", 1.0f);

        // Test prune
        Map<String, Float> result = PruneUtils.pruneSparseVector(PruneType.ABS_VALUE, 3.0f, input);

        assertEquals(2, result.size());
        assertEquals(5.0f, result.get("a"), 0.001);
        assertEquals(3.0f, result.get("b"), 0.001);

        // Test split
        Tuple<Map<String, Float>, Map<String, Float>> tupleResult = PruneUtils.splitSparseVector(PruneType.ABS_VALUE, 3.0f, input);

        assertEquals(2, tupleResult.v1().size());
        assertEquals(2, tupleResult.v2().size());
        assertEquals(5.0f, tupleResult.v1().get("a"), 0.001);
        assertEquals(3.0f, tupleResult.v1().get("b"), 0.001);
        assertEquals(2.0f, tupleResult.v2().get("c"), 0.001);
        assertEquals(1.0f, tupleResult.v2().get("d"), 0.001);
    }

    public void testPruneByAlphaMass() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", 10.0f);
        input.put("b", 6.0f);
        input.put("c", 3.0f);
        input.put("d", 1.0f);

        // Test prune
        Map<String, Float> result = PruneUtils.pruneSparseVector(PruneType.ALPHA_MASS, 0.8f, input);

        assertEquals(2, result.size());
        assertEquals(10.0f, result.get("a"), 0.001);
        assertEquals(6.0f, result.get("b"), 0.001);

        // Test split
        Tuple<Map<String, Float>, Map<String, Float>> tupleResult = PruneUtils.splitSparseVector(PruneType.ALPHA_MASS, 0.8f, input);

        assertEquals(2, tupleResult.v1().size());
        assertEquals(2, tupleResult.v2().size());
        assertEquals(10.0f, tupleResult.v1().get("a"), 0.001);
        assertEquals(6.0f, tupleResult.v1().get("b"), 0.001);
        assertEquals(3.0f, tupleResult.v2().get("c"), 0.001);
        assertEquals(1.0f, tupleResult.v2().get("d"), 0.001);
    }

    public void testNonePrune() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", 5.0f);
        input.put("b", 3.0f);
        input.put("c", 4.0f);
        input.put("d", 1.0f);

        // Test prune
        Map<String, Float> result = PruneUtils.pruneSparseVector(PruneType.NONE, 2, input);

        assertEquals(4, result.size());
        assertEquals(5.0f, result.get("a"), 0.001);
        assertEquals(3.0f, result.get("b"), 0.001);
        assertEquals(4.0f, result.get("c"), 0.001);
        assertEquals(1.0f, result.get("d"), 0.001);

        // Test split
        Tuple<Map<String, Float>, Map<String, Float>> tupleResult = PruneUtils.splitSparseVector(PruneType.NONE, 2, input);

        assertEquals(4, tupleResult.v1().size());
        assertEquals(0, tupleResult.v2().size());
        assertEquals(5.0f, tupleResult.v1().get("a"), 0.001);
        assertEquals(3.0f, tupleResult.v1().get("b"), 0.001);
        assertEquals(4.0f, tupleResult.v1().get("c"), 0.001);
        assertEquals(1.0f, tupleResult.v1().get("d"), 0.001);
    }

    public void testEmptyInput() {
        Map<String, Float> input = new HashMap<>();

        // Test prune
        Map<String, Float> result = PruneUtils.pruneSparseVector(PruneType.TOP_K, 5, input);
        assertTrue(result.isEmpty());

        // Test split
        Tuple<Map<String, Float>, Map<String, Float>> tupleResult = PruneUtils.splitSparseVector(PruneType.TOP_K, 5, input);
        assertTrue(tupleResult.v1().isEmpty());
        assertTrue(tupleResult.v2().isEmpty());
    }

    public void testNegativeValues() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", -5.0f);
        input.put("b", 3.0f);
        input.put("c", 4.0f);

        // Test prune
        IllegalArgumentException exception1 = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.pruneSparseVector(PruneType.TOP_K, 2, input)
        );
        assertEquals("Pruned values must be positive", exception1.getMessage());

        // Test split
        IllegalArgumentException exception2 = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.splitSparseVector(PruneType.TOP_K, 2, input)
        );
        assertEquals("Pruned values must be positive", exception2.getMessage());
    }

    public void testInvalidPruneType() {
        Map<String, Float> input = new HashMap<>();
        input.put("a", 1.0f);
        input.put("b", 2.0f);

        // Test prune
        IllegalArgumentException exception1 = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.pruneSparseVector(null, 2, input)
        );
        assertEquals(exception1.getMessage(), "Prune type must be provided");

        // Test split
        IllegalArgumentException exception2 = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.splitSparseVector(null, 2, input)
        );
        assertEquals(exception2.getMessage(), "Prune type must be provided");
    }

    public void testNullSparseVector() {
        IllegalArgumentException exception1 = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.pruneSparseVector(PruneType.TOP_K, 2, null)
        );
        assertEquals(exception1.getMessage(), "Sparse vector must be provided");

        IllegalArgumentException exception2 = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.splitSparseVector(PruneType.TOP_K, 2, null)
        );
        assertEquals(exception2.getMessage(), "Sparse vector must be provided");
    }

    public void testIsValidPruneRatio() {
        // Test TOP_K validation
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.TOP_K, 1));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.TOP_K, 100));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.TOP_K, 0));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.TOP_K, -1));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.TOP_K, 1.5f));

        // Test ALPHA_MASS validation
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ALPHA_MASS, 0.5f));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.ALPHA_MASS, 1.0f));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ALPHA_MASS, 0));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.ALPHA_MASS, -0.1f));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.ALPHA_MASS, 1.1f));

        // Test MAX_RATIO validation
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.MAX_RATIO, 0.0f));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.MAX_RATIO, 0.5f));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.MAX_RATIO, 1.0f));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.MAX_RATIO, -0.1f));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.MAX_RATIO, 1.1f));

        // Test ABS_VALUE validation
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ABS_VALUE, 0.0f));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ABS_VALUE, 1.0f));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ABS_VALUE, 100.0f));
        assertFalse(PruneUtils.isValidPruneRatio(PruneType.ABS_VALUE, -0.1f));

        // Test with extreme cases
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.TOP_K, Float.MAX_VALUE));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ABS_VALUE, Float.MAX_VALUE));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.ALPHA_MASS, Float.MIN_VALUE));
        assertTrue(PruneUtils.isValidPruneRatio(PruneType.MAX_RATIO, Float.MIN_VALUE));
    }

    public void testIsValidPruneRatioWithNullType() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PruneUtils.isValidPruneRatio(null, 1.0f));
        assertEquals("Prune type cannot be null", exception.getMessage());
    }

    public void testGetValidPruneRatioDescription() {
        assertEquals("prune_ratio should be positive integer.", PruneUtils.getValidPruneRatioDescription(PruneType.TOP_K));
        assertEquals("prune_ratio should be in the range [0, 1).", PruneUtils.getValidPruneRatioDescription(PruneType.MAX_RATIO));
        assertEquals("prune_ratio should be in the range [0, 1).", PruneUtils.getValidPruneRatioDescription(PruneType.ALPHA_MASS));
        assertEquals("prune_ratio should be non-negative.", PruneUtils.getValidPruneRatioDescription(PruneType.ABS_VALUE));
        assertEquals(
            "prune_ratio field is not supported when prune_type is none",
            PruneUtils.getValidPruneRatioDescription(PruneType.NONE)
        );

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> PruneUtils.getValidPruneRatioDescription(null)
        );
        assertEquals(exception.getMessage(), "Prune type cannot be null");
    }
}
