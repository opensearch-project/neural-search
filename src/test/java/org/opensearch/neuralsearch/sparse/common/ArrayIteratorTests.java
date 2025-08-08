/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class ArrayIteratorTests extends AbstractSparseTestBase {

    public void testArrayIterator_withStringArray_iteratesCorrectly() {
        String[] array = { "first", "second", "third" };
        ArrayIterator<String> iterator = new ArrayIterator<>(array);

        assertTrue(iterator.hasNext());
        assertEquals("first", iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals("second", iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals("third", iterator.next());

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testArrayIterator_withEmptyArray_hasNoElements() {
        String[] array = {};
        ArrayIterator<String> iterator = new ArrayIterator<>(array);

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testArrayIterator_withSingleElement_iteratesOnce() {
        Integer[] array = { 42 };
        ArrayIterator<Integer> iterator = new ArrayIterator<>(array);

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(42), iterator.next());

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testArrayIterator_withNullElements_handlesNulls() {
        String[] array = { "first", null, "third" };
        ArrayIterator<String> iterator = new ArrayIterator<>(array);

        assertEquals("first", iterator.next());
        assertNull(iterator.next());
        assertEquals("third", iterator.next());
        assertFalse(iterator.hasNext());
    }

    public void testIntArrayIterator_withIntArray_iteratesCorrectly() {
        int[] array = { 1, 2, 3, 4, 5 };
        ArrayIterator.IntArrayIterator iterator = new ArrayIterator.IntArrayIterator(array);

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(1), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(2), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(3), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(4), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(5), iterator.next());

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testIntArrayIterator_withEmptyArray_hasNoElements() {
        int[] array = {};
        ArrayIterator.IntArrayIterator iterator = new ArrayIterator.IntArrayIterator(array);

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testIntArrayIterator_withSingleElement_iteratesOnce() {
        int[] array = { 100 };
        ArrayIterator.IntArrayIterator iterator = new ArrayIterator.IntArrayIterator(array);

        assertTrue(iterator.hasNext());
        assertEquals(Integer.valueOf(100), iterator.next());

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testByteArrayIterator_withByteArray_iteratesCorrectly() {
        byte[] array = { 1, 2, 3 };
        ArrayIterator.ByteArrayIterator iterator = new ArrayIterator.ByteArrayIterator(array);

        assertTrue(iterator.hasNext());
        assertEquals(Byte.valueOf((byte) 1), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Byte.valueOf((byte) 2), iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(Byte.valueOf((byte) 3), iterator.next());

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testByteArrayIterator_withEmptyArray_hasNoElements() {
        byte[] array = {};
        ArrayIterator.ByteArrayIterator iterator = new ArrayIterator.ByteArrayIterator(array);

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testByteArrayIterator_withSingleElement_iteratesOnce() {
        byte[] array = { 127 };
        ArrayIterator.ByteArrayIterator iterator = new ArrayIterator.ByteArrayIterator(array);

        assertTrue(iterator.hasNext());
        assertEquals(Byte.valueOf((byte) 127), iterator.next());

        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
    }

    public void testByteArrayIterator_withNegativeValues_handlesCorrectly() {
        byte[] array = { -1, 0, 1 };
        ArrayIterator.ByteArrayIterator iterator = new ArrayIterator.ByteArrayIterator(array);

        assertEquals(Byte.valueOf((byte) -1), iterator.next());
        assertEquals(Byte.valueOf((byte) 0), iterator.next());
        assertEquals(Byte.valueOf((byte) 1), iterator.next());
        assertFalse(iterator.hasNext());
    }

    public void testArrayIterator_multipleCallsToNext_afterEnd_returnsNull() {
        String[] array = { "only" };
        ArrayIterator<String> iterator = new ArrayIterator<>(array);

        assertEquals("only", iterator.next());
        assertNull(iterator.next());
        assertNull(iterator.next());
        assertNull(iterator.next());
    }

    public void testIntArrayIterator_multipleCallsToNext_afterEnd_returnsNull() {
        int[] array = { 42 };
        ArrayIterator.IntArrayIterator iterator = new ArrayIterator.IntArrayIterator(array);

        assertEquals(Integer.valueOf(42), iterator.next());
        assertNull(iterator.next());
        assertNull(iterator.next());
    }

    public void testByteArrayIterator_multipleCallsToNext_afterEnd_returnsNull() {
        byte[] array = { 1 };
        ArrayIterator.ByteArrayIterator iterator = new ArrayIterator.ByteArrayIterator(array);

        assertEquals(Byte.valueOf((byte) 1), iterator.next());
        assertNull(iterator.next());
        assertNull(iterator.next());
    }

    public void testArrayIterator_withDifferentTypes_worksGenerically() {
        Double[] array = { 1.1, 2.2, 3.3 };
        ArrayIterator<Double> iterator = new ArrayIterator<>(array);

        assertEquals(Double.valueOf(1.1), iterator.next());
        assertEquals(Double.valueOf(2.2), iterator.next());
        assertEquals(Double.valueOf(3.3), iterator.next());
        assertFalse(iterator.hasNext());
    }
}
