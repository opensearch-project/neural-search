/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;

public class CombinedIteratorTests extends AbstractSparseTestBase {

    public void testCombinedIterator_withEqualSizedIterators_combinesCorrectly() {
        Iterator<String> firstIter = Arrays.asList("a", "b", "c").iterator();
        Iterator<Integer> secondIter = Arrays.asList(1, 2, 3).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertTrue(combinedIter.hasNext());
        assertEquals("a1", combinedIter.next());

        assertTrue(combinedIter.hasNext());
        assertEquals("b2", combinedIter.next());

        assertTrue(combinedIter.hasNext());
        assertEquals("c3", combinedIter.next());

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withFirstIteratorShorter_stopsAtShorter() {
        Iterator<String> firstIter = Arrays.asList("a", "b").iterator();
        Iterator<Integer> secondIter = Arrays.asList(1, 2, 3, 4).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertEquals("a1", combinedIter.next());
        assertEquals("b2", combinedIter.next());

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withSecondIteratorShorter_stopsAtShorter() {
        Iterator<String> firstIter = Arrays.asList("a", "b", "c", "d").iterator();
        Iterator<Integer> secondIter = Arrays.asList(1, 2).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertEquals("a1", combinedIter.next());
        assertEquals("b2", combinedIter.next());

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withEmptyFirstIterator_hasNoElements() {
        Iterator<String> firstIter = Collections.<String>emptyList().iterator();
        Iterator<Integer> secondIter = Arrays.asList(1, 2, 3).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withEmptySecondIterator_hasNoElements() {
        Iterator<String> firstIter = Arrays.asList("a", "b", "c").iterator();
        Iterator<Integer> secondIter = Collections.<Integer>emptyList().iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withBothEmptyIterators_hasNoElements() {
        Iterator<String> firstIter = Collections.<String>emptyList().iterator();
        Iterator<Integer> secondIter = Collections.<Integer>emptyList().iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withDifferentTypes_combinesCorrectly() {
        Iterator<Integer> firstIter = Arrays.asList(10, 20, 30).iterator();
        Iterator<Double> secondIter = Arrays.asList(1.5, 2.5, 3.5).iterator();

        CombinedIterator<Integer, Double, Double> combinedIter = new CombinedIterator<>(firstIter, secondIter, (i, d) -> i + d);

        assertEquals(Double.valueOf(11.5), combinedIter.next());
        assertEquals(Double.valueOf(22.5), combinedIter.next());
        assertEquals(Double.valueOf(33.5), combinedIter.next());

        assertFalse(combinedIter.hasNext());
    }

    public void testCombinedIterator_withComplexCombineFunction_appliesCorrectly() {
        Iterator<String> firstIter = Arrays.asList("hello", "world").iterator();
        Iterator<String> secondIter = Arrays.asList("foo", "bar").iterator();

        CombinedIterator<String, String, String> combinedIter = new CombinedIterator<>(
            firstIter,
            secondIter,
            (s1, s2) -> s1.toUpperCase(Locale.ROOT) + "_" + s2.toUpperCase(Locale.ROOT)
        );

        assertEquals("HELLO_FOO", combinedIter.next());
        assertEquals("WORLD_BAR", combinedIter.next());

        assertFalse(combinedIter.hasNext());
    }

    public void testCombinedIterator_withNullElements_handlesNulls() {
        Iterator<String> firstIter = Arrays.asList("a", null, "c").iterator();
        Iterator<Integer> secondIter = Arrays.asList(1, 2, null).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(
            firstIter,
            secondIter,
            (s, i) -> String.valueOf(s) + String.valueOf(i)
        );

        assertEquals("a1", combinedIter.next());
        assertEquals("null2", combinedIter.next());
        assertEquals("cnull", combinedIter.next());

        assertFalse(combinedIter.hasNext());
    }

    public void testCombinedIterator_withSingleElementIterators_combinesOnce() {
        Iterator<String> firstIter = Arrays.asList("single").iterator();
        Iterator<Integer> secondIter = Arrays.asList(42).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + ":" + i);

        assertTrue(combinedIter.hasNext());
        assertEquals("single:42", combinedIter.next());

        assertFalse(combinedIter.hasNext());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_multipleCallsToNext_afterEnd_returnsNull() {
        Iterator<String> firstIter = Arrays.asList("a").iterator();
        Iterator<Integer> secondIter = Arrays.asList(1).iterator();

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + i);

        assertEquals("a1", combinedIter.next());
        assertNull(combinedIter.next());
        assertNull(combinedIter.next());
        assertNull(combinedIter.next());
    }

    public void testCombinedIterator_withArrayIterators_worksCorrectly() {
        String[] firstArray = { "x", "y", "z" };
        Integer[] secondArray = { 100, 200, 300 };

        ArrayIterator<String> firstIter = new ArrayIterator<>(firstArray);
        ArrayIterator<Integer> secondIter = new ArrayIterator<>(secondArray);

        CombinedIterator<String, Integer, String> combinedIter = new CombinedIterator<>(firstIter, secondIter, (s, i) -> s + "=" + i);

        assertEquals("x=100", combinedIter.next());
        assertEquals("y=200", combinedIter.next());
        assertEquals("z=300", combinedIter.next());

        assertFalse(combinedIter.hasNext());
    }

    public void testCombinedIterator_withObjectCreation_createsNewObjects() {
        Iterator<String> firstIter = Arrays.asList("name1", "name2").iterator();
        Iterator<Integer> secondIter = Arrays.asList(25, 30).iterator();

        CombinedIterator<String, Integer, TestPerson> combinedIter = new CombinedIterator<>(firstIter, secondIter, TestPerson::new);

        TestPerson person1 = combinedIter.next();
        assertEquals("name1", person1.name);
        assertEquals(25, person1.age);

        TestPerson person2 = combinedIter.next();
        assertEquals("name2", person2.name);
        assertEquals(30, person2.age);

        assertFalse(combinedIter.hasNext());
    }

    private static class TestPerson {
        final String name;
        final int age;

        TestPerson(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
