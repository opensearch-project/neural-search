/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class IteratorWrapperTests extends AbstractSparseTestBase {

    private void assertIteratorResults(IteratorWrapper<Integer> wrapper, Integer... expected) {
        for (Integer expectedValue : expected) {
            assertEquals(expectedValue, wrapper.next());
            assertEquals(expectedValue, wrapper.getCurrent());
        }
    }

    public void testIteratorWrapperWithNullIterator() {
        IteratorWrapper<String> wrapper = new IteratorWrapper<>(null);
        expectThrows(NullPointerException.class, wrapper::hasNext);
    }

    public void testNextWhenHasNext() {
        Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
        IteratorWrapper<Integer> wrapper = new IteratorWrapper<>(iterator);

        assertIteratorResults(wrapper, 1, 2, 3);
    }

    public void testNextWhenNoMoreElements() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        IteratorWrapper<String> wrapper = new IteratorWrapper<>(emptyIterator);

        assertNull(wrapper.next());
    }

    public void testNextWithEmptyIterator() {
        Iterator<String> emptyIterator = Collections.emptyIterator();
        IteratorWrapper<String> wrapper = new IteratorWrapper<>(emptyIterator);

        assertNull(wrapper.next());
    }

    public void testIteratorWrapperInitialization() {
        Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
        IteratorWrapper<Integer> wrapper = new IteratorWrapper<>(iterator);

        assertNotNull(wrapper);
        assertNull(wrapper.getCurrent());
        assertTrue(wrapper.hasNext());
    }

}
