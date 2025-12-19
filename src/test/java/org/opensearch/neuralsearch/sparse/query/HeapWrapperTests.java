/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.util.List;

public class HeapWrapperTests extends AbstractSparseTestBase {
    public void testHeapWrapper() {
        // Create a heap wrapper
        HeapWrapper<Integer> heapWrapper = new HeapWrapper<>(3);

        // Add some pairs
        heapWrapper.add(Pair.of(1, 10));
        heapWrapper.add(Pair.of(2, 20));
        heapWrapper.add(Pair.of(3, 30));

        // Verify heap is full
        assertTrue(heapWrapper.isFull());

        // Add a pair with lower score, should not be added
        heapWrapper.add(Pair.of(4, 5));
        assertEquals(3, heapWrapper.size());

        // Add a pair with higher score, should replace lowest score
        heapWrapper.add(Pair.of(5, 40));
        assertEquals(3, heapWrapper.size());

        // Get ordered list
        List<Pair<Integer, Integer>> orderedList = heapWrapper.toOrderedList();
        assertEquals(3, orderedList.size());
        assertEquals(2, orderedList.get(0).getLeft().intValue());
        assertEquals(3, orderedList.get(1).getLeft().intValue());
        assertEquals(5, orderedList.get(2).getLeft().intValue());

        assertEquals(20, heapWrapper.peek().getRight().intValue());
    }
}
