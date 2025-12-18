/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Min-heap wrapper for maintaining top-K highest scoring results.
 * Uses threshold optimization to skip low-scoring entries.
 *
 * @param <V> numeric score type
 */
class HeapWrapper<V extends Number> {
    private final PriorityQueue<Pair<Integer, V>> heap = makeHeap();
    @SuppressWarnings("unchecked")
    private V heapThreshold = (V) Float.valueOf(Float.NEGATIVE_INFINITY);
    private final int k;

    /**
     * Creates a heap wrapper with specified capacity.
     *
     * @param k maximum number of results to maintain
     */
    HeapWrapper(int k) {
        this.k = k;
    }

    /**
     * Checks if heap has reached capacity.
     *
     * @return true if heap contains k elements
     */
    public boolean isFull() {
        return heap.size() == this.k;
    }

    /**
     * Adds pair to heap if score exceeds threshold, maintaining size limit.
     */
    public void add(Pair<Integer, V> pair) {
        if (pair.getRight().floatValue() > heapThreshold.floatValue()) {
            heap.add(pair);
            if (heap.size() > k) {
                heap.poll();
                assert heap.peek() != null;
                heapThreshold = heap.peek().getRight();
            }
        }
    }

    /**
     * Returns heap contents as ordered list sorted by document ID.
     */
    public List<Pair<Integer, V>> toOrderedList() {
        List<Pair<Integer, V>> list = new ArrayList<>(heap);
        list.sort(Comparator.comparingInt(Pair::getLeft));
        return list;
    }

    /**
     * Returns current number of elements in heap.
     *
     * @return heap size
     */
    public int size() {
        return heap.size();
    }

    /**
     * Returns the minimum scoring element without removing it.
     *
     * @return pair with lowest score, or null if empty
     */
    public Pair<Integer, V> peek() {
        return heap.peek();
    }

    /**
     * Creates a min-heap ordered by score.
     *
     * @return new priority queue with score-based comparator
     */
    public PriorityQueue<Pair<Integer, V>> makeHeap() {
        return new PriorityQueue<>(Comparator.comparingDouble(p -> p.getRight().floatValue()));
    }
}
