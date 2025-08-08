/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.util.Iterator;
import java.util.function.BiFunction;

/**
 * An iterator that combines elements from two iterators using a provided function.
 * The iteration continues as long as both iterators have elements.
 *
 * @param <T> the type of elements from the first iterator
 * @param <U> the type of elements from the second iterator
 * @param <R> the type of the combined result
 */
public class CombinedIterator<T, U, R> implements Iterator<R> {
    private final Iterator<T> firstIter;
    private final Iterator<U> secondIter;
    private final BiFunction<T, U, R> combinedDataCreator;

    /**
     * Constructs a combined iterator from two iterators and a combining function.
     *
     * @param firstIter the first iterator
     * @param secondIter the second iterator
     * @param combinedDataCreator function to combine elements from both iterators
     */
    public CombinedIterator(Iterator<T> firstIter, Iterator<U> secondIter, BiFunction<T, U, R> combinedDataCreator) {
        this.firstIter = firstIter;
        this.secondIter = secondIter;
        this.combinedDataCreator = combinedDataCreator;
    }

    /**
     * Returns true if both iterators have more elements.
     *
     * @return true if both iterators have elements, false otherwise
     */
    @Override
    public boolean hasNext() {
        return firstIter.hasNext() && secondIter.hasNext();
    }

    /**
     * Returns the next combined element by applying the combining function
     * to the next elements from both iterators.
     *
     * @return the combined result, or null if either iterator is exhausted
     */
    @Override
    public R next() {
        if (!hasNext()) return null;
        T t = firstIter.next();
        U u = secondIter.next();
        return this.combinedDataCreator.apply(t, u);
    }
}
