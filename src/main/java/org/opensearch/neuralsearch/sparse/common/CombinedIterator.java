/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.util.Iterator;
import java.util.function.BiFunction;

public class CombinedIterator<T, U, R> implements Iterator<R> {
    private final Iterator<T> firstIter;
    private final Iterator<U> secondIter;
    private final BiFunction<T, U, R> combinedDataCreator;

    public CombinedIterator(Iterator<T> firstIter, Iterator<U> secondIter, BiFunction<T, U, R> combinedDataCreator) {
        this.firstIter = firstIter;
        this.secondIter = secondIter;
        this.combinedDataCreator = combinedDataCreator;
    }

    @Override
    public boolean hasNext() {
        return firstIter.hasNext() && secondIter.hasNext();
    }

    @Override
    public R next() {
        if (!hasNext()) return null;
        T t = firstIter.next();
        U u = secondIter.next();
        return this.combinedDataCreator.apply(t, u);
    }
}
