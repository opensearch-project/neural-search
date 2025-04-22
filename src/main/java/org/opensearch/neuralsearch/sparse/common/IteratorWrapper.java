/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.util.Iterator;

/**
 * Wrapper class for iterator, it holds the current value of the iterator.
 */
public class IteratorWrapper<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    T current;

    public IteratorWrapper(Iterator<T> iterator) {
        this.iterator = iterator;
        this.current = null;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        if (hasNext()) {
            current = iterator.next();
            return current;
        }
        return null;
    }

    public T getCurrent() {
        return current;
    }
}
