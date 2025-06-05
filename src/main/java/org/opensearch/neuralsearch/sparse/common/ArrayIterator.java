/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.util.Iterator;

public class ArrayIterator<T> implements Iterator<T> {
    private final T[] array;  // Reference to the original array
    private int position = 0;

    public ArrayIterator(T[] array) {
        this.array = array;  // Just stores a reference, no duplication
    }

    @Override
    public boolean hasNext() {
        return position < array.length;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        return array[position++];
    }

    public static class IntArrayIterator implements Iterator<Integer> {
        private final int[] array;
        private int index = 0;

        public IntArrayIterator(int[] array) {
            this.array = array;
        }

        @Override
        public boolean hasNext() {
            return index < array.length;
        }

        @Override
        public Integer next() {
            if (!hasNext()) {
                return null;
            }
            return array[index++];
        }
    }

    public static class ByteArrayIterator implements Iterator<Byte> {
        private final byte[] array;
        private int index = 0;

        public ByteArrayIterator(byte[] array) {
            this.array = array;
        }

        @Override
        public boolean hasNext() {
            return index < array.length;
        }

        @Override
        public Byte next() {
            if (!hasNext()) {
                return null;
            }
            return array[index++];
        }
    }
}
