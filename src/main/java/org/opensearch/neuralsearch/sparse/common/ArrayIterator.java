/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.util.Iterator;

/**
 * A generic iterator implementation for arrays that provides efficient iteration
 * without duplicating the underlying array data.
 *
 * @param <T> the type of elements in the array
 */
public class ArrayIterator<T> implements Iterator<T> {
    private final T[] array;  // Reference to the original array
    private int position = 0;

    /**
     * Constructs an iterator for the given array.
     *
     * @param array the array to iterate over
     */
    public ArrayIterator(T[] array) {
        this.array = array;  // Just stores a reference, no duplication
    }

    /**
     * Returns true if there are more elements to iterate over.
     *
     * @return true if there are more elements, false otherwise
     */
    @Override
    public boolean hasNext() {
        return position < array.length;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element, or null if no more elements exist
     */
    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        return array[position++];
    }

    /**
     * Specialized iterator for int arrays to avoid boxing overhead.
     */
    public static class IntArrayIterator implements Iterator<Integer> {
        private final int[] array;
        private int index = 0;

        /**
         * Constructs an iterator for the given int array.
         *
         * @param array the int array to iterate over
         */
        public IntArrayIterator(int[] array) {
            this.array = array;
        }

        /**
         * Returns true if there are more elements to iterate over.
         *
         * @return true if there are more elements, false otherwise
         */
        @Override
        public boolean hasNext() {
            return index < array.length;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element, or null if no more elements exist
         */
        @Override
        public Integer next() {
            if (!hasNext()) {
                return null;
            }
            return array[index++];
        }
    }

    /**
     * Specialized iterator for byte arrays to avoid boxing overhead.
     */
    public static class ByteArrayIterator implements Iterator<Byte> {
        private final byte[] array;
        private int index = 0;

        /**
         * Constructs an iterator for the given byte array.
         *
         * @param array the byte array to iterate over
         */
        public ByteArrayIterator(byte[] array) {
            this.array = array;
        }

        /**
         * Returns true if there are more elements to iterate over.
         *
         * @return true if there are more elements, false otherwise
         */
        @Override
        public boolean hasNext() {
            return index < array.length;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element, or null if no more elements exist
         */
        @Override
        public Byte next() {
            if (!hasNext()) {
                return null;
            }
            return array[index++];
        }
    }
}
