/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats;

import org.opensearch.neuralsearch.stats.suppliers.CounterSupplier;

import java.util.function.Supplier;

public class NeuralStat<T> {
    private Supplier<T> supplier;

    public NeuralStat(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T getValue() {
        return supplier.get();
    }

    /**
     * Increments the supplier if it can be incremented
     */
    public void increment() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).increment();
        }
    }

    /**
     * Decrease the supplier if it can be decreased.
     */
    public void decrement() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).decrement();
        }
    }
}
