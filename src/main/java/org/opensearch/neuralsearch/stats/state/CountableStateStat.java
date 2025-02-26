/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import java.util.concurrent.atomic.LongAdder;

public class CountableStateStat implements StateStat<Long> {
    private LongAdder adder;

    public CountableStateStat() {
        this.adder = new LongAdder();
    }

    public Long getValue() {
        return adder.longValue();
    }

    public void increment() {
        adder.increment();
    }

    public void incrementBy(Long delta) {
        adder.add(delta);
    }
}
