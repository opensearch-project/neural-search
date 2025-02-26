/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import lombok.Getter;
import lombok.Setter;

public class SettableStateStat<T> implements StateStat<T> {
    @Getter
    @Setter
    private T value;

    public SettableStateStat() {
        this.value = null;
    }

    public SettableStateStat(T value) {
        this.value = value;
    }
}
