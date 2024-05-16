/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.executors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.function.Function;

@RequiredArgsConstructor(staticName = "newCollector")
public class HybridQueryExecutorCollector<I, R> {

    private final I param;
    @Getter
    private R result = null;

    public void collect(Function<I, R> action) {
        result = action.apply(param);
    }
}
