/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.executors;

import java.util.List;

public interface HybridQueryExecutorCollectorManager<C extends HybridQueryExecutorCollector, R> {
    C newCollector();

    List<R> merge(List<C> collectors);
}
