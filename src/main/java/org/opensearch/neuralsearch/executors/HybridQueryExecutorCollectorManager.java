/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.executors;

public interface HybridQueryExecutorCollectorManager<C extends HybridQueryExecutorCollector, R> {
    C newCollector();
}
