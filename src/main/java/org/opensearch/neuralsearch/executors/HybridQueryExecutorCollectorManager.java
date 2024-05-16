/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.executors;

/**
 * {@link HybridQueryExecutorCollectorManager} is responsible for creating new {@link HybridQueryExecutorCollector} instances
 */
public interface HybridQueryExecutorCollectorManager<C extends HybridQueryExecutorCollector, R> {
    C newCollector();
}
