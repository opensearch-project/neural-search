/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.neuralsearch.stats.common.StatSnapshot;

/**
 * Interface for metric stats. These contain logic to store and update ongoing metric information.
 */
public interface MetricStat {
    /**
     * Returns a single point in time value associated with the stat.
     * @return the value of the stat
     */
    Double getValue();

    /**
     * Returns a snapshot of the stat. Used to cross transport layer/rest layer
     * @return a snapshot of the stat
     */
    StatSnapshot<?> getStatSnapshot();
}
