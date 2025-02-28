/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

public interface EventStat {
    long getValue();

    EventStatSnapshot getEventStatData();

    /**
     * Increments the supplier if it can be incremented
     */
    void increment();
}
