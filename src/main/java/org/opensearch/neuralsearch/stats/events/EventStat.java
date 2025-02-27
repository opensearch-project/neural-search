/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

public interface EventStat {
    public Long getValue();

    public Long getTrailingIntervalValue();

    public Long getMinutesSinceLastEvent();

    public EventStatSnapshot getEventStatData();

    /**
     * Increments the supplier if it can be incremented
     */
    public void increment();
}
