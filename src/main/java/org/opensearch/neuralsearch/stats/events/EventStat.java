/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.neuralsearch.stats.common.StatSnapshot;

/**
 * Interface for event stats. These contain logic to store and update ongoing event information.
 */
public interface EventStat {
    /**
     * Returns a single point in time value associated with the stat. Typically a counter.
     * @return the value of the stat
      */
    long getValue();

    /**
     * Returns a snapshot of the stat. Used to cross transport layer/rest layer
     * @return
     */
    StatSnapshot<?> getStatSnapshot();

    /**
     * Increments the stat
     */
    void increment();

    /**
     * Resets the stat value
     */
    void reset();
}
