/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import lombok.NoArgsConstructor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Singleton manager class for event stats, used to increment and store event stat related data
 * The statistics collection can be enabled or disabled via cluster settings.
 */
@NoArgsConstructor
public class MetricStatsManager {
    private static MetricStatsManager INSTANCE;

    /**
     * Returns the singleton instance of EventStatsManager.
     * Creates a new instance with default settings if one doesn't exist.
     *
     * @return The singleton instance of EventStatsManager
     */
    public static MetricStatsManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new MetricStatsManager();
        }
        return INSTANCE;
    }

    /**
     * Retrieves snapshots of specified event statistics.
     *
     * @param statsToRetrieve Set of event stat names to retrieve data for
     * @return Map of event stat names to their current snapshots
     */
    public Map<MetricStatName, MemoryStatSnapshot> getMemoryEventStatSnapshots(EnumSet<MetricStatName> statsToRetrieve) {
        // Filter stats based on passed in collection
        Map<MetricStatName, MemoryStatSnapshot> metricStatsDataMap = new HashMap<>();
        for (MetricStatName statName : statsToRetrieve) {
            if (statName.getStatType() == MetricStatType.MEMORY) {
                StatSnapshot<?> snapshot = statName.getMetricStat().getStatSnapshot();
                if (snapshot instanceof MemoryStatSnapshot memoryStatSnapshot) {
                    // Get event data snapshot
                    metricStatsDataMap.put(statName, memoryStatSnapshot);
                }
            }
        }
        return metricStatsDataMap;
    }
}
