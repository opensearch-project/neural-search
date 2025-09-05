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
 * Singleton manager class for metrics stats, used to record and store metric stat related data
 */
@NoArgsConstructor
public class MetricStatsManager {

    private static volatile MetricStatsManager INSTANCE;

    /**
     * Returns the singleton instance of EventStatsManager.
     * Creates a new instance with default settings if one doesn't exist.
     *
     * @return The singleton instance of EventStatsManager
     */
    public static MetricStatsManager instance() {
        if (INSTANCE == null) {
            synchronized (MetricStatsManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MetricStatsManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Retrieves snapshots of specified metric statistics.
     *
     * @param statsToRetrieve Set of metric stat names to retrieve data for
     * @return Map of metric stat names to their current snapshots
     */
    public Map<MetricStatName, MemoryStatSnapshot> getStats(EnumSet<MetricStatName> statsToRetrieve) {
        // Filter stats based on passed in collection
        Map<MetricStatName, MemoryStatSnapshot> metricStatsDataMap = new HashMap<>();
        for (MetricStatName statName : statsToRetrieve) {
            if (statName.getStatType() == MetricStatType.MEMORY) {
                StatSnapshot<?> snapshot = statName.getMetricStat().getStatSnapshot();
                if (snapshot instanceof MemoryStatSnapshot memoryStatSnapshot) {
                    // Get metric data snapshot
                    metricStatsDataMap.put(statName, memoryStatSnapshot);
                }
            }
        }
        return metricStatsDataMap;
    }
}
