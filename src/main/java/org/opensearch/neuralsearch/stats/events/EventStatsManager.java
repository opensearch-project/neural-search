/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.Getter;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Manager class for event counter stats, used to track monotonically increasing counts plus some possible metadata
 * The statistics collection can be enabled or disabled via cluster settings.
 */
public class EventStatsManager {
    @Getter
    private Map<EventStatName, EventStat> stats;

    private static EventStatsManager INSTANCE;
    private NeuralSearchSettingsAccessor settingsAccessor;

    /**
     * Returns the singleton instance of EventStatsManager.
     * Creates a new instance with default settings if one doesn't exist.
     *
     * @return The singleton instance of EventStatsManager
     */
    public static EventStatsManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new EventStatsManager(NeuralSearchSettingsAccessor.instance());
        }
        return INSTANCE;
    }

    /**
     * Static helper to increments the counter for a specified event statistic on the singleton
     *
     * @param eventStatName The name of the event stat to increment
     */
    public static void increment(EventStatName eventStatName) {
        instance().inc(eventStatName);
    }

    /**
     * Constructs a new EventStatsManager with the provided settings accessor.
     * Initializes statistics counters for all defined event types.
     *
     * @param settingsAccessor The accessor used to retrieve neural search settings
     */
    public EventStatsManager(NeuralSearchSettingsAccessor settingsAccessor) {
        this.settingsAccessor = settingsAccessor;
        this.stats = new HashMap<>();

        for (EventStatName eventStatName : EnumSet.allOf(EventStatName.class)) {
            if (eventStatName.getStatType() == EventStatType.TIMESTAMPED_COUNTER) {
                stats.put(eventStatName, new TimestampedEventStat(eventStatName));
            }
        }
    }

    /**
     *  Instance level method to increment the counter for a specified event statistic.
     *
     * @param eventStatName The name of the event stat to increment
     */
    public void inc(EventStatName eventStatName) {
        boolean enabled = settingsAccessor.getIsStatsEnabled();
        if (enabled && stats.containsKey(eventStatName)) {
            stats.get(eventStatName).increment();
        }
    }

    /**
     * Retrieves snapshots of specified event statistics.
     *
     * @param statsToRetrieve Set of event stat names to retrieve data for
     * @return Map of event stat names to their current snapshots
     */
    public Map<EventStatName, TimestampedEventStatSnapshot> getEventStatSnapshots(EnumSet<EventStatName> statsToRetrieve) {
        // Filter stats based on passed in collection
        Map<EventStatName, TimestampedEventStatSnapshot> eventStatsDataMap = new HashMap<>();
        for (EventStatName statName : statsToRetrieve) {
            if (stats.containsKey(statName)) {
                // Get event data snapshot
                eventStatsDataMap.put(statName, stats.get(statName).getEventStatSnapshot());
            }
        }
        return eventStatsDataMap;
    }

    /**
     * Resets all statistics counters to their initial state.
     * Called when stats_enabled cluster setting is toggled off
     */
    public void reset() {
        for (Map.Entry<EventStatName, EventStat> entry : stats.entrySet()) {
            entry.getValue().reset();
        }
    }
}
