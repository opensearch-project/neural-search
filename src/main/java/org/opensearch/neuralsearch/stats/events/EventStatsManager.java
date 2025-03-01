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

public class EventStatsManager {
    @Getter
    private Map<EventStatName, EventStat> stats;

    private static EventStatsManager INSTANCE;
    private NeuralSearchSettingsAccessor settingsAccessor;

    public static EventStatsManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new EventStatsManager(NeuralSearchSettingsAccessor.instance());
        }
        return INSTANCE;
    }

    public static void increment(EventStatName eventStatName) {
        boolean enabled = instance().settingsAccessor.getIsStatsEnabled();
        if (enabled && instance().getStats().containsKey(eventStatName)) {
            instance().getStats().get(eventStatName).increment();
        }
    }

    public EventStatsManager(NeuralSearchSettingsAccessor settingsAccessor) {
        this.settingsAccessor = settingsAccessor;
        this.stats = new HashMap<>();

        for (EventStatName eventStatName : EnumSet.allOf(EventStatName.class)) {
            if (eventStatName.getStatType() == EventStatType.TIMESTAMPED_COUNTER) {
                stats.put(eventStatName, new TimestampedEventStat(eventStatName));
            }
        }
    }

    public Map<EventStatName, TimestampedEventStatSnapshot> getEventStatData(EnumSet<EventStatName> statsToRetrieve) {
        // Filter stats based on passed in collection
        Map<EventStatName, TimestampedEventStatSnapshot> eventStatsDataMap = new HashMap<>();
        for (EventStatName statName : statsToRetrieve) {
            if (stats.containsKey(statName)) {
                // Get event data snapshot
                eventStatsDataMap.put(statName, stats.get(statName).getEventStatData());
            }
        }
        return eventStatsDataMap;
    }

    /**
     * Called when stats_enabled cluster setting is toggled off. Resets all stat counters.
     */
    public void reset() {
        for (Map.Entry<EventStatName, EventStat> entry : stats.entrySet()) {
            entry.getValue().reset();
        }
    }
}
