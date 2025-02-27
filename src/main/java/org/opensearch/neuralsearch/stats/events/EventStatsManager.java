/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.Getter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class EventStatsManager {
    @Getter
    private Map<EventStatName, EventStat> stats;

    public static EventStatsManager INSTANCE;

    public static EventStatsManager instance() {
        if (INSTANCE == null) {
            INSTANCE = new EventStatsManager();
        }
        return INSTANCE;
    }

    public static void increment(EventStatName eventStatName) {
        if (instance().getStats().containsKey(eventStatName)) {
            instance().getStats().get(eventStatName).increment();
        }
    }

    public EventStatsManager() {
        this.stats = new ConcurrentSkipListMap<>();

        for (EventStatName eventStatName : EnumSet.allOf(EventStatName.class)) {
            if (eventStatName.getEventStatType() == EventStatType.TIMESTAMPED_COUNTER) {
                stats.put(eventStatName, new TimestampedEventStat(eventStatName));
            }
        }
    }

    public Map<EventStatName, EventStatData> getEventStatData(EnumSet<EventStatName> statsToRetrieve) {
        // Filter stats based on passed in collection
        Map<EventStatName, EventStatData> eventStatsDataMap = new HashMap<>();
        for (EventStatName statName : statsToRetrieve) {
            if (stats.containsKey(statName)) {
                eventStatsDataMap.put(statName, stats.get(statName).getEventStatData());
            }
        }
        return eventStatsDataMap;
    }
}
