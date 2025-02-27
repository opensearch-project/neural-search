/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import java.util.concurrent.atomic.LongAdder;

public class TimestampedEventStat implements EventStat {
    private EventStatName statName;
    private long lastEventTimestamp;
    private LongAdder adder;

    public TimestampedEventStat(EventStatName statName) {
        this.statName = statName;
        this.lastEventTimestamp = 0L;
        this.adder = new LongAdder();
    }

    public Long getValue() {
        return adder.longValue();
    }

    public void increment() {
        adder.increment();
    }

    public Long getTrailingIntervalValue() {
        // Not yet implemented
        return 0L;
    }

    public Long getMinutesSinceLastEvent() {
        // Not yet implemented
        return 0L;
    }

    public EventStatSnapshot getEventStatData() {
        return EventStatSnapshot.builder()
            .statName(statName)
            .value(getValue())
            .trailingIntervalValue(getTrailingIntervalValue())
            .minutesSinceLastEvent(getMinutesSinceLastEvent())
            .build();
    }
}
