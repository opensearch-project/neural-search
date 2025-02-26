/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import java.util.concurrent.atomic.LongAdder;

public class TimestampedEventStat implements EventStat {
    private long lastEventTimestamp;
    private LongAdder adder;

    public TimestampedEventStat(EventStatName statName) {
        lastEventTimestamp = 0L;
        adder = new LongAdder();
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
}
