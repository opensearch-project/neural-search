/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;

import java.io.IOException;
import java.util.Collection;

@Getter
@Builder
@AllArgsConstructor
public class TimestampedEventStatSnapshot implements Writeable, StatSnapshot<Long> {
    public static final String TRAILING_INTERVAL_KEY = "trailing_interval_value";
    public static final String MINUTES_SINCE_LAST_EVENT_KEY = "minutes_since_last_event";

    private EventStatName statName;
    private Long value;
    private Long trailingIntervalValue;
    private Long minutesSinceLastEvent;

    public TimestampedEventStatSnapshot(StreamInput in) throws IOException {
        this.statName = in.readEnum(EventStatName.class);
        this.value = in.readLong();
        this.trailingIntervalValue = in.readLong();
        this.minutesSinceLastEvent = in.readLong();
    }

    @Override
    public Long getValue() {
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(statName);
        out.writeLong(value);
        out.writeLong(trailingIntervalValue);
        out.writeLong(minutesSinceLastEvent);
    }

    public static TimestampedEventStatSnapshot aggregateEventStatData(
        Collection<TimestampedEventStatSnapshot> timestampedEventStatSnapshotCollection
    ) {
        if (timestampedEventStatSnapshotCollection == null || timestampedEventStatSnapshotCollection.isEmpty()) {
            return null;
        }

        EventStatName name = null;
        long totalValue = 0;
        long totalTrailingValue = 0;
        Long minMinutes = null;

        for (TimestampedEventStatSnapshot stat : timestampedEventStatSnapshotCollection) {
            if (name == null) {
                name = stat.getStatName();
            }
            totalValue += stat.getValue();
            totalTrailingValue += stat.getTrailingIntervalValue();
            if (minMinutes == null || stat.getMinutesSinceLastEvent() < minMinutes) {
                minMinutes = stat.getMinutesSinceLastEvent();
            }
        }

        return TimestampedEventStatSnapshot.builder()
            .statName(name)
            .value(totalValue)
            .trailingIntervalValue(totalTrailingValue)
            .minutesSinceLastEvent(minMinutes)
            .build();
    }

    /**
     * Converts to fields xContent
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(StatSnapshot.VALUE_KEY, value);
        builder.field(StatSnapshot.STAT_TYPE_KEY, statName.getStatType().getName());
        builder.field(TRAILING_INTERVAL_KEY, trailingIntervalValue);
        builder.field(MINUTES_SINCE_LAST_EVENT_KEY, minutesSinceLastEvent);
        builder.endObject();
        return builder;
    }
}
