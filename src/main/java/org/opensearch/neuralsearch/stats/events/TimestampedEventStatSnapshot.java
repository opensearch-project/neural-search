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

/**
 * A stat snapshot for a timestamped event stat at a point in time
 * This is generated from an event stat to freeze the ongoing counter and other time related metadata field
 * These are meant for transport layer/rest layer and not meant to be persisted
 */
@Getter
@Builder
@AllArgsConstructor
public class TimestampedEventStatSnapshot implements Writeable, StatSnapshot<Long> {
    public static final String TRAILING_INTERVAL_KEY = "trailing_interval_value";
    public static final String MINUTES_SINCE_LAST_EVENT_KEY = "minutes_since_last_event";

    private EventStatName statName;
    private long value;
    private long trailingIntervalValue;
    private long minutesSinceLastEvent;

    /**
     * Create a stat new snapshot from an input stream
     * @param in the input stream
     * @throws IOException
     */
    public TimestampedEventStatSnapshot(StreamInput in) throws IOException {
        this.statName = in.readEnum(EventStatName.class);
        this.value = in.readLong();
        this.trailingIntervalValue = in.readLong();
        this.minutesSinceLastEvent = in.readLong();
    }

    /**
     * Gets the value of the counter
     * @return the value of the counter
     */
    @Override
    public Long getValue() {
        return value;
    }

    /**
     * Writes the stat snapshot to an output stream
     * @param out the output stream
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(statName);
        out.writeLong(value);
        out.writeLong(trailingIntervalValue);
        out.writeLong(minutesSinceLastEvent);
    }

    /**
     * Static method to aggregate multiple event stats snapshots.
     * This is intended for combining stat snapshots from multiple nodes to give an cluster level aggregate
     * for the stat across nodes.
     * Different metadata fields are aggregated differently
     * @param snapshots the collection of snapshots
     * @return
     */
    public static TimestampedEventStatSnapshot aggregateEventStatSnapshots(Collection<TimestampedEventStatSnapshot> snapshots)
        throws IllegalArgumentException {
        if (snapshots == null || snapshots.isEmpty()) {
            return null;
        }

        EventStatName name = null;
        long totalValue = 0;
        long totalTrailingValue = 0;
        Long minMinutes = null;

        for (TimestampedEventStatSnapshot stat : snapshots) {
            // Mixed version clusters may have nodes that return null stat snapshots not available on older versions.
            // If so, exclude those from aggregation
            if (stat == null) {
                continue;
            }

            // The first stat name is taken. This should never be called across event stats that don't share stat names
            if (name == null) {
                name = stat.getStatName();
            } else if (name != stat.getStatName()) {
                throw new IllegalArgumentException("Should not aggregate snapshots across different stat names");
            }

            // The value is summed
            totalValue += stat.getValue();

            // The trailing value is summed
            totalTrailingValue += stat.getTrailingIntervalValue();

            // Take the min of minutes since last event
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
     * Converts to fields xContent, including stat metadata
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(StatSnapshot.VALUE_FIELD, value);
        builder.field(StatSnapshot.STAT_TYPE_FIELD, statName.getStatType().getTypeString());
        builder.field(TRAILING_INTERVAL_KEY, trailingIntervalValue);
        builder.field(MINUTES_SINCE_LAST_EVENT_KEY, minutesSinceLastEvent);
        builder.endObject();
        return builder;
    }
}
