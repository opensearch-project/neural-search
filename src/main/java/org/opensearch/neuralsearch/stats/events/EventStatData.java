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

import java.io.IOException;

@Getter
@Builder
@AllArgsConstructor
public class EventStatData implements Writeable {
    private EventStatName eventStatName;
    private Long value;
    private Long trailingIntervalValue;
    private Long minutesSinceLastEvent;

    public EventStatData(StreamInput in) throws IOException {
        this.eventStatName = in.readEnum(EventStatName.class);
        this.value = in.readLong();
        this.trailingIntervalValue = in.readLong();
        this.minutesSinceLastEvent = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(eventStatName);
        out.writeLong(value);
        out.writeLong(trailingIntervalValue);
        out.writeLong(minutesSinceLastEvent);
    }
}
