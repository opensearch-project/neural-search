/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
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
public class MemoryStatSnapshot implements Writeable, StatSnapshot<String> {

    private final MetricStatName statName;
    private final boolean isAggregationMetric;
    private String value;
    private long byteSize;

    /**
     * Create a stat new snapshot from an input stream
     * @param in the input stream
     * @throws IOException
     */
    public MemoryStatSnapshot(StreamInput in) throws IOException {
        this.statName = in.readEnum(MetricStatName.class);
        this.isAggregationMetric = in.readBoolean();
        this.value = in.readString();
        this.byteSize = in.readLong();
    }

    /**
     * Writes the stat snapshot to an output stream
     * @param out the output stream
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(statName);
        out.writeBoolean(isAggregationMetric);
        out.writeString(value);
        out.writeLong(byteSize);
    }

    /**
     * Static method to aggregate multiple event stats snapshots.
     * This is intended for combining stat snapshots from multiple nodes to give a cluster level aggregate
     * for the stat across nodes.
     * Different metadata fields are aggregated differently
     * @param snapshots the collection of snapshots
     * @return MemoryStatSnapshot The aggregated stats
     */
    public static MemoryStatSnapshot aggregateMetricSnapshots(Collection<MemoryStatSnapshot> snapshots) throws IllegalArgumentException {
        if (snapshots == null || snapshots.isEmpty()) {
            return null;
        }

        MetricStatName name = null;
        long totalByteSize = 0;

        for (MemoryStatSnapshot stat : snapshots) {
            // Mixed version clusters may have nodes that return null stat snapshots not available on older versions.
            // If so, exclude those from aggregation
            if (stat == null) {
                continue;
            }

            // The first stat name is taken. This should never be called across memory stats that don't share stat names
            if (name == null) {
                name = stat.getStatName();
            } else if (name != stat.getStatName()) {
                throw new IllegalArgumentException("Should not aggregate snapshots across different stat names");
            }

            // The value is summed
            totalByteSize += stat.getByteSize();
        }
        String totalValue = new ByteSizeValue(totalByteSize).toString();

        return MemoryStatSnapshot.builder().statName(name).value(totalValue).byteSize(totalByteSize).build();
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
        builder.field(StatSnapshot.STAT_TYPE_FIELD, statName == null ? null : statName.getStatType().getTypeString());
        builder.endObject();
        return builder;
    }
}
