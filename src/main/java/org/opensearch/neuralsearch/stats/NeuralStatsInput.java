/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.state.StateStatName;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Holds user requester input parameters for retrieving neural stats via rest handler API.
 * It allows filtering statistics by node IDs, event statistic types, and state statistic types.
 */
@Getter
public class NeuralStatsInput implements ToXContentObject, Writeable {
    public static final String NODE_IDS_FIELD = "node_ids";
    public static final String EVENT_STAT_NAMES_FIELD = "event_stats";
    public static final String STATE_STAT_NAMES_FIELD = "state_stats";

    /**
     * Collection of node IDs to filter statistics retrieval.
     * If empty, stats from all nodes will be retrieved.
     */
    private Set<String> nodeIds;

    /**
     * Collection of event statistic types to filter.
     */
    private EnumSet<EventStatName> eventStatNames;

    /**
     * Collection of state statistic types to filter.
     */
    private EnumSet<StateStatName> stateStatNames;

    /**
     * Controls whether metadata should be included in the statistics response.
     */
    @Setter
    private boolean includeMetadata;

    /**
     * Controls whether the response keys should be flattened.
     */
    @Setter
    private boolean flatten;

    /**
     * Builder constructor for creating NeuralStatsInput with specific filtering parameters.
     *
     * @param nodeIds node IDs to retrieve stats from
     * @param eventStatNames event stats to retrieve
     * @param stateStatNames state stats to retrieve
     * @param includeMetadata whether to include metadata
     * @param flatten whether to flatten keys
     */
    @Builder
    public NeuralStatsInput(
        Set<String> nodeIds,
        EnumSet<EventStatName> eventStatNames,
        EnumSet<StateStatName> stateStatNames,
        boolean includeMetadata,
        boolean flatten
    ) {
        this.nodeIds = nodeIds;
        this.eventStatNames = eventStatNames;
        this.stateStatNames = stateStatNames;
        this.includeMetadata = includeMetadata;
        this.flatten = flatten;
    }

    /**
     * Default constructor that initializes with empty filters and default settings.
     * By default, metadata is excluded and keys are not flattened.
     */
    public NeuralStatsInput() {
        this.nodeIds = new HashSet<>();
        this.eventStatNames = EnumSet.noneOf(EventStatName.class);
        this.stateStatNames = EnumSet.noneOf(StateStatName.class);
        this.includeMetadata = false;
        this.flatten = false;
    }

    /**
     * Constructor for stream input
     *
     * @param input the StreamInput to read data from
     * @throws IOException if there's an error reading from the stream
     */
    public NeuralStatsInput(StreamInput input) throws IOException {
        nodeIds = input.readBoolean() ? new HashSet<>(input.readStringList()) : new HashSet<>();
        eventStatNames = input.readBoolean() ? input.readEnumSet(EventStatName.class) : EnumSet.noneOf(EventStatName.class);
        stateStatNames = input.readBoolean() ? input.readEnumSet(StateStatName.class) : EnumSet.noneOf(StateStatName.class);
        includeMetadata = input.readBoolean();
        flatten = input.readBoolean();
    }

    /**
     * Serializes this object to a StreamOutput.
     *
     * @param out the StreamOutput to write data to
     * @throws IOException If there's an error writing to the stream
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(nodeIds);
        writeOptionalEnumSet(out, eventStatNames);
        writeOptionalEnumSet(out, stateStatNames);
        out.writeBoolean(includeMetadata);
        out.writeBoolean(flatten);
    }

    /**
     * Helper method to write an EnumSet to a StreamOutput with a presence flag.
     *
     * @param out the StreamOutput to write to
     * @param set the EnumSet to write
     * @throws IOException If there's an error writing to the stream
     */
    private void writeOptionalEnumSet(StreamOutput out, EnumSet<?> set) throws IOException {
        if (set != null && set.size() > 0) {
            out.writeBoolean(true);
            out.writeEnumSet(set);
        } else {
            out.writeBoolean(false);
        }
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
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (nodeIds != null) {
            builder.field(NODE_IDS_FIELD, nodeIds);
        }
        if (eventStatNames != null) {
            builder.field(EVENT_STAT_NAMES_FIELD, eventStatNames);
        }
        if (stateStatNames != null) {
            builder.field(STATE_STAT_NAMES_FIELD, stateStatNames);
        }
        builder.field("include_metadata", includeMetadata);
        builder.field("flat_keys", flatten);
        builder.endObject();
        return builder;
    }
}
