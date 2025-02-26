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

@Getter
public class NeuralStatsInput implements ToXContentObject, Writeable {
    public static final String NODE_IDS = "node_ids";
    public static final String EVENT_STAT_NAMES = "event_stats";
    public static final String STATE_STAT_NAMES = "state_stats";

    /**
     * Which node's stats will be retrieved.
     */
    private Set<String> nodeIds;
    private EnumSet<EventStatName> eventStatNames;
    private EnumSet<StateStatName> stateStatNames;

    @Setter
    private boolean includeMetadata;
    @Setter
    private boolean flattenResponse;

    @Builder
    public NeuralStatsInput(
        Set<String> nodeIds,
        EnumSet<EventStatName> eventStatNames,
        EnumSet<StateStatName> stateStatNames,
        boolean includeMetadata,
        boolean flattenResponse
    ) {
        this.nodeIds = nodeIds;
        this.eventStatNames = eventStatNames;
        this.stateStatNames = stateStatNames;
        this.includeMetadata = includeMetadata;
        this.flattenResponse = flattenResponse;
    }

    public NeuralStatsInput() {
        this.nodeIds = new HashSet<>();
        this.eventStatNames = EnumSet.noneOf(EventStatName.class);
        this.stateStatNames = EnumSet.noneOf(StateStatName.class);
        this.includeMetadata = false;
        this.flattenResponse = false;
    }

    public NeuralStatsInput(StreamInput input) throws IOException {
        nodeIds = input.readBoolean() ? new HashSet<>(input.readStringList()) : new HashSet<>();
        eventStatNames = input.readBoolean() ? input.readEnumSet(EventStatName.class) : EnumSet.noneOf(EventStatName.class);
        stateStatNames = input.readBoolean() ? input.readEnumSet(StateStatName.class) : EnumSet.noneOf(StateStatName.class);
        includeMetadata = input.readBoolean();
        flattenResponse = input.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(nodeIds);
        writeOptionalEnumSet(out, eventStatNames);
        writeOptionalEnumSet(out, stateStatNames);
        out.writeBoolean(includeMetadata);
        out.writeBoolean(flattenResponse);
    }

    private void writeOptionalEnumSet(StreamOutput out, EnumSet<?> set) throws IOException {
        if (set != null && set.size() > 0) {
            out.writeBoolean(true);
            out.writeEnumSet(set);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (nodeIds != null) {
            builder.field(NODE_IDS, nodeIds);
        }
        if (eventStatNames != null) {
            builder.field(EVENT_STAT_NAMES, eventStatNames);
        }
        if (stateStatNames != null) {
            builder.field(STATE_STAT_NAMES, stateStatNames);
        }
        builder.endObject();
        return builder;
    }

    public boolean retrieveAllStats() {
        return retrieveAllEventStats() && retrieveAllStateStats();
    }

    public boolean retrieveAllEventStats() {
        return eventStatNames == null || eventStatNames.isEmpty();
    }

    public boolean retrieveAllStateStats() {
        return stateStatNames == null || stateStatNames.isEmpty();
    }
}
