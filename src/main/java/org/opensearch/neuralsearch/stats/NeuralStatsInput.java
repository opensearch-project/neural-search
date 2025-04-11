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
import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForMetricStats;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForStatCategoryFiltering;

/**
 * Entity class to hold input parameters for retrieving neural stats
 * Responsible for filtering statistics by node IDs, event statistic types, and info stat types.
 */
@Getter
public class NeuralStatsInput implements ToXContentObject, Writeable {
    public static final String NODE_IDS_FIELD = "node_ids";
    public static final String EVENT_STAT_NAMES_FIELD = "event_stats";
    public static final String STATE_STAT_NAMES_FIELD = "state_stats";
    public static final String METRIC_STAT_NAMES_FIELD = "metric_stats";

    /**
     * Collection of node IDs to filter statistics retrieval.
     * If empty, stats from all nodes will be retrieved.
     */
    private final List<String> nodeIds;

    /**
     * Collection of event statistic types to filter.
     */
    private final EnumSet<EventStatName> eventStatNames;

    /**
     * Collection of info stat types to filter.
     */
    private final EnumSet<InfoStatName> infoStatNames;

    /**
     * Collection of metric stat types to filter.
     */
    private final EnumSet<MetricStatName> metricStatNames;

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

    @Setter
    /**
     * Controls whether the response will include individual nodes
     */
    private boolean includeIndividualNodes;
    @Setter
    /**
     * Controls whether the response will include aggregated nodes
     */
    private boolean includeAllNodes;
    @Setter
    /**
     * Controls whether the response will include info nodes
     */
    private boolean includeInfo;
    @Setter
    /**
     * Controls whether the response will include info nodes
     */
    private boolean includeMetrics;

    /**
     * Builder constructor for creating NeuralStatsInput with specific filtering parameters.
     *
     * @param nodeIds node IDs to retrieve stats from
     * @param eventStatNames event stats to retrieve
     * @param infoStatNames info stats to retrieve
     * @param includeMetadata whether to include metadata
     * @param flatten whether to flatten keys
     */
    @Builder
    public NeuralStatsInput(
        List<String> nodeIds,
        EnumSet<EventStatName> eventStatNames,
        EnumSet<InfoStatName> infoStatNames,
        EnumSet<MetricStatName> metricStatNames,
        boolean includeMetadata,
        boolean flatten,
        boolean includeIndividualNodes,
        boolean includeAllNodes,
        boolean includeInfo,
        boolean includeMetrics
    ) {
        this.nodeIds = nodeIds;
        this.eventStatNames = eventStatNames;
        this.infoStatNames = infoStatNames;
        this.metricStatNames = metricStatNames;
        this.includeMetadata = includeMetadata;
        this.flatten = flatten;
        this.includeIndividualNodes = includeIndividualNodes;
        this.includeAllNodes = includeAllNodes;
        this.includeInfo = includeInfo;
        this.includeMetrics = includeMetrics;
    }

    /**
     * Default constructor that initializes with empty filters and default settings.
     * By default, metadata is excluded and keys are not flattened.
     */
    public NeuralStatsInput() {
        this.nodeIds = new ArrayList<>();
        this.eventStatNames = EnumSet.noneOf(EventStatName.class);
        this.infoStatNames = EnumSet.noneOf(InfoStatName.class);
        this.metricStatNames = EnumSet.noneOf(MetricStatName.class);
        this.includeMetadata = false;
        this.flatten = false;
        this.includeIndividualNodes = true;
        this.includeAllNodes = true;
        this.includeInfo = true;
        this.includeMetrics = true;
    }

    /**
     * Constructor for stream input
     *
     * @param input the StreamInput to read data from
     * @throws IOException if there's an error reading from the stream
     */
    public NeuralStatsInput(StreamInput input) throws IOException {
        nodeIds = input.readOptionalStringList();
        eventStatNames = input.readOptionalEnumSet(EventStatName.class);
        infoStatNames = input.readOptionalEnumSet(InfoStatName.class);
        includeMetadata = input.readBoolean();
        flatten = input.readBoolean();
        if (isClusterOnOrAfterMinReqVersionForStatCategoryFiltering()) {
            includeIndividualNodes = input.readBoolean();
            includeAllNodes = input.readBoolean();
            includeInfo = input.readBoolean();
        } else {
            includeIndividualNodes = true;
            includeAllNodes = true;
            includeInfo = true;
        }
        if (isClusterOnOrAfterMinReqVersionForMetricStats()) {
            metricStatNames = input.readOptionalEnumSet(MetricStatName.class);
            includeMetrics = input.readBoolean();
        } else {
            metricStatNames = EnumSet.noneOf(MetricStatName.class);
            includeMetrics = true;
        }
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
        out.writeOptionalEnumSet(eventStatNames);
        out.writeOptionalEnumSet(infoStatNames);
        out.writeBoolean(includeMetadata);
        out.writeBoolean(flatten);
        if (isClusterOnOrAfterMinReqVersionForStatCategoryFiltering()) {
            out.writeBoolean(includeIndividualNodes);
            out.writeBoolean(includeAllNodes);
            out.writeBoolean(includeInfo);
        }
        if (isClusterOnOrAfterMinReqVersionForMetricStats()) {
            out.writeOptionalEnumSet(metricStatNames);
            out.writeBoolean(includeMetrics);
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
        if (infoStatNames != null) {
            builder.field(STATE_STAT_NAMES_FIELD, infoStatNames);
        }
        if (metricStatNames != null) {
            builder.field(METRIC_STAT_NAMES_FIELD, metricStatNames);
        }
        builder.field(RestNeuralStatsAction.INCLUDE_METADATA_PARAM, includeMetadata);
        builder.field(RestNeuralStatsAction.FLATTEN_PARAM, flatten);
        builder.field(RestNeuralStatsAction.INCLUDE_INDIVIDUAL_NODES_PARAM, includeIndividualNodes);
        builder.field(RestNeuralStatsAction.INCLUDE_ALL_NODES_PARAM, includeAllNodes);
        builder.field(RestNeuralStatsAction.INCLUDE_INFO_PARAM, includeInfo);
        builder.field(RestNeuralStatsAction.INCLUDE_METRIC_PARAM, includeMetrics);
        builder.endObject();
        return builder;
    }

    /**
     * Helper to determine if we should fetch event stats or if we can skip them
     * If we exclude both individual and all nodes, then there is no need to fetch any specific stats from nodes
     * @return whether we need to fetch event stats
     */
    public boolean includeEventsAndMetrics() {
        return this.isIncludeAllNodes() || this.isIncludeIndividualNodes();
    }
}
