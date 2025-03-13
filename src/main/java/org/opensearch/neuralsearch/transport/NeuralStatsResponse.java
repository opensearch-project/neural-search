/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import lombok.Getter;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * NeuralStatsResponse consists of the aggregated responses from the nodes
 */
@Getter
public class NeuralStatsResponse extends BaseNodesResponse<NeuralStatsNodeResponse> implements ToXContentObject {
    public static final String INFO_KEY_PREFIX = "info";
    public static final String NODES_KEY_PREFIX = "nodes";
    public static final String AGGREGATED_NODES_KEY_PREFIX = "all_nodes";

    private Map<String, StatSnapshot<?>> infoStats;
    private Map<String, StatSnapshot<?>> aggregatedNodeStats;
    private Map<String, Map<String, StatSnapshot<?>>> nodeIdToNodeEventStats;
    private boolean flatten;
    private boolean includeMetadata;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public NeuralStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(NeuralStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
        Map<String, StatSnapshot<?>> castedInfoStats = (Map<String, StatSnapshot<?>>) (Map) in.readMap();
        Map<String, StatSnapshot<?>> castedAggregatedNodeStats = (Map<String, StatSnapshot<?>>) (Map) in.readMap();
        Map<String, Map<String, StatSnapshot<?>>> castedNodeIdToNodeEventStats = (Map<String, Map<String, StatSnapshot<?>>>) (Map) in
            .readMap();

        this.infoStats = castedInfoStats;
        this.aggregatedNodeStats = castedAggregatedNodeStats;
        this.nodeIdToNodeEventStats = castedNodeIdToNodeEventStats;
        this.flatten = in.readBoolean();
        this.includeMetadata = in.readBoolean();
    }

    /**
     * Constructor
     *
     * @param clusterName the cluster name
     * @param nodes the nodes responses
     * @param failures the failures
     * @param infoStats the cluster level info stats
     * @param aggregatedNodeStats the cluster level aggregated node stats
     * @param nodeIdToNodeEventStats the node id to node event stats
     * @param flatten whether to flatten keys
     * @param includeMetadata whether to include metadata
     */
    public NeuralStatsResponse(
        ClusterName clusterName,
        List<NeuralStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        Map<String, StatSnapshot<?>> infoStats,
        Map<String, StatSnapshot<?>> aggregatedNodeStats,
        Map<String, Map<String, StatSnapshot<?>>> nodeIdToNodeEventStats,
        boolean flatten,
        boolean includeMetadata
    ) {
        super(clusterName, nodes, failures);
        this.infoStats = infoStats;
        this.aggregatedNodeStats = aggregatedNodeStats;
        this.nodeIdToNodeEventStats = nodeIdToNodeEventStats;
        this.flatten = flatten;
        this.includeMetadata = includeMetadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Map<String, Object> downcastedInfoStats = (Map<String, Object>) (Map) (infoStats);
        Map<String, Object> downcastedAggregatedNodeStats = (Map<String, Object>) (Map) (aggregatedNodeStats);
        Map<String, Object> downcastedNodeIdToNodeEventStats = (Map<String, Object>) (Map) (nodeIdToNodeEventStats);

        out.writeMap(downcastedInfoStats);
        out.writeMap(downcastedAggregatedNodeStats);
        out.writeMap(downcastedNodeIdToNodeEventStats);
        out.writeBoolean(flatten);
        out.writeBoolean(includeMetadata);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<NeuralStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<NeuralStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NeuralStatsNodeResponse::readStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Map<String, Object> formattedInfoStats = formatStats(infoStats);
        builder.startObject(INFO_KEY_PREFIX);
        builder.mapContents(formattedInfoStats);
        builder.endObject();

        Map<String, Object> formattedAggregatedNodeStats = formatStats(aggregatedNodeStats);
        builder.startObject(AGGREGATED_NODES_KEY_PREFIX);
        builder.mapContents(formattedAggregatedNodeStats);
        builder.endObject();

        Map<String, Object> formattedNodeEventStats = formatNodeEventStats(nodeIdToNodeEventStats);
        builder.startObject(NODES_KEY_PREFIX);
        builder.mapContents(formattedNodeEventStats);
        builder.endObject();

        return builder;
    }

    private Map<String, Object> formatStats(Map<String, StatSnapshot<?>> rawStats) {
        if (flatten) {
            return getFlattenedStats(rawStats);
        }
        return writeNestedMapWithDotNotation(rawStats, includeMetadata);
    }

    private Map<String, Object> getFlattenedStats(Map<String, StatSnapshot<?>> rawStats) {
        if (includeMetadata) {
            // Safe downcast to object
            return (Map<String, Object>) (Map) rawStats;
        }
        return rawStats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));
    }

    private Map<String, Object> formatNodeEventStats(Map<String, Map<String, StatSnapshot<?>>> rawNodeStats) {
        // Format nested maps for node event stats;
        Map<String, Object> formattedNodeIdsToNodeEventStats = new HashMap<>();
        for (Map.Entry<String, Map<String, StatSnapshot<?>>> nodeEventStats : rawNodeStats.entrySet()) {
            String nodeId = nodeEventStats.getKey();

            // Format each nested map
            Map<String, Object> formattedNodeStats = formatStats(nodeEventStats.getValue());
            formattedNodeIdsToNodeEventStats.put(nodeId, formattedNodeStats);
        }
        return formattedNodeIdsToNodeEventStats;
    }

    private Map<String, Object> writeNestedMapWithDotNotation(Map<String, StatSnapshot<?>> dotMap, boolean includeMetadata) {
        Map<String, Object> nestedMap = new HashMap<>();

        // For every key, iteratively create or access maps to put the final value;
        for (Map.Entry<String, StatSnapshot<?>> entry : dotMap.entrySet()) {
            String[] parts = entry.getKey().split("\\.");
            Map<String, Object> current = nestedMap;

            // Navigate to the end of the nested map
            for (int i = 0; i < parts.length - 1; i++) {
                // This is the only place we're putting things into nestedMap, so this cast is safe
                // So long as we verify there are no stat path collisions (done in unit tests)
                current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new HashMap<>());
            }

            // If include metadata, put object in and it'll be written toXContent via StatSnapshot<T> implementation
            // Otherwise, provide the raw value
            Object value = includeMetadata ? entry.getValue() : entry.getValue().getValue();
            current.put(parts[parts.length - 1], value);
        }
        return nestedMap;
    }
}
