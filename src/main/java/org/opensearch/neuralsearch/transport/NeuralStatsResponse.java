/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.StatSnapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * NeuralStatsResponse consists of the aggregated responses from the nodes
 */
public class NeuralStatsResponse extends BaseNodesResponse<NeuralStatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";
    private Map<String, StatSnapshot<?>> stats;
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
        Map<String, Object> inputMap = in.readMap();

        // Upcast stats from stream
        Map<String, StatSnapshot<?>> upcastedStats = new HashMap<>();
        inputMap.forEach((key, value) -> {
            if (value instanceof StatSnapshot<?>) {
                upcastedStats.put(key, (StatSnapshot<?>) value);
            }
        });
        this.stats = upcastedStats;
        flatten = in.readBoolean();
        includeMetadata = in.readBoolean();
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of NeuralStatsNodeResponses
     * @param failures List of failures from nodes
     * @param stats
     */
    public NeuralStatsResponse(
        ClusterName clusterName,
        List<NeuralStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        Map<String, StatSnapshot<?>> stats,
        boolean flatten,
        boolean includeMetadata
    ) {
        super(clusterName, nodes, failures);
        this.stats = stats;
        this.flatten = flatten;
        this.includeMetadata = includeMetadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Map<String, Object> downcastedStats = new HashMap<>(stats);
        out.writeMap(downcastedStats);
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
        Map<String, Object> finalStats = applyStatOptions(stats);
        builder.mapContents(finalStats);

        // Build node level stats
        Map<String, Map<String, Object>> nodeIdToStats = processorNodeStats();

        builder.startObject(NODES_KEY);
        for (Map.Entry<String, Map<String, Object>> entry : nodeIdToStats.entrySet()) {
            builder.startObject(entry.getKey());
            builder.mapContents(entry.getValue());
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    private Map<String, Object> applyStatOptions(Map<String, StatSnapshot<?>> rawStats) {
        if (flatten) {
            return getFlattenedStats(rawStats);
        }
        return writeNestedMapWithDotNotation(rawStats, includeMetadata);
    }

    private Map<String, Object> getFlattenedStats(Map<String, StatSnapshot<?>> rawStats) {
        if (includeMetadata) {
            return new HashMap<>(rawStats);
        }
        return rawStats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));
    }

    private Map<String, Map<String, Object>> processorNodeStats() {
        Map<String, Map<String, Object>> results = new HashMap<>();

        String nodeId;
        for (NeuralStatsNodeResponse nodesResponse : getNodes()) {
            nodeId = nodesResponse.getNode().getId();

            // Convert StatNames into paths
            Map<String, StatSnapshot<?>> resultNodeStatsMap = nodesResponse.getStats()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getFullPath(), Map.Entry::getValue));

            // Apply options (flattening, metadata)
            Map<String, Object> nodeStats = applyStatOptions(resultNodeStatsMap);

            // Map each nodeid to its stats
            results.put(nodeId, nodeStats);
        }
        return results;
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
                // So long as we verify there are no stat path collisions
                current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new HashMap<>());
            }

            // If include metadata, put object in and it'll be written to xcontent automatically
            // Otherwise, provide the raw value
            Object value = includeMetadata ? entry.getValue() : entry.getValue().getValue();
            current.put(parts[parts.length - 1], value);
        }
        return nestedMap;
    }
}
