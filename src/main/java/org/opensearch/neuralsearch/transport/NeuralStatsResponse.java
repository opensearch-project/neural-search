/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * NeuralStatsResponse consists of the aggregated responses from the nodes
 */
public class NeuralStatsResponse extends BaseNodesResponse<NeuralStatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";
    private Map<String, Object> stats;
    private boolean flatten;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public NeuralStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(NeuralStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
        stats = new TreeMap<>(in.readMap());
        flatten = in.readBoolean();
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of NeuralStatsNodeResponses
     * @param failures List of failures from nodes
     * @param stats // TODO
     */
    public NeuralStatsResponse(
        ClusterName clusterName,
        List<NeuralStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        Map<String, Object> stats,
        boolean flatten
    ) {
        super(clusterName, nodes, failures);
        this.stats = new TreeMap<>(stats);
        this.flatten = flatten;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(stats);
        out.writeBoolean(flatten);
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
        Map<String, Object> clusterStats = stats;
        if (flatten == false) {
            clusterStats = convertFlatToNestedMap(stats);
        }
        buildNestedMapXContent(builder, clusterStats);

        // Build node level stats
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (NeuralStatsNodeResponse nodesResponse : getNodes()) {
            node = nodesResponse.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);

            Map<String, Object> resultNodeStatsMap = nodesResponse.getStats()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getFullPath(), entry -> entry.getValue().getValue()));

            if (flatten == false) {
                resultNodeStatsMap = convertFlatToNestedMap(resultNodeStatsMap);
            }

            buildNestedMapXContent(builder, resultNodeStatsMap);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private void buildNestedMapXContent(XContentBuilder builder, Map<String, Object> map) throws IOException {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                builder.startObject(entry.getKey());
                buildNestedMapXContent(builder, (Map<String, Object>) entry.getValue());
                builder.endObject();
            } else {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
    }

    private Map<String, Object> convertFlatToNestedMap(Map<String, Object> map) {
        Map<String, Object> nestedMap = new TreeMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            createNestedStructure(nestedMap, entry.getKey(), entry.getValue());
        }
        return nestedMap;
    }

    private void createNestedStructure(Map<String, Object> targetMap, String dotNotationPath, Object value) {
        String[] pathParts = dotNotationPath.split("\\.");
        Map<String, Object> currentLevel = targetMap;

        // Navigate through all parts except the last one, creating nested maps as needed
        for (int i = 0; i < pathParts.length - 1; i++) {
            String pathPart = pathParts[i];
            currentLevel = (Map<String, Object>) currentLevel.computeIfAbsent(pathPart, key -> new HashMap<String, Object>());
        }

        // Set the value at the final path location
        String lastPathPart = pathParts[pathParts.length - 1];
        currentLevel.put(lastPathPart, value);
    }
}
