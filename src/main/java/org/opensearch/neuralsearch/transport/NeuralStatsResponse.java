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

/**
 * NeuralStatsResponse consists of the aggregated responses from the nodes
 */
public class NeuralStatsResponse extends BaseNodesResponse<NeuralStatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";
    private Map<String, Object> stats;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public NeuralStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(NeuralStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
        stats = new TreeMap<>(in.readMap());
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
        Map<String, Object> stats
    ) {
        super(clusterName, nodes, failures);
        this.stats = new TreeMap<>(stats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(stats);
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
        // Return cluster level stats
        Map<String, Object> nestedClusterStats = convertFlatToNestedMap(stats);
        buildNestedMapXContent(builder, nestedClusterStats);

        // Return node level stats
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (NeuralStatsNodeResponse nodesResponse : getNodes()) {
            node = nodesResponse.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);

            Map<String, Object> downcastedNodesResponse = new HashMap<>(nodesResponse.getStatsMap());

            Map<String, Object> nestedMap = convertFlatToNestedMap(downcastedNodesResponse);
            buildNestedMapXContent(builder, nestedMap);
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
            putNested(nestedMap, entry.getKey(), entry.getValue());
        }
        return nestedMap;
    }

    private void putNested(Map<String, Object> map, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = map;

        // Navigate to path in map
        for (int i = 0; i < parts.length - 1; i++) {
            current = (Map<String, Object>) current.computeIfAbsent(parts[i], k -> new HashMap<String, Object>());
        }

        // Put object at map path
        current.put(parts[parts.length - 1], value);
    }
}
