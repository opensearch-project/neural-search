/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.stats.info.SettableInfoStatSnapshot;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class NeuralStatsResponseTests extends OpenSearchTestCase {

    private ClusterName clusterName;
    private List<NeuralStatsNodeResponse> nodes;
    private List<FailedNodeException> failures;
    private Map<String, StatSnapshot<?>> infoStats;
    private Map<String, StatSnapshot<?>> aggregatedNodeStats;
    private Map<String, Map<String, StatSnapshot<?>>> nodeIdToNodeEventStats;

    @Mock
    private StreamInput mockStreamInput;

    @Mock
    private StreamOutput mockStreamOutput;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        clusterName = new ClusterName("test-cluster");
        nodes = new ArrayList<>();
        failures = new ArrayList<>();
        infoStats = new HashMap<>();
        aggregatedNodeStats = new HashMap<>();
        nodeIdToNodeEventStats = new HashMap<>();
    }

    public void test_constructor() throws IOException {
        when(mockStreamInput.readString()).thenReturn("test-cluster");
        when(mockStreamInput.readList(any())).thenReturn((List) nodes).thenReturn((List) failures);
        when(mockStreamInput.readMap())
                .thenReturn((Map) infoStats)
                .thenReturn((Map) aggregatedNodeStats)
                .thenReturn((Map) nodeIdToNodeEventStats);

        // Booleans as bytes
        when(mockStreamInput.readByte()).thenReturn((byte) 1).thenReturn((byte) 0);

        NeuralStatsResponse response = new NeuralStatsResponse(mockStreamInput);

        assertEquals("test-cluster", response.getClusterName().value());
        assertEquals(nodes, response.getNodes());
        assertEquals(failures, response.failures());
        assertEquals(infoStats, response.getInfoStats());
        assertEquals(aggregatedNodeStats, response.getAggregatedNodeStats());
        assertEquals(nodeIdToNodeEventStats, response.getNodeIdToNodeEventStats());
        assertTrue(response.isFlatten());
        assertFalse(response.isIncludeMetadata());
    }

    public void test_writeTo() throws IOException {
        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            true,
            false
        );

        response.writeTo(mockStreamOutput);

        verify(mockStreamOutput).writeString(clusterName.value());

        // 2 calls, one by BaseNodesResponse, one by class under test
        verify(mockStreamOutput, times(2)).writeList(nodes);

        // 2 calls, one by BaseNodesResponse, one by class under test
        verify(mockStreamOutput, times(2)).writeList(failures);
        verify(mockStreamOutput, times(3)).writeMap(any());
        verify(mockStreamOutput).writeBoolean(true);
        verify(mockStreamOutput).writeBoolean(false);
    }

    public void test_toXContent_emptyStats() throws IOException {
        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            false,
            true
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        response.toXContent(builder, null);
        builder.endObject();

        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        assertTrue(responseMap.containsKey("nodes"));
        assertTrue(((Map<String, Object>) responseMap.get("nodes")).isEmpty());
    }

    public void test_toXContent_withInfoStats() throws IOException {
        StatSnapshot<Long> mockSnapshot = mock(StatSnapshot.class);
        when(mockSnapshot.getValue()).thenReturn(42L);
        infoStats.put("test.nested.stat", mockSnapshot);

        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            false,
            false
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        response.toXContent(builder, null);
        builder.endObject();
        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        Map<String, Object> infoMap = (Map<String, Object>) responseMap.get("info");
        Map<String, Object> testMap = (Map<String, Object>) infoMap.get("test");
        Map<String, Object> nestedMap = (Map<String, Object>) testMap.get("nested");
        assertEquals(42, nestedMap.get("stat"));
    }

    public void test_toXContent_withStats_flattened() throws IOException {
        StatSnapshot<Long> mockSnapshot = mock(StatSnapshot.class);
        when(mockSnapshot.getValue()).thenReturn(42L);
        infoStats.put("test.nested.stat", mockSnapshot);

        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            true,
            false
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        response.toXContent(builder, null);
        builder.endObject();
        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        Map<String, Object> infoMap = (Map<String, Object>) responseMap.get("info");
        assertEquals(42, infoMap.get("test.nested.stat"));
    }

    public void test_toXContent_withNodeStats() throws IOException {
        StatSnapshot<Long> mockSnapshot = mock(StatSnapshot.class);
        when(mockSnapshot.getValue()).thenReturn(42L);
        Map<String, StatSnapshot<?>> nodeStats = new HashMap<>();
        nodeStats.put("test.stat", mockSnapshot);
        nodeIdToNodeEventStats.put("node1", nodeStats);

        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            false,
            false
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        response.toXContent(builder, null);
        builder.endObject();
        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        Map<String, Object> nodesMap = (Map<String, Object>) responseMap.get("nodes");
        Map<String, Object> node1Stats = (Map<String, Object>) nodesMap.get("node1");
        Map<String, Object> node1StatsTest = (Map<String, Object>) node1Stats.get("test");
        assertEquals(42, node1StatsTest.get("stat"));
    }

    public void test_toXContent_withNodeStats_flattened() throws IOException {
        StatSnapshot<Long> mockSnapshot = mock(StatSnapshot.class);
        when(mockSnapshot.getValue()).thenReturn(42L);
        Map<String, StatSnapshot<?>> nodeStats = new HashMap<>();
        nodeStats.put("test.stat", mockSnapshot);
        nodeIdToNodeEventStats.put("node1", nodeStats);

        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            true,
            false
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        response.toXContent(builder, null);
        builder.endObject();
        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        Map<String, Object> nodesMap = (Map<String, Object>) responseMap.get("nodes");
        Map<String, Object> node1Stats = (Map<String, Object>) nodesMap.get("node1");
        assertEquals(42, node1Stats.get("test.stat"));
    }

    public void test_toXContent_withMetadata() throws IOException {
        // Use a real stat snapshot here to use real toXContent functionality
        SettableInfoStatSnapshot<String> infoStatSnapshot = new SettableInfoStatSnapshot<>(
            InfoStatName.CLUSTER_VERSION,
            "For crying out loud!"
        );

        infoStats.put("test.nested.stat", infoStatSnapshot);

        NeuralStatsResponse response = new NeuralStatsResponse(
            clusterName,
            nodes,
            failures,
            infoStats,
            aggregatedNodeStats,
            nodeIdToNodeEventStats,
            false,
            true
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        response.toXContent(builder, null);
        builder.endObject();

        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        Map<String, Object> infoMap = (Map<String, Object>) responseMap.get("info");
        Map<String, Object> testMap = (Map<String, Object>) infoMap.get("test");
        Map<String, Object> nestedMap = (Map<String, Object>) testMap.get("nested");
        Map<String, Object> statMap = (Map<String, Object>) nestedMap.get("stat");

        // Verify fields
        assertEquals(infoStatSnapshot.getValue(), statMap.get(StatSnapshot.VALUE_FIELD));
        assertEquals(InfoStatName.CLUSTER_VERSION.getStatType().getTypeString(), statMap.get(StatSnapshot.STAT_TYPE_FIELD));
    }
}
