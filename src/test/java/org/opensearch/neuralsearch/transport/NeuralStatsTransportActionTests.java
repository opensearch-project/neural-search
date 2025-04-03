/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.stats.events.TimestampedEventStatSnapshot;
import org.opensearch.neuralsearch.stats.info.CountableInfoStatSnapshot;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatsManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NeuralStatsTransportActionTests extends OpenSearchTestCase {

    @Mock
    private ThreadPool threadPool;

    @Mock
    private ClusterService clusterService;

    @Mock
    private TransportService transportService;

    @Mock
    private ActionFilters actionFilters;

    @Mock
    private EventStatsManager eventStatsManager;

    @Mock
    private InfoStatsManager infoStatsManager;

    private NeuralStatsTransportAction transportAction;
    private ClusterName clusterName;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        clusterName = new ClusterName("test-cluster");
        when(clusterService.getClusterName()).thenReturn(clusterName);

        transportAction = new NeuralStatsTransportAction(
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            eventStatsManager,
            infoStatsManager
        );
    }

    public void test_newResponse() {
        // Create inputs
        NeuralStatsInput input = new NeuralStatsInput();
        NeuralStatsRequest request = new NeuralStatsRequest(new String[] {}, input);
        List<NeuralStatsNodeResponse> responses = new ArrayList<>();
        List<FailedNodeException> failures = new ArrayList<>();

        // Execute response
        NeuralStatsResponse response = transportAction.newResponse(request, responses, failures);

        // Validate response
        assertNotNull(response);
        assertEquals(clusterName, response.getClusterName());
        assertTrue(response.getNodes().isEmpty());
    }

    public void test_newResponseMultipleNodesStateAndEventStats() {
        // Create inputs
        EnumSet<EventStatName> eventStats = EnumSet.of(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS);
        EnumSet<InfoStatName> infoStats = EnumSet.of(InfoStatName.TEXT_EMBEDDING_PROCESSORS);

        NeuralStatsInput input = NeuralStatsInput.builder().eventStatNames(eventStats).infoStatNames(infoStats).build();
        NeuralStatsRequest request = new NeuralStatsRequest(new String[] {}, input);

        // Create multiple nodes
        DiscoveryNode node1 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("test-node-1");
        DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node2.getId()).thenReturn("test-node-2");

        // Create event stats
        TimestampedEventStatSnapshot snapshot1 = TimestampedEventStatSnapshot.builder()
            .statName(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
            .value(17)
            .minutesSinceLastEvent(3)
            .trailingIntervalValue(5)
            .build();

        TimestampedEventStatSnapshot snapshot2 = TimestampedEventStatSnapshot.builder()
            .statName(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
            .value(33)
            .minutesSinceLastEvent(0)
            .trailingIntervalValue(15)
            .build();

        Map<EventStatName, TimestampedEventStatSnapshot> nodeStats1 = new HashMap<>();
        nodeStats1.put(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, snapshot1);
        Map<EventStatName, TimestampedEventStatSnapshot> nodeStats2 = new HashMap<>();
        nodeStats2.put(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, snapshot2);

        List<NeuralStatsNodeResponse> responses = Arrays.asList(
            new NeuralStatsNodeResponse(node1, nodeStats1),
            new NeuralStatsNodeResponse(node2, nodeStats2)
        );

        // Create info stats
        CountableInfoStatSnapshot infoStatSnapshot = new CountableInfoStatSnapshot(InfoStatName.TEXT_EMBEDDING_PROCESSORS);
        infoStatSnapshot.incrementBy(2001L);
        Map<InfoStatName, StatSnapshot<?>> mockInfoStats = new HashMap<>();
        mockInfoStats.put(InfoStatName.TEXT_EMBEDDING_PROCESSORS, infoStatSnapshot);
        when(infoStatsManager.getStats(infoStats)).thenReturn(mockInfoStats);

        List<FailedNodeException> failures = new ArrayList<>();

        // Execute
        NeuralStatsResponse response = transportAction.newResponse(request, responses, failures);

        // Verify node level event stats
        assertNotNull(response);
        assertEquals(2, response.getNodes().size());

        Map<String, Map<String, StatSnapshot<?>>> nodeEventStats = response.getNodeIdToNodeEventStats();

        assertNotNull(nodeEventStats);
        assertTrue(nodeEventStats.containsKey("test-node-1"));
        assertTrue(nodeEventStats.containsKey("test-node-2"));

        StatSnapshot<?> node1Stat = nodeEventStats.get("test-node-1").get(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getFullPath());
        assertEquals(17L, node1Stat.getValue());

        StatSnapshot<?> node2Stat = nodeEventStats.get("test-node-2").get(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getFullPath());
        assertEquals(33L, node2Stat.getValue());

        Map<String, StatSnapshot<?>> aggregatedNodeStats = response.getAggregatedNodeStats();
        assertNotNull(aggregatedNodeStats);

        // Validate timestamped event stats aggregated correctly
        String aggregatedStatPath = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getFullPath();
        TimestampedEventStatSnapshot aggregatedStat = (TimestampedEventStatSnapshot) aggregatedNodeStats.get(aggregatedStatPath);
        assertNotNull(aggregatedStat);

        assertEquals(50L, aggregatedStat.getValue().longValue());
        assertEquals(0L, aggregatedStat.getMinutesSinceLastEvent());
        assertEquals(20L, aggregatedStat.getTrailingIntervalValue());
        assertEquals(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, aggregatedStat.getStatName());

        // Verify info stats
        Map<String, StatSnapshot<?>> resultStats = response.getInfoStats();
        assertNotNull(resultStats);

        // Verify info stats
        String infoStatPath = InfoStatName.TEXT_EMBEDDING_PROCESSORS.getFullPath();
        StatSnapshot<?> resultStateSnapshot = resultStats.get(infoStatPath);
        assertNotNull(resultStateSnapshot);
        assertEquals(2001L, resultStateSnapshot.getValue());
    }

    public void test_nodeOperation() {
        EnumSet<EventStatName> eventStats = EnumSet.of(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS);
        NeuralStatsInput input = NeuralStatsInput.builder().eventStatNames(eventStats).build();

        NeuralStatsRequest request = new NeuralStatsRequest(new String[] {}, input);
        NeuralStatsNodeRequest nodeRequest = new NeuralStatsNodeRequest(request);

        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(clusterService.localNode()).thenReturn(localNode);

        TimestampedEventStatSnapshot snapshot2 = TimestampedEventStatSnapshot.builder()
            .statName(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS)
            .value(33)
            .minutesSinceLastEvent(3)
            .trailingIntervalValue(15)
            .build();

        Map<EventStatName, TimestampedEventStatSnapshot> mockStats = new HashMap<>();
        mockStats.put(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, snapshot2);
        when(eventStatsManager.getTimestampedEventStatSnapshots(eventStats)).thenReturn(mockStats);

        NeuralStatsNodeResponse response = transportAction.nodeOperation(nodeRequest);

        assertNotNull(response);
        assertEquals(localNode, response.getNode());

        Map<EventStatName, TimestampedEventStatSnapshot> responseStats = response.getStats();
        assertFalse(responseStats.isEmpty());

        TimestampedEventStatSnapshot stat = responseStats.get(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS);
        assertNotNull(stat);
        assertEquals(33L, stat.getValue().longValue());
        assertEquals(3L, stat.getMinutesSinceLastEvent());
        assertEquals(15L, stat.getTrailingIntervalValue());
        assertEquals(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, stat.getStatName());

        verify(eventStatsManager).getTimestampedEventStatSnapshots(eventStats);
    }
}
