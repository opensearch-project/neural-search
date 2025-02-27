/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.neuralsearch.stats.StatSnapshot;
import org.opensearch.neuralsearch.stats.events.EventStatSnapshot;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.state.StateStatName;
import org.opensearch.neuralsearch.stats.state.StateStatsManager;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.transport.TransportService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  NeuralStatsTransportAction contains the logic to extract the stats from the nodes
 */
public class NeuralStatsTransportAction extends TransportNodesAction<
    NeuralStatsRequest,
    NeuralStatsResponse,
    NeuralStatsNodeRequest,
    NeuralStatsNodeResponse> {
    private static final String ALL_NODES_PREFIX = "all_nodes";

    private final EventStatsManager eventStatsManager;
    private final StateStatsManager stateStatsManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param eventStatsManager Event stats object
     * @param stateStatsManager State stats object
     */
    @Inject
    public NeuralStatsTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        EventStatsManager eventStatsManager,
        StateStatsManager stateStatsManager
    ) {
        super(
            NeuralStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NeuralStatsRequest::new,
            NeuralStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NeuralStatsNodeResponse.class
        );
        this.eventStatsManager = eventStatsManager;
        this.stateStatsManager = stateStatsManager;
    }

    @Override
    protected NeuralStatsResponse newResponse(
        NeuralStatsRequest request,
        List<NeuralStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        // Final object that will hold the stats in format Map<ResponsePath, Value>
        Map<String, StatSnapshot<?>> retrievedStats = new HashMap<>();

        // Sum the map to aggregate
        Map<String, StatSnapshot<?>> nodeAggregatedEventStats = aggregateNodesResponses(
            responses,
            request.getNeuralStatsInput().getEventStatNames()
        );

        // Add aggregate to summed map
        retrievedStats.putAll(nodeAggregatedEventStats);

        // Get state stats
        Map<StateStatName, StatSnapshot<?>> stateStats = stateStatsManager.getStats(request.getNeuralStatsInput().getStateStatNames());

        // Convert state stats into <ResponsePath, Value>
        Map<String, StatSnapshot<?>> flatStateStats = stateStats.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getFullPath(), Map.Entry::getValue));

        retrievedStats.putAll(flatStateStats);

        return new NeuralStatsResponse(
            clusterService.getClusterName(),
            responses,
            failures,
            retrievedStats,
            request.getNeuralStatsInput().isFlattenResponse(),
            request.getNeuralStatsInput().isIncludeMetadata()
        );
    }

    @Override
    protected NeuralStatsNodeRequest newNodeRequest(NeuralStatsRequest request) {
        return new NeuralStatsNodeRequest(request);
    }

    @Override
    protected NeuralStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new NeuralStatsNodeResponse(in);
    }

    @Override
    protected NeuralStatsNodeResponse nodeOperation(NeuralStatsNodeRequest request) {
        // Reads from NeuralStats to node level stats on an individual node
        EnumSet<EventStatName> eventStatsToRetrieve = request.getRequest().getNeuralStatsInput().getEventStatNames();
        Map<EventStatName, EventStatSnapshot> eventStatDataMap = eventStatsManager.getEventStatData(eventStatsToRetrieve);

        return new NeuralStatsNodeResponse(clusterService.localNode(), eventStatDataMap);
    }

    private Map<String, StatSnapshot<?>> aggregateNodesResponses(
        List<NeuralStatsNodeResponse> responses,
        EnumSet<EventStatName> statsToRetrieve
    ) {
        // Convert node responses into list of Map<EventStatName, EventStatData>
        List<Map<EventStatName, EventStatSnapshot>> nodeEventStatsList = responses.stream()
            .map(NeuralStatsNodeResponse::getStats)
            .map(map -> map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .toList();

        // Aggregate all events from all responses for all stats to retrieve
        Map<String, StatSnapshot<?>> aggregatedMap = new HashMap<>();
        for (EventStatName eventStatName : statsToRetrieve) {
            Set<EventStatSnapshot> eventStatSnapshotCollection = new HashSet<>();
            for (Map<EventStatName, EventStatSnapshot> eventStats : nodeEventStatsList) {
                eventStatSnapshotCollection.add(eventStats.get(eventStatName));
            }

            EventStatSnapshot aggregatedEventSnapshots = EventStatSnapshot.aggregateEventStatData(eventStatSnapshotCollection);
            aggregatedMap.put(ALL_NODES_PREFIX + "." + eventStatName.getFullPath(), aggregatedEventSnapshots);
        }

        return aggregatedMap;
    }
}
