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
import org.opensearch.neuralsearch.stats.events.EventStat;
import org.opensearch.neuralsearch.stats.events.EventStatData;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.state.StateStat;
import org.opensearch.neuralsearch.stats.state.StateStatName;
import org.opensearch.neuralsearch.stats.state.StateStatsManager;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.transport.TransportService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  NeuralStatsTransportAction contains the logic to extract the stats from the nodes
 */
public class NeuralStatsTransportAction extends TransportNodesAction<
    NeuralStatsRequest,
    NeuralStatsResponse,
    NeuralStatsNodeRequest,
    NeuralStatsNodeResponse> {
    private static final String AGG_KEY_PREFIX = "all_nodes";

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
        Map<String, Object> combinedStats = new HashMap<>();

        // Sum the map to aggregate
        Map<String, Long> nodeAggregatedStats = aggregateNodesResponses(responses);
        combinedStats.putAll(nodeAggregatedStats);

        // Get state stats
        Map<StateStatName, StateStat<?>> stateStats = stateStatsManager.getStats();

        // Filter state stats
        if (request.getNeuralStatsInput().retrieveAllStats() == false) {
            Map<StateStatName, StateStat<?>> filteredStats = new HashMap<>();
            for (StateStatName stateStatName : request.getNeuralStatsInput().getStateStatNames()) {
                filteredStats.put(stateStatName, stateStats.get(stateStatName));
            }
            stateStats = filteredStats;
        }

        // Convert state stats into <flat path, value>
        Map<String, Object> flatStateStats = stateStats.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getFullPath(), entry -> entry.getValue().getValue()));

        combinedStats.putAll(flatStateStats);

        return new NeuralStatsResponse(
            clusterService.getClusterName(),
            responses,
            failures,
            combinedStats,
            request.getNeuralStatsInput().isFlattenResponse()
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
        Map<EventStatName, EventStat> eventStatsMap = eventStatsManager.getStats();
        Map<EventStatName, EventStatData> eventStatsDataMap = new HashMap<>();

        // Get all stats case
        if (request.getRequest().getNeuralStatsInput().retrieveAllStats()) {
            for (Map.Entry<EventStatName, EventStat> entry : eventStatsMap.entrySet()) {
                eventStatsDataMap.put(entry.getKey(), entry.getValue().getEventStatData());
            }
            return new NeuralStatsNodeResponse(clusterService.localNode(), eventStatsDataMap);
        }

        // Otherwise, filter for requested stats
        for (EventStatName eventStatName : request.getRequest().getNeuralStatsInput().getEventStatNames()) {
            eventStatsDataMap.put(eventStatName, eventStatsManager.getStats().get(eventStatName).getEventStatData());
        }
        return new NeuralStatsNodeResponse(clusterService.localNode(), eventStatsDataMap);
    }

    private Map<String, Long> aggregateNodesResponses(List<NeuralStatsNodeResponse> responses) {
        // Convert node responses into list of Map<Stat path, stat value>
        List<Map<String, Long>> nodeResponsesList = responses.stream()
            .map(NeuralStatsNodeResponse::getStats)
            .map(
                map -> map.entrySet()
                    .stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().getFullPath(), entry -> entry.getValue().getValue()))
            )
            .toList();

        // Sum the counters values from all nodes
        Map<String, Long> summedMap = new HashMap<>();
        for (Map<String, Long> map : nodeResponsesList) {
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                summedMap.merge(String.join(".", AGG_KEY_PREFIX, entry.getKey()), entry.getValue(), Long::sum);
            }
        }
        return summedMap;
    }
}
