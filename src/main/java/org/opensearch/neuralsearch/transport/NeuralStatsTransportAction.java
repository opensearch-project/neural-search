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
import java.util.TreeMap;
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
        // Final object that will hold the stats in format <response path, value>
        Map<String, Object> combinedStats = new TreeMap<>();

        // Aggregate node stats
        List<Map<String, Long>> nodeResponsesList = responses.stream().map(NeuralStatsNodeResponse::getStatsMap).toList();
        Map<String, Long> nodeAggregatedStats = aggregateNodesResponses(nodeResponsesList);

        // Get state stats
        Map<StateStatName, StateStat<?>> stateStats = stateStatsManager.getStats();

        // Convert state stats into <flat path, value>
        Map<String, Object> flatStateStats = stateStats.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getFullPath(), entry -> entry.getValue().getValue()));

        combinedStats.putAll(nodeAggregatedStats);
        combinedStats.putAll(flatStateStats);

        return new NeuralStatsResponse(clusterService.getClusterName(), responses, failures, combinedStats);
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
        Map<String, Long> statValues = new HashMap<>();

        for (EventStatName eventStatName : eventStatsManager.getStats().keySet()) {
            statValues.put(eventStatName.getFullPath(), eventStatsManager.getStats().get(eventStatName).getValue());
        }
        return new NeuralStatsNodeResponse(clusterService.localNode(), statValues);
    }

    private Map<String, Long> aggregateNodesResponses(List<Map<String, Long>> nodeResponses) {
        // Sum the counter values from all nodes
        Map<String, Long> summedMap = new HashMap<>();
        for (Map<String, Long> map : nodeResponses) {
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                summedMap.merge(String.join(".", AGG_KEY_PREFIX, entry.getKey()), entry.getValue(), Long::sum);
            }
        }
        return summedMap;
    }
}
