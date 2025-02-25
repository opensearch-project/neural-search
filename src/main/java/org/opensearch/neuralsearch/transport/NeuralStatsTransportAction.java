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
import org.opensearch.neuralsearch.stats.DerivedStatsManager;
import org.opensearch.neuralsearch.stats.EventStatsManager;
import org.opensearch.transport.TransportService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  NeuralStatsTransportAction contains the logic to extract the stats from the nodes
 */
public class NeuralStatsTransportAction extends TransportNodesAction<
    NeuralStatsRequest,
    NeuralStatsResponse,
    NeuralStatsNodeRequest,
    NeuralStatsNodeResponse> {

    private EventStatsManager eventStatsManager;
    private DerivedStatsManager derivedStatsManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param eventStatsManager NeuralStats object
     * @param eventStatsManager Derived stats object
     */
    @Inject
    public NeuralStatsTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        EventStatsManager eventStatsManager,
        DerivedStatsManager derivedStatsManager
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
        this.derivedStatsManager = derivedStatsManager;
    }

    @Override
    protected NeuralStatsResponse newResponse(
        NeuralStatsRequest request,
        List<NeuralStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        Map<String, Object> clusterStats = new HashMap<>(
            derivedStatsManager.getStats(responses.stream().map(NeuralStatsNodeResponse::getStatsMap).toList())
        );
        return new NeuralStatsResponse(clusterService.getClusterName(), responses, failures, clusterStats);
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

        for (String statName : eventStatsManager.getStats().keySet()) {
            statValues.put(statName, eventStatsManager.getStats().get(statName).getValue());
        }
        return new NeuralStatsNodeResponse(clusterService.localNode(), statValues);
    }
}
