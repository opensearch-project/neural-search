/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.neuralsearch.plugin.NeuralSearch.NEURAL_BASE_URI;

@Log4j2
@AllArgsConstructor
public class RestNeuralStatsHandler extends BaseRestHandler {
    private static final String NAME = "neural_stats_action";
    public static final String CLEAR_PARAM = "_clear";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/{nodeId}/stats/"),
            new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/{nodeId}/stats/{stat}"),
            new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/stats/"),
            new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/stats/{stat}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        // Read inputs and convert to BaseNodesRequest with correct info configured
        NeuralStatsRequest neuralStatsRequest = getRequest(request);

        return channel -> client.execute(
            NeuralStatsAction.INSTANCE,
            neuralStatsRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );

    }

    /**
     * Creates a NeuralStatsRequest from a RestRequest
     *
     * @param request Rest request
     * @return NeuralStatsRequest
     */
    private NeuralStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query
        String[] nodeIdsArr = null;
        String nodesIdsStr = request.param("nodeId");
        if (StringUtils.isNotEmpty(nodesIdsStr)) {
            nodeIdsArr = nodesIdsStr.split(",");
        }

        NeuralStatsRequest neuralStatsRequest = new NeuralStatsRequest(nodeIdsArr, new NeuralStatsInput());
        neuralStatsRequest.timeout(request.param("timeout"));

        // TODO : stats filtering to build NeuralStatsInput should go here

        return neuralStatsRequest;
    }
}
