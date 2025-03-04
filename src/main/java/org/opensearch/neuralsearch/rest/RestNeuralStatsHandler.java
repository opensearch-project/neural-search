/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.StringUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.state.StateStatName;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.plugin.NeuralSearch.NEURAL_BASE_URI;
import static org.opensearch.neuralsearch.processor.util.RestActionUtils.splitCommaSeparatedParam;

@Log4j2
@AllArgsConstructor
public class RestNeuralStatsHandler extends BaseRestHandler {
    private static final String NAME = "neural_stats_action";
    public static final String FLATTEN_PARAM = "flat_keys";
    public static final String INCLUDE_METADATA_PARAM = "include_metadata";

    private static final Set<String> EVENT_STAT_NAMES = EnumSet.allOf(EventStatName.class)
        .stream()
        .map(EventStatName::getName)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());

    private static final Set<String> STATE_STAT_NAMES = EnumSet.allOf(StateStatName.class)
        .stream()
        .map(StateStatName::getName)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());

    private NeuralSearchSettingsAccessor settingsAccessor;

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
        if (settingsAccessor.getIsStatsEnabled() == false) {
            // Process params, or else will automatically return a 400 instead of a 403
            splitCommaSeparatedParam(request, "nodeId");
            splitCommaSeparatedParam(request, "stat");
            request.paramAsBoolean(FLATTEN_PARAM, false);
            request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);

            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN, "Stats endpoint is disabled"));
        }

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

        NeuralStatsInput neuralStatsInput = createNeuralStatsInputFromRequestParams(request);

        NeuralStatsRequest neuralStatsRequest = new NeuralStatsRequest(nodeIdsArr, neuralStatsInput);
        neuralStatsRequest.timeout(request.param("timeout"));

        return neuralStatsRequest;
    }

    NeuralStatsInput createNeuralStatsInputFromRequestParams(RestRequest request) {
        NeuralStatsInput neuralStatsInput = new NeuralStatsInput();

        // Parse specified nodes
        Optional<String[]> nodeIds = splitCommaSeparatedParam(request, "nodeId");
        if (nodeIds.isPresent()) {
            neuralStatsInput.getNodeIds().addAll(Arrays.asList(nodeIds.get()));
        }

        // Parse query parameters
        boolean flatten = request.paramAsBoolean(FLATTEN_PARAM, false);
        neuralStatsInput.setFlatten(flatten);

        boolean includeMetadata = request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);
        neuralStatsInput.setIncludeMetadata(includeMetadata);

        // Determine which stat names to retrieve based on user parameters
        Optional<String[]> stats = splitCommaSeparatedParam(request, "stat");
        boolean retrieveAllStats = true;

        // Add stats to input to retrieve if specified
        if (stats.isPresent()) {
            for (String stat : stats.get()) {
                stat = stat.toLowerCase(Locale.ROOT);

                if (EVENT_STAT_NAMES.contains(stat)) {
                    retrieveAllStats = false;
                    neuralStatsInput.getEventStatNames().add(EventStatName.from(stat));
                } else if (STATE_STAT_NAMES.contains(stat)) {
                    retrieveAllStats = false;
                    neuralStatsInput.getStateStatNames().add(StateStatName.from(stat));
                }
            }
        }

        // If no stats are specified, add all stats to retrieve all by default
        if (retrieveAllStats) {
            neuralStatsInput.getEventStatNames().addAll(EnumSet.allOf(EventStatName.class));
            neuralStatsInput.getStateStatNames().addAll(EnumSet.allOf(StateStatName.class));
        }
        return neuralStatsInput;
    }
}
