/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
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

/**
 * Rest action handler for the neural stats API
 * Calculates info stats and aggregates event stats from nodes and returns them in the response
 */
@Log4j2
@AllArgsConstructor
public class RestNeuralStatsAction extends BaseRestHandler {
    /**
     * Query parameter name to request flattened stat paths as keys
     */
    public static final String FLATTEN_PARAM = "flat_stat_paths";

    /**
     * Query parameter name to include metadata
     */
    public static final String INCLUDE_METADATA_PARAM = "include_metadata";

    /**
     * Regex for valid params, containing only alphanumeric, -, or _
     */
    public static final String PARAM_REGEX = "^[A-Za-z0-9-_]+$";

    /**
     * Max length for an individual query or path param
     */
    public static final int MAX_PARAM_LENGTH = 255;

    private static final String NAME = "neural_stats_action";

    private static final Set<String> EVENT_STAT_NAMES = EnumSet.allOf(EventStatName.class)
        .stream()
        .map(EventStatName::getNameString)
        .map(str -> str.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    private static final Set<String> STATE_STAT_NAMES = EnumSet.allOf(InfoStatName.class)
        .stream()
        .map(InfoStatName::getNameString)
        .map(str -> str.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    private static final List<Route> ROUTES = ImmutableList.of(
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/{nodeId}/stats/"),
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/{nodeId}/stats/{stat}"),
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/stats/"),
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/stats/{stat}")
    );

    private static final Set<String> RESPONSE_PARAMS = ImmutableSet.of("nodeId", "stat", INCLUDE_METADATA_PARAM, FLATTEN_PARAM);

    /**
     * Validates a param string if its under the max length and matches simple string pattern
     * @param param the string to validate
     * @return whether it's valid
     */
    public static boolean isValidParamString(String param) {
        return param.matches(PARAM_REGEX) && param.length() < MAX_PARAM_LENGTH;
    }

    private NeuralSearchSettingsAccessor settingsAccessor;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ROUTES;
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (settingsAccessor.isStatsEnabled() == false) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.FORBIDDEN, "Stats endpoint is disabled"));
        }

        // Read inputs and convert to BaseNodesRequest with correct info configured
        NeuralStatsRequest neuralStatsRequest = createNeuralStatsRequest(request);

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
    private NeuralStatsRequest createNeuralStatsRequest(RestRequest request) {
        NeuralStatsInput neuralStatsInput = createNeuralStatsInputFromRequestParams(request);
        String[] nodeIdsArr = neuralStatsInput.getNodeIds().toArray(new String[0]);

        NeuralStatsRequest neuralStatsRequest = new NeuralStatsRequest(nodeIdsArr, neuralStatsInput);
        neuralStatsRequest.timeout(request.param("timeout"));

        return neuralStatsRequest;
    }

    private NeuralStatsInput createNeuralStatsInputFromRequestParams(RestRequest request) {
        NeuralStatsInput neuralStatsInput = new NeuralStatsInput();

        // Parse specified nodes
        Optional<String[]> nodeIds = splitCommaSeparatedParam(request, "nodeId");
        if (nodeIds.isPresent()) {
            // Ignore node ids that don't pattern match
            List<String> validFormatNodeIds = Arrays.stream(nodeIds.get()).filter(this::isValidNodeId).toList();
            neuralStatsInput.getNodeIds().addAll(validFormatNodeIds);
        }

        // Parse query parameters
        boolean flatten = request.paramAsBoolean(FLATTEN_PARAM, false);
        neuralStatsInput.setFlatten(flatten);

        boolean includeMetadata = request.paramAsBoolean(INCLUDE_METADATA_PARAM, false);
        neuralStatsInput.setIncludeMetadata(includeMetadata);

        // Determine which stat names to retrieve based on user parameters
        Optional<String[]> stats = splitCommaSeparatedParam(request, "stat");

        if (stats.isPresent() == false) {
            // No specific stats requested, add all stats by default
            addAllStats(neuralStatsInput);
            return neuralStatsInput;
        }

        // Process requested stats
        boolean anyStatAdded = processRequestedStats(stats.get(), neuralStatsInput);

        // If no valid stats were added, fall back to all stats
        if (anyStatAdded == false) {
            addAllStats(neuralStatsInput);
        }

        return neuralStatsInput;
    }

    private boolean processRequestedStats(String[] stats, NeuralStatsInput neuralStatsInput) {
        boolean statAdded = false;

        for (String stat : stats) {
            // Validate parameter
            String normalizedStat = stat.toLowerCase(Locale.ROOT);
            if (isValidParamString(normalizedStat) == false) {
                log.info("Invalid stat name parameter format: {}", normalizedStat);
                continue;
            }

            if (EVENT_STAT_NAMES.contains(normalizedStat)) {
                neuralStatsInput.getEventStatNames().add(EventStatName.from(normalizedStat));
                statAdded = true;
            } else if (STATE_STAT_NAMES.contains(normalizedStat)) {
                neuralStatsInput.getInfoStatNames().add(InfoStatName.from(normalizedStat));
                statAdded = true;
            }
            log.info("Non-existent stat name parsed: {}", normalizedStat);
        }
        return statAdded;
    }

    private void addAllStats(NeuralStatsInput neuralStatsInput) {
        neuralStatsInput.getEventStatNames().addAll(EnumSet.allOf(EventStatName.class));
        neuralStatsInput.getInfoStatNames().addAll(EnumSet.allOf(InfoStatName.class));
    }

    private Optional<String[]> splitCommaSeparatedParam(RestRequest request, String paramName) {
        return Optional.ofNullable(request.param(paramName)).map(s -> s.split(","));
    }

    private boolean isValidNodeId(String nodeId) {
        // Validate node id parameter
        return isValidParamString(nodeId) && nodeId.length() == 22;
    }
}
