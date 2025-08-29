/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.Version;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsRequest;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
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
     * Path parameter name for specified stats
     */
    public static final String STAT_PARAM = "stat";

    /**
     * Path parameter name for specified node ids
     */
    public static final String NODE_ID_PARAM = "nodeId";

    /**
     * Query parameter name to request flattened stat paths as keys
     */
    public static final String FLATTEN_PARAM = "flat_stat_paths";

    /**
     * Query parameter name to include metadata
     */
    public static final String INCLUDE_METADATA_PARAM = "include_metadata";

    /**
     * Query parameter name to include individual nodes data
     */
    public static final String INCLUDE_INDIVIDUAL_NODES_PARAM = "include_individual_nodes";

    /**
     * Query parameter name to include individual nodes data
     */
    public static final String INCLUDE_ALL_NODES_PARAM = "include_all_nodes";

    /**
     * Query parameter name to include individual nodes data
     */
    public static final String INCLUDE_INFO_PARAM = "include_info";

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

    private static final Set<String> INFO_STAT_NAMES = EnumSet.allOf(InfoStatName.class)
        .stream()
        .map(InfoStatName::getNameString)
        .map(str -> str.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    private static final Set<String> METRIC_STAT_NAMES = EnumSet.allOf(MetricStatName.class)
        .stream()
        .map(MetricStatName::getNameString)
        .map(str -> str.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    private static final List<Route> ROUTES = ImmutableList.of(
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/{nodeId}/stats/"),
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/{nodeId}/stats/{stat}"),
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/stats/"),
        new Route(RestRequest.Method.GET, NEURAL_BASE_URI + "/stats/{stat}")
    );

    private static final Set<String> RESPONSE_PARAMS = ImmutableSet.of(
        NODE_ID_PARAM,
        STAT_PARAM,
        INCLUDE_METADATA_PARAM,
        FLATTEN_PARAM,
        INCLUDE_INDIVIDUAL_NODES_PARAM,
        INCLUDE_ALL_NODES_PARAM,
        INCLUDE_INFO_PARAM
    );

    /**
     * Validates a param string if its under the max length and matches simple string pattern
     * @param param the string to validate
     * @return whether it's valid
     */
    public static boolean isValidParamString(String param) {
        return param.matches(PARAM_REGEX) && param.length() < MAX_PARAM_LENGTH;
    }

    private NeuralSearchSettingsAccessor settingsAccessor;
    private NeuralSearchClusterUtil clusterUtil;

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
        Optional<String[]> nodeIds = splitCommaSeparatedParam(request, NODE_ID_PARAM);
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

        boolean includeIndividualNodes = request.paramAsBoolean(INCLUDE_INDIVIDUAL_NODES_PARAM, true);
        neuralStatsInput.setIncludeIndividualNodes(includeIndividualNodes);

        boolean includeAllNodes = request.paramAsBoolean(INCLUDE_ALL_NODES_PARAM, true);
        neuralStatsInput.setIncludeAllNodes(includeAllNodes);

        boolean includeInfo = request.paramAsBoolean(INCLUDE_INFO_PARAM, true);
        neuralStatsInput.setIncludeInfo(includeInfo);

        // Process requested stats parameters
        processStatsRequestParameters(request, neuralStatsInput);

        return neuralStatsInput;
    }

    private void processStatsRequestParameters(RestRequest request, NeuralStatsInput neuralStatsInput) {
        // Determine which stat names to retrieve based on user parameters
        Optional<String[]> optionalStats = splitCommaSeparatedParam(request, STAT_PARAM);
        Version minClusterVersion = clusterUtil.getClusterMinVersion();

        if (optionalStats.isPresent() == false || optionalStats.get().length == 0) {
            // No specific stats requested, add all stats by default
            addAllStats(neuralStatsInput, minClusterVersion);
            return;
        }

        String[] stats = optionalStats.get();
        Set<String> invalidStatNames = new HashSet<>();
        boolean includeEventsAndMetrics = neuralStatsInput.includeEventsAndMetrics();
        boolean includeInfo = neuralStatsInput.isIncludeInfo();

        for (String stat : stats) {
            // Validate parameter
            String normalizedStat = stat.toLowerCase(Locale.ROOT);
            if (!isValidParamString(normalizedStat) || !isValidEventOrInfoStatName(normalizedStat)) {
                invalidStatNames.add(normalizedStat);
                continue;
            }
            if (includeInfo && InfoStatName.isValidName(normalizedStat)) {
                InfoStatName infoStatName = InfoStatName.from(normalizedStat);
                if (infoStatName.version().onOrBefore(minClusterVersion)) {
                    neuralStatsInput.getInfoStatNames().add(InfoStatName.from(normalizedStat));
                }
            } else if (includeEventsAndMetrics) {
                if (EventStatName.isValidName(normalizedStat)) {
                    EventStatName eventStatName = EventStatName.from(normalizedStat);
                    if (eventStatName.version().onOrBefore(minClusterVersion)) {
                        neuralStatsInput.getEventStatNames().add(EventStatName.from(normalizedStat));
                    }
                } else if (MetricStatName.isValidName(normalizedStat)) {
                    MetricStatName metricStatName = MetricStatName.from(normalizedStat);
                    if (metricStatName.version().onOrBefore(minClusterVersion)) {
                        neuralStatsInput.getMetricStatNames().add(MetricStatName.from(normalizedStat));
                    }
                }
            }
        }

        // When we reach this block, we must have added at least one stat to the input, or else invalid stats will be
        // non empty. So throwing this exception here without adding all covers the empty input case.
        if (invalidStatNames.isEmpty() == false) {
            throw new IllegalArgumentException(
                unrecognized(request, invalidStatNames, Sets.union(EVENT_STAT_NAMES, INFO_STAT_NAMES), STAT_PARAM)
            );
        }
    }

    private void addAllStats(NeuralStatsInput neuralStatsInput, Version minVersion) {
        if (minVersion == Version.CURRENT) {
            if (neuralStatsInput.isIncludeInfo()) {
                neuralStatsInput.getInfoStatNames().addAll(EnumSet.allOf(InfoStatName.class));
            }
            if (neuralStatsInput.includeEventsAndMetrics()) {
                neuralStatsInput.getEventStatNames().addAll(EnumSet.allOf(EventStatName.class));
                neuralStatsInput.getMetricStatNames().addAll(EnumSet.allOf(MetricStatName.class));
            }
        } else {
            if (neuralStatsInput.isIncludeInfo()) {
                neuralStatsInput.getInfoStatNames()
                    .addAll(
                        EnumSet.allOf(InfoStatName.class)
                            .stream()
                            .filter(statName -> statName.version().onOrBefore(minVersion))
                            .collect(Collectors.toCollection(() -> EnumSet.noneOf(InfoStatName.class)))
                    );
            }
            if (neuralStatsInput.includeEventsAndMetrics()) {
                neuralStatsInput.getEventStatNames()
                    .addAll(
                        EnumSet.allOf(EventStatName.class)
                            .stream()
                            .filter(statName -> statName.version().onOrBefore(minVersion))
                            .collect(Collectors.toCollection(() -> EnumSet.noneOf(EventStatName.class)))
                    );
                neuralStatsInput.getMetricStatNames()
                    .addAll(
                        EnumSet.allOf(MetricStatName.class)
                            .stream()
                            .filter(statName -> statName.version().onOrBefore(minVersion))
                            .collect(Collectors.toCollection(() -> EnumSet.noneOf(MetricStatName.class)))
                    );
            }
        }
    }

    private Optional<String[]> splitCommaSeparatedParam(RestRequest request, String paramName) {
        return Optional.ofNullable(request.param(paramName)).map(s -> s.split(","));
    }

    private boolean isValidNodeId(String nodeId) {
        // Validate node id parameter
        return isValidParamString(nodeId) && nodeId.length() == 22;
    }

    private boolean isValidEventOrInfoStatName(String statName) {
        return InfoStatName.isValidName(statName) || EventStatName.isValidName(statName);
    }

}
