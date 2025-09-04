/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.opensearch.core.common.Strings;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupAction;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupRequest;
import com.google.common.collect.ImmutableList;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;
import java.util.Locale;

import static org.opensearch.action.support.IndicesOptions.strictExpandOpen;

/**
 * RestHandler for SEISMIC index warmup API.
 * API provides the ability for a user to load forward index and clustered posting for SEISMIC indices
 * into memory.
 */
public class RestNeuralSparseWarmupHandler extends BaseRestHandler {
    private static final String URL_PATH = "/warmup/{index}";
    public static String NAME = "neural_sparse_warmup_action";
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;

    public RestNeuralSparseWarmupHandler(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s%s", NeuralSearch.NEURAL_BASE_URI, URL_PATH))
        );
    }

    /**
     * @param request RestRequest of warm up cache
     * @param client NodeClient to execute actions according to request
     * @return RestChannelConsumer
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        NeuralSparseWarmupRequest neuralSparseWarmupRequest = createNeuralSparseWarmupRequest(request);
        return channel -> client.execute(
            NeuralSparseWarmupAction.INSTANCE,
            neuralSparseWarmupRequest,
            new RestToXContentListener<>(channel)
        );
    }

    // Create a warm up cache request by processing the rest request and validating the indices
    private NeuralSparseWarmupRequest createNeuralSparseWarmupRequest(RestRequest request) {
        String[] indexNames = Strings.splitStringByCommaToArray(request.param("index"));
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), strictExpandOpen(), indexNames);
        RestUtils.validateSparseIndices(indices, clusterService, NAME);

        return new NeuralSparseWarmupRequest(indexNames);
    }
}
