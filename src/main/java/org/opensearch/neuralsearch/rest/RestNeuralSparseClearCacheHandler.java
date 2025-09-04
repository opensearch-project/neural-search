/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheAction;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;
import java.util.Locale;

import static org.opensearch.action.support.IndicesOptions.strictExpandOpen;

/**
 * RestHandler for SEISMIC Clear Cache API.
 * API provides the ability for a user to evict index data from Cache.
 */
@AllArgsConstructor
public class RestNeuralSparseClearCacheHandler extends BaseRestHandler {
    private static final String URL_PATH = "/clear_cache/{index}";
    public static String NAME = "neural_sparse_clear_cache_action";
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * @return name of Clear Cache API action
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * @return Immutable List of Clear Cache API endpoint
     */
    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s%s", NeuralSearch.NEURAL_BASE_URI, URL_PATH))
        );
    }

    /**
     * @param request RestRequest of clear cache
     * @param client NodeClient to execute actions according to request
     * @return RestChannelConsumer
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        NeuralSparseClearCacheRequest clearCacheRequest = createClearCacheRequest(request);
        return channel -> client.execute(NeuralSparseClearCacheAction.INSTANCE, clearCacheRequest, new RestToXContentListener<>(channel));
    }

    // Create a clear cache request by processing the rest request and validating the indices
    private NeuralSparseClearCacheRequest createClearCacheRequest(RestRequest request) {
        String[] indexNames = Strings.splitStringByCommaToArray(request.param("index"));
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), strictExpandOpen(), indexNames);
        RestUtils.validateSparseIndices(indices, clusterService, NAME);

        return new NeuralSparseClearCacheRequest(indexNames);
    }
}
