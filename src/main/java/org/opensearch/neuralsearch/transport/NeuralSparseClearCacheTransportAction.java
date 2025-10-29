/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.neuralsearch.sparse.NeuralSparseIndexShard;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport Action to evict neural-sparse indices from Cache. TransportBroadcastByNodeAction will distribute the request to
 * all shards across the cluster for the given indices. For each shard, shardOperation will be called and the
 * indices will be cleared from cache.
 */
public class NeuralSparseClearCacheTransportAction extends TransportBroadcastByNodeAction<
    NeuralSparseClearCacheRequest,
    NeuralSparseClearCacheResponse,
    TransportBroadcastByNodeAction.EmptyResult> {
    private final IndicesService indicesService;

    /**
     * Constructor
     *
     * @param clusterService Service providing access to cluster state and updates
     * @param transportService Service for handling transport-level operations
     * @param actionFilters Filters for pre and post processing of actions
     * @param indexNameExpressionResolver Resolver for index expressions to concrete indices
     * @param indicesService Service for accessing and managing indices
     */
    @Inject
    public NeuralSparseClearCacheTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService
    ) {
        super(
            NeuralSparseClearCacheAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            NeuralSparseClearCacheRequest::new,
            SparseConstants.THREAD_POOL_NAME
        );
        this.indicesService = indicesService;
    }

    /**
     * @param streamInput Input stream to read the serialized result from
     * @return Empty result object read from the input stream
     */
    @Override
    protected EmptyResult readShardResult(StreamInput streamInput) {
        return EmptyResult.readEmptyResultFrom(streamInput);
    }

    /**
     * @param request ClearCacheRequest
     * @param totalShards Total number of shards on which ClearCache was performed
     * @param successfulShards Number of shards that succeeded
     * @param failedShards Number of shards that failed
     * @param emptyResults List of EmptyResult
     * @param shardFailures List of shard failure exceptions
     * @param clusterState ClusterState
     * @return {@link NeuralSparseClearCacheResponse} Response containing results of the cache clear operation
     */
    @Override
    protected NeuralSparseClearCacheResponse newResponse(
        NeuralSparseClearCacheRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> emptyResults,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new NeuralSparseClearCacheResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    /**
     * @param streamInput Input stream to read the serialized request from
     * @return {@link NeuralSparseClearCacheRequest} Cache clear request deserialized from the input stream
     * @throws IOException Throws exception if there is error with stream input
     */
    @Override
    protected NeuralSparseClearCacheRequest readRequestFrom(StreamInput streamInput) throws IOException {
        return new NeuralSparseClearCacheRequest(streamInput);
    }

    /**
     * Operation performed at a shard level on all the shards of given index where the index is removed from the cache.
     *
     * @param request Request containing parameters for the cache clear operation
     * @param shardRouting Routing information for the current shard
     * @return Empty result object indicating operation completion
     */
    @Override
    protected EmptyResult shardOperation(NeuralSparseClearCacheRequest request, ShardRouting shardRouting) throws IOException {
        Index index = shardRouting.shardId().getIndex();
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        NeuralSparseIndexShard neuralSparseIndexShard = new NeuralSparseIndexShard(indexShard);
        neuralSparseIndexShard.clearCache();
        return EmptyResult.INSTANCE;
    }

    /**
     * @param clusterState ClusterState
     * @param request NeuralSparseClearCacheRequest
     * @param concreteIndices Indices in the request
     * @return ShardsIterator with all the shards for given concrete indices
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, NeuralSparseClearCacheRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    /**
     * @param clusterState ClusterState
     * @param request NeuralSparseClearCacheRequest
     * @return ClusterBlockException if there is any global cluster block at a cluster block level of "METADATA_WRITE"
     */
    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState clusterState, NeuralSparseClearCacheRequest request) {
        return clusterState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * @param clusterState ClusterState
     * @param request NeuralSparseClearCacheRequest
     * @param concreteIndices Indices in the request
     * @return ClusterBlockException if there is any cluster block on any of the given indices at a cluster block level of "METADATA_WRITE"
     */
    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState clusterState,
        NeuralSparseClearCacheRequest request,
        String[] concreteIndices
    ) {
        // First validate that all indices are sparse indices
        TransportUtils.validateSparseIndices(clusterState, concreteIndices, "neural_sparse_clear_cache_action");

        return clusterState.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
