/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.neuralsearch.sparse.NeuralSparseIndexShard; // This to change
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.indices.IndicesService;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport Action for warming up neural-sparse indices. TransportBroadcastByNodeAction will distribute the request to
 * all shards across the cluster for the given indices. For each shard, shardOperation will be called and the
 * warmup will take place.
 */
public class NeuralSparseWarmupTransportAction extends TransportBroadcastByNodeAction<
    NeuralSparseWarmupRequest,
    NeuralSparseWarmupResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private final IndicesService indicesService;

    /**
     * Constructor
     *
     * @param clusterService Service providing access to cluster state and updates
     * @param transportService Service for handling transport-level operations
     * @param indicesService Service for accessing and managing indices
     * @param actionFilters Filters for pre and post processing of actions
     * @param indexNameExpressionResolver Resolver for index expressions to concrete indices
     */
    @Inject
    public NeuralSparseWarmupTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            NeuralSparseWarmupAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            NeuralSparseWarmupRequest::new,
            SparseConstants.THREAD_POOL_NAME
        );
        this.indicesService = indicesService;
    }

    /**
     * @param in Input stream to read the serialized result from
     * @return Empty result object read from the input stream
     */
    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    /**
     * @param request WarmupRequest
     * @param totalShards Total number of shards on which Warmup was performed
     * @param successfulShards Number of shards that succeeded
     * @param failedShards Number of shards that failed
     * @param emptyResults List of EmptyResult
     * @param shardFailures List of shard failure exceptions
     * @param clusterState ClusterState
     * @return {@link NeuralSparseWarmupResponse} Response containing results of the warmup operation
     */
    @Override
    protected NeuralSparseWarmupResponse newResponse(
        NeuralSparseWarmupRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> emptyResults,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new NeuralSparseWarmupResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    /**
     * @param in Input stream to read the serialized request from
     * @return {@link NeuralSparseWarmupRequest} Warmup request deserialized from the input stream
     * @throws IOException Throws exception if there is error with stream input
     */
    @Override
    protected NeuralSparseWarmupRequest readRequestFrom(StreamInput in) throws IOException {
        return new NeuralSparseWarmupRequest(in);
    }

    /**
     * Operation performed at a shard level on all the shards of given index where the index is warmed up.
     * Any exception thrown here will be caught by the framework and result in shard failure.
     *
     * @param request Request containing parameters for the warmup operation
     * @param shardRouting Routing information for the current shard
     * @return Empty result object indicating operation completion
     */
    @Override
    protected EmptyResult shardOperation(NeuralSparseWarmupRequest request, ShardRouting shardRouting) throws IOException {
        NeuralSparseIndexShard neuralSparseIndexShard = new NeuralSparseIndexShard(
            indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id())
        );
        neuralSparseIndexShard.warmUp();
        return EmptyResult.INSTANCE;
    }

    /**
     * @param state ClusterState
     * @param request NeuralSparseWarmupRequest
     * @param concreteIndices Indices in the request
     * @return ShardsIterator with all the shards for given concrete indices
     */
    @Override
    protected ShardsIterator shards(ClusterState state, NeuralSparseWarmupRequest request, String[] concreteIndices) {
        return state.routingTable().allShards(concreteIndices);
    }

    /**
     * @param state ClusterState
     * @param request NeuralSparseWarmupRequest
     * @return ClusterBlockException if there is any global cluster block at a cluster block level of "METADATA_READ"
     */
    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, NeuralSparseWarmupRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * @param state ClusterState
     * @param request NeuralSparseWarmupRequest
     * @param concreteIndices Indices in the request
     * @return ClusterBlockException if there is any cluster block on any of the given indices at a cluster block level of "METADATA_READ"
     */
    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, NeuralSparseWarmupRequest request, String[] concreteIndices) {
        // First validate that all indices are sparse indices
        TransportUtils.validateSparseIndices(state, concreteIndices, "neural_sparse_warmup_action");

        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
