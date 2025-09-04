/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NeuralSparseWarmupTransportActionTests extends AbstractSparseTestBase {

    @Mock
    private ClusterService clusterService;

    @Mock
    private TransportService transportService;

    @Mock
    private ActionFilters actionFilters;

    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Mock
    private IndicesService indicesService;

    @Mock
    private ClusterState clusterState;

    @Mock
    private ClusterBlocks clusterBlocks;

    @Mock
    private RoutingTable routingTable;

    private NeuralSparseWarmupTransportAction transportAction;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        transportAction = new NeuralSparseWarmupTransportAction(
            clusterService,
            transportService,
            indicesService,
            actionFilters,
            indexNameExpressionResolver
        );
    }

    public void testReadShardResult() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        TransportBroadcastByNodeAction.EmptyResult.INSTANCE.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TransportBroadcastByNodeAction.EmptyResult result = transportAction.readShardResult(in);

        assertNotNull(result);
        assertEquals(TransportBroadcastByNodeAction.EmptyResult.INSTANCE, result);
    }

    public void testNewResponse() {
        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest("test-index");
        int totalShards = 5;
        int successfulShards = 4;
        int failedShards = 1;
        List<TransportBroadcastByNodeAction.EmptyResult> emptyResults = new ArrayList<>();
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();

        NeuralSparseWarmupResponse response = transportAction.newResponse(
            request,
            totalShards,
            successfulShards,
            failedShards,
            emptyResults,
            shardFailures,
            clusterState
        );

        assertNotNull(response);
        assertEquals(totalShards, response.getTotalShards());
        assertEquals(successfulShards, response.getSuccessfulShards());
        assertEquals(failedShards, response.getFailedShards());
        assertEquals(shardFailures, Arrays.asList(response.getShardFailures()));
    }

    public void testReadRequestFrom() throws IOException {
        NeuralSparseWarmupRequest originalRequest = new NeuralSparseWarmupRequest("test-index");

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NeuralSparseWarmupRequest deserializedRequest = transportAction.readRequestFrom(in);

        assertNotNull(deserializedRequest);
        assertArrayEquals(originalRequest.indices(), deserializedRequest.indices());
    }

    public void testShardOperation() throws IOException {
        // Setup
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shardRouting.shardId()).thenReturn(shardId);

        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        Engine.Searcher searcher = mock(Engine.Searcher.class);

        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.acquireSearcher(any())).thenReturn(searcher);
        when(searcher.getIndexReader()).thenReturn(TestsPrepareUtils.prepareIndexReaderWithSparseField(15));

        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);

        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest("test-index");

        // Execute
        TransportBroadcastByNodeAction.EmptyResult result = transportAction.shardOperation(request, shardRouting);

        // Verify
        assertNotNull(result);
        assertEquals(TransportBroadcastByNodeAction.EmptyResult.INSTANCE, result);
        verify(indicesService).indexServiceSafe(index);
        verify(indexService).getShard(0);
    }

    public void testShardOperationWithRuntimeException() {
        // Setup
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(shardRouting.shardId()).thenReturn(shardId);

        when(indicesService.indexServiceSafe(index)).thenThrow(new RuntimeException("Test runtime exception"));

        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest("test-index");

        // Execute and verify exception
        RuntimeException exception = assertThrows(RuntimeException.class, () -> { transportAction.shardOperation(request, shardRouting); });

        assertEquals("Test runtime exception", exception.getMessage());
    }

    public void testShards() {
        String[] concreteIndices = { "index1", "index2" };
        ShardsIterator shardsIterator = mock(ShardsIterator.class);

        when(clusterState.routingTable()).thenReturn(routingTable);
        when(routingTable.allShards(concreteIndices)).thenReturn(shardsIterator);

        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest(concreteIndices);
        ShardsIterator result = transportAction.shards(clusterState, request, concreteIndices);

        assertEquals(shardsIterator, result);
        verify(routingTable).allShards(concreteIndices);
    }

    public void testCheckGlobalBlock() {
        when(clusterState.blocks()).thenReturn(clusterBlocks);
        when(clusterBlocks.globalBlockedException(any())).thenReturn(null);

        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest("test-index");

        assertNull(transportAction.checkGlobalBlock(clusterState, request));
        verify(clusterBlocks).globalBlockedException(any());
    }

    public void testCheckRequestBlock() {
        String[] concreteIndices = { "index1", "index2" };
        when(clusterState.blocks()).thenReturn(clusterBlocks);
        when(clusterBlocks.indicesBlockedException(any(), any())).thenReturn(null);

        NeuralSparseWarmupRequest request = new NeuralSparseWarmupRequest(concreteIndices);

        assertNull(transportAction.checkRequestBlock(clusterState, request, concreteIndices));
        verify(clusterBlocks).indicesBlockedException(any(), any());
    }
}
