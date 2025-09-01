/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import lombok.SneakyThrows;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.rest.RestChannel;
import org.opensearch.core.index.Index;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.sparse.common.exception.NeuralSparseInvalidIndicesException;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheAction;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestNeuralSparseClearCacheHandlerTests extends RestNeuralSparseTestCase {

    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Mock
    private NodeClient nodeClient;

    @Mock
    private RestRequest restRequest;

    private RestNeuralSparseClearCacheHandler handler;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        handler = new RestNeuralSparseClearCacheHandler(clusterService, indexNameExpressionResolver);
    }

    public void testGetName() {
        assertEquals("neural_sparse_clear_cache_action", handler.getName());
    }

    public void testRoutes() {
        List<RestNeuralSparseClearCacheHandler.Route> routes = handler.routes();
        assertEquals(1, routes.size());

        RestNeuralSparseClearCacheHandler.Route route = routes.get(0);
        assertEquals(RestRequest.Method.POST, route.getMethod());
        assertEquals(String.format(Locale.ROOT, "%s/clear_cache/{index}", NeuralSearch.NEURAL_BASE_URI), route.getPath());
    }

    @SneakyThrows
    public void testPrepareRequestWithSingleIndex() {
        // Setup
        String indexName = "test-index";
        when(restRequest.param("index")).thenReturn(indexName);

        Index[] indices = { new Index(indexName, "uuid1") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { indexName }))).thenReturn(indices);

        setupValidSparseIndices();

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);

        // Verify
        assertNotNull(consumer);
        verify(indexNameExpressionResolver).concreteIndices(any(), any(), eq(new String[] { indexName }));
    }

    @SneakyThrows
    public void testPrepareRequestWithMultipleIndices() {
        // Setup
        String indexNames = "index1,index2,index3";
        when(restRequest.param("index")).thenReturn(indexNames);

        Index[] indices = { new Index("index1", "uuid1"), new Index("index2", "uuid2"), new Index("index3", "uuid3") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { "index1", "index2", "index3" }))).thenReturn(
            indices
        );

        setupValidSparseIndices();

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);

        // Verify
        assertNotNull(consumer);
        verify(indexNameExpressionResolver).concreteIndices(any(), any(), eq(new String[] { "index1", "index2", "index3" }));
    }

    public void testPrepareRequestWithInvalidSparseIndex() {
        // Setup
        String indexName = "invalid-index";
        when(restRequest.param("index")).thenReturn(indexName);

        Index[] indices = { new Index(indexName, "uuid1") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { indexName }))).thenReturn(indices);

        setupInvalidSparseIndices();

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> handler.prepareRequest(restRequest, nodeClient)
        );

        assertTrue(exception.getMessage().contains(indexName));
        assertTrue(exception.getMessage().contains("neural_sparse_clear_cache_action"));
    }

    @SneakyThrows
    public void testCreateClearCacheRequest() {
        // Setup
        String indexNames = "index1,index2";
        when(restRequest.param("index")).thenReturn(indexNames);

        Index[] indices = { new Index("index1", "uuid1"), new Index("index2", "uuid2") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { "index1", "index2" }))).thenReturn(indices);

        setupValidSparseIndices();

        // Setup nodeClient to capture the execute call
        doAnswer(invocation -> {
            assertEquals(NeuralSparseClearCacheAction.INSTANCE, invocation.getArgument(0));
            NeuralSparseClearCacheRequest request = invocation.getArgument(1);
            assertNotNull(request);
            return null;
        }).when(nodeClient).execute(any(), any(NeuralSparseClearCacheRequest.class), any());

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);
        assertNotNull(consumer);

        // Execute the consumer to trigger the nodeClient.execute call
        ((CheckedConsumer<RestChannel, Exception>) consumer).accept(mock(RestChannel.class));

        // Verify the action was called
        verify(nodeClient).execute(eq(NeuralSparseClearCacheAction.INSTANCE), any(NeuralSparseClearCacheRequest.class), any());
    }
}
