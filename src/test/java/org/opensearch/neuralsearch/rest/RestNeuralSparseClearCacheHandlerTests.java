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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.rest.RestChannel;

import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheAction;
import org.opensearch.neuralsearch.transport.NeuralSparseClearCacheRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestNeuralSparseClearCacheHandlerTests extends OpenSearchTestCase {

    @Mock
    protected ClusterService clusterService;

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

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);

        // Verify
        assertNotNull(consumer);
    }

    @SneakyThrows
    public void testPrepareRequestWithMultipleIndices() {
        // Setup
        String indexNames = "index1,index2,index3";
        when(restRequest.param("index")).thenReturn(indexNames);

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);

        // Verify
        assertNotNull(consumer);
    }

    @SneakyThrows
    public void testCreateClearCacheRequest() {
        // Setup
        String indexNames = "index1,index2";
        when(restRequest.param("index")).thenReturn(indexNames);

        // Setup nodeClient to capture the execute call
        doAnswer(invocation -> {
            assertEquals(NeuralSparseClearCacheAction.INSTANCE, invocation.getArgument(0));
            NeuralSparseClearCacheRequest request = invocation.getArgument(1);
            assertNotNull(request);
            // Verify the request contains the correct index names
            assertArrayEquals(new String[] { "index1", "index2" }, request.indices());
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
