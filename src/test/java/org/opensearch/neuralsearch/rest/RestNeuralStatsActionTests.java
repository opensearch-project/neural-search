/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.neuralsearch.processor.InferenceProcessorTestCase;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsRequest;
import org.opensearch.neuralsearch.transport.NeuralStatsResponse;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestNeuralStatsActionTests extends InferenceProcessorTestCase {
    private NodeClient client;
    private ThreadPool threadPool;

    @Mock
    RestChannel channel;

    @Mock
    private NeuralSearchSettingsAccessor settingsAccessor;

    @Mock
    private NeuralSearchClusterUtil clusterUtil;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");
        client = spy(new NodeClient(Settings.EMPTY, threadPool));

        doAnswer(invocation -> {
            ActionListener<NeuralStatsResponse> actionListener = invocation.getArgument(2);
            return null;
        }).when(client).execute(eq(NeuralStatsAction.INSTANCE), any(), any());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        client.close();
    }

    public void test_execute_containsAllStats() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        RestRequest request = getRestRequest();
        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        // Verify all stats available in current version should match all available stats
        // If this test is failing after adding a new stat, make sure to update the version stat map in MinClusterVersionUtil.
        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(capturedInput.getEventStatNames(), EnumSet.allOf(EventStatName.class));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.allOf(InfoStatName.class));
        assertEquals(capturedInput.getMetricStatNames(), EnumSet.allOf(MetricStatName.class));
        assertFalse(capturedInput.isFlatten());
        assertFalse(capturedInput.isIncludeMetadata());
        assertTrue(capturedInput.isIncludeIndividualNodes());
    }

    public void test_execute_customParams_includePartial() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        Map<String, String> params = Map.of(
            RestNeuralStatsAction.FLATTEN_PARAM, "true",
            RestNeuralStatsAction.INCLUDE_METADATA_PARAM, "true",
            RestNeuralStatsAction.INCLUDE_INDIVIDUAL_NODES_PARAM, "false",
            RestNeuralStatsAction.INCLUDE_ALL_NODES_PARAM, "true",
            RestNeuralStatsAction.INCLUDE_INFO_PARAM, "true",
            RestNeuralStatsAction.INCLUDE_METRIC_PARAM, "true"
        );
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();

        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();

        assertEquals(capturedInput.getEventStatNames(), EnumSet.allOf(EventStatName.class));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.allOf(InfoStatName.class));
        assertEquals(capturedInput.getMetricStatNames(), EnumSet.allOf(MetricStatName.class));
        assertTrue(capturedInput.isFlatten());
        assertTrue(capturedInput.isIncludeMetadata());
        assertFalse(capturedInput.isIncludeIndividualNodes());
        assertTrue(capturedInput.isIncludeAllNodes());
        assertTrue(capturedInput.isIncludeInfo());
        assertTrue(capturedInput.isIncludeMetrics());
    }

    public void test_execute_customParams_includeNone() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        Map<String, String> params = new HashMap<>();
        params.put(RestNeuralStatsAction.FLATTEN_PARAM, "true");
        params.put(RestNeuralStatsAction.INCLUDE_METADATA_PARAM, "true");
        params.put(RestNeuralStatsAction.INCLUDE_INDIVIDUAL_NODES_PARAM, "false");
        params.put(RestNeuralStatsAction.INCLUDE_ALL_NODES_PARAM, "false");
        params.put(RestNeuralStatsAction.INCLUDE_INFO_PARAM, "false");
        params.put(RestNeuralStatsAction.INCLUDE_METRIC_PARAM, "false");

        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();

        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();

        // Since we set individual nodes and all nodes to false, we shouldn't fetch any stats
        assertEquals(capturedInput.getEventStatNames(), EnumSet.noneOf(EventStatName.class));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.noneOf(InfoStatName.class));
        assertEquals(capturedInput.getMetricStatNames(), EnumSet.noneOf(MetricStatName.class));
        assertTrue(capturedInput.isFlatten());
        assertTrue(capturedInput.isIncludeMetadata());
        assertFalse(capturedInput.isIncludeIndividualNodes());
        assertFalse(capturedInput.isIncludeAllNodes());
        assertFalse(capturedInput.isIncludeInfo());
        assertFalse(capturedInput.isIncludeMetrics());
    }

    public void test_handleRequest_disabledForbidden() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(false);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        RestRequest request = getRestRequest();
        restNeuralStatsAction.handleRequest(request, channel, client);

        verify(client, never()).execute(eq(NeuralStatsAction.INSTANCE), any(), any());

        ArgumentCaptor<BytesRestResponse> responseCaptor = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(channel).sendResponse(responseCaptor.capture());

        BytesRestResponse response = responseCaptor.getValue();
        assertEquals(RestStatus.FORBIDDEN, response.status());
    }

    public void test_handleRequest_invalidStatParameter() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        // Create request with invalid stat parameter
        Map<String, String> params = new HashMap<>();
        params.put("stat", "INVALID_STAT");
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                .withParams(params)
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> restNeuralStatsAction.handleRequest(request, channel, client)
        );

        verify(client, never()).execute(eq(NeuralStatsAction.INSTANCE), any(), any());
    }

    public void test_execute_olderVersion() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.V_3_0_0);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        RestRequest request = getRestRequest();
        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(capturedInput.getEventStatNames(), EnumSet.of(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.of(InfoStatName.TEXT_EMBEDDING_PROCESSORS, InfoStatName.CLUSTER_VERSION));
    }

    public void test_execute_statParameters() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        // Create request with stats not existing on 3.0.0
        Map<String, String> params = new HashMap<>();
        params.put("stat", String.join(",",
                EventStatName.TEXT_CHUNKING_PROCESSOR_EXECUTIONS.getNameString(),
                EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getNameString(),
                InfoStatName.TEXT_CHUNKING_PROCESSORS.getNameString(),
                InfoStatName.TEXT_EMBEDDING_PROCESSORS.getNameString(),
                MetricStatName.MEMORY_SPARSE_CLUSTERED_POSTING_USAGE.getNameString()
        ));
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                .withParams(params)
                .build();

        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(capturedInput.getEventStatNames(), EnumSet.of(
                EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS,
                EventStatName.TEXT_CHUNKING_PROCESSOR_EXECUTIONS
        ));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.of(
                InfoStatName.TEXT_CHUNKING_PROCESSORS,
                InfoStatName.TEXT_EMBEDDING_PROCESSORS
        ));
        assertEquals(capturedInput.getMetricStatNames(), EnumSet.of(MetricStatName.MEMORY_SPARSE_CLUSTERED_POSTING_USAGE));
    }

    public void test_execute_statParameters_olderVersion() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.V_3_0_0);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        // Create request with stats not existing on 3.0.0
        Map<String, String> params = new HashMap<>();
        params.put("stat", String.join(",",
                EventStatName.TEXT_CHUNKING_PROCESSOR_EXECUTIONS.getNameString(),
                EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getNameString(),
                InfoStatName.TEXT_CHUNKING_PROCESSORS.getNameString(),
                InfoStatName.TEXT_EMBEDDING_PROCESSORS.getNameString()
        ));
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                .withParams(params)
                .build();

        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(capturedInput.getEventStatNames(), EnumSet.of(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.of(InfoStatName.TEXT_EMBEDDING_PROCESSORS));
    }

    private RestRequest getRestRequest() {
        Map<String, String> params = new HashMap<>();
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
    }
}
