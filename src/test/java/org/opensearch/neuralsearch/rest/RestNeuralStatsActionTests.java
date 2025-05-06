/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.neuralsearch.processor.InferenceProcessorTestCase;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.NeuralStatsInput;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.neuralsearch.transport.NeuralStatsAction;
import org.opensearch.neuralsearch.transport.NeuralStatsRequest;
import org.opensearch.neuralsearch.transport.NeuralStatsResponse;
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

    public void test_execute() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor);

        RestRequest request = getRestRequest();
        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(capturedInput.getEventStatNames(), EnumSet.allOf(EventStatName.class));
        assertEquals(capturedInput.getInfoStatNames(), EnumSet.allOf(InfoStatName.class));
    }

    public void test_handleRequest_disabledForbidden() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(false);
        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor);

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
        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor);

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

    private RestRequest getRestRequest() {
        Map<String, String> params = new HashMap<>();
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
    }
}
