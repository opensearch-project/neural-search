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
import static org.mockito.Mockito.atLeastOnce;
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

    /**
     * The stats a cluster at the build's own version can exchange: everything except the resolver stats, which are gated at
     * 3.10 while this branch compiles against a 3.8 core and are therefore withheld.
     *
     * <p>Deliberately explicit rather than a re-implementation of
     * {@code RestNeuralStatsAction#statsSupportedByAllNodes} — restating the production filter here would make these tests
     * pass by construction. It also means <b>this method is what fails when the resolver stats' version is changed</b>: once
     * they are gated at or below the version in the build, these sets go back to {@code EnumSet.allOf}, and that coupling is
     * intentional.
     */
    private static EnumSet<EventStatName> eventStatsAtBuildVersion() {
        return EnumSet.complementOf(
            EnumSet.range(EventStatName.HYBRID_QUERY_FUSION_REQUESTS, EventStatName.HYBRID_QUERY_FUSION_COMB_HARMONIC_EXECUTIONS)
        );
    }

    /** As above for info stats. */
    private static EnumSet<InfoStatName> infoStatsAtBuildVersion() {
        return EnumSet.complementOf(EnumSet.of(InfoStatName.HYBRID_FUSION_ENABLED));
    }

    /**
     * Both sides of the resolver stats' version gate, which nothing else pins.
     *
     * <p>{@code statsSupportedByAllNodes} keeps the leading run of stats whose version is {@code onOrBefore} the oldest
     * node's and stops at the first newer one, because a stat enum travels as ordinals and a filtered set is only readable
     * by an older node if it is a prefix of that node's own enum. So the gate has two halves worth asserting separately:
     * on a cluster at the release these stats ship in they must be <b>present</b>, and on one below it they must be
     * <b>withheld</b>. Testing only the second half would pass for a stat gated at any unreachable future version,
     * including a typo.
     */
    public void test_execute_resolverStatsAreGatedOnTheirOwnRelease() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        Version resolverStatsRelease = EventStatName.HYBRID_QUERY_FUSION_REQUESTS.version();

        when(clusterUtil.getClusterMinVersion()).thenReturn(resolverStatsRelease);
        NeuralStatsInput atRelease = captureStatsInput();
        assertTrue(
            "a cluster at the release these stats ship in must be able to report them",
            atRelease.getEventStatNames().contains(EventStatName.HYBRID_QUERY_FUSION_REQUESTS)
        );
        assertTrue(
            "including the info stat, which is gated through the same mechanism",
            atRelease.getInfoStatNames().contains(InfoStatName.HYBRID_FUSION_ENABLED)
        );

        // One minor version below, which is the case the ordinal contract exists for: an older node has no ordinal for them.
        Version justBefore = Version.fromString((resolverStatsRelease.major) + "." + (resolverStatsRelease.minor - 1) + ".0");
        when(clusterUtil.getClusterMinVersion()).thenReturn(justBefore);
        NeuralStatsInput beforeRelease = captureStatsInput();
        assertFalse(
            "a cluster below that release must not be sent ordinals it cannot read",
            beforeRelease.getEventStatNames().contains(EventStatName.HYBRID_QUERY_FUSION_REQUESTS)
        );
        assertFalse(
            "nor the info stat",
            beforeRelease.getInfoStatNames().contains(InfoStatName.HYBRID_FUSION_ENABLED)
        );
    }

    /** Drive the action once and return the stats input it asked for. */
    private NeuralStatsInput captureStatsInput() throws Exception {
        RestNeuralStatsAction action = new RestNeuralStatsAction(settingsAccessor, clusterUtil);
        action.handleRequest(getRestRequest(), channel, client);
        ArgumentCaptor<NeuralStatsRequest> captor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, atLeastOnce()).execute(eq(NeuralStatsAction.INSTANCE), captor.capture(), any());
        return captor.getValue().getNeuralStatsInput();
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
        assertEquals(capturedInput.getEventStatNames(), eventStatsAtBuildVersion());
        assertEquals(capturedInput.getInfoStatNames(), infoStatsAtBuildVersion());
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

        assertEquals(capturedInput.getEventStatNames(), eventStatsAtBuildVersion());
        assertEquals(capturedInput.getInfoStatNames(), infoStatsAtBuildVersion());
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

    /**
     * A stat is only requested while every node in the cluster reads its ordinal as the same stat, so the request stops at
     * the first stat the oldest node does not have rather than skipping it: 3.3.0 added event stats in the middle of the
     * enum, which moved the ordinal of every event stat declared after them, and appended the agentic info stat.
     */
    public void test_execute_versionOlderThanEventStatOrdinalShift() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.V_3_2_0);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        RestRequest request = getRestRequest();
        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(
            EnumSet.range(
                EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS,
                EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT
            ),
            capturedInput.getEventStatNames()
        );
        assertEquals(
            EnumSet.range(InfoStatName.CLUSTER_VERSION, InfoStatName.RERANK_ML_PROCESSORS),
            capturedInput.getInfoStatNames()
        );
        assertEquals(EnumSet.noneOf(MetricStatName.class), capturedInput.getMetricStatNames());
    }

    /**
     * 3.5.0 added an event stat ahead of the last one 3.3.0 had added, so a node on 3.3.x or 3.4.x has every event stat up
     * to that one and none of the two after it, whatever version those two declare.
     */
    public void test_execute_versionOlderThanLastEventStatOrdinalShift() throws Exception {
        when(settingsAccessor.isStatsEnabled()).thenReturn(true);
        when(clusterUtil.getClusterMinVersion()).thenReturn(Version.V_3_4_0);

        RestNeuralStatsAction restNeuralStatsAction = new RestNeuralStatsAction(settingsAccessor, clusterUtil);

        RestRequest request = getRestRequest();
        restNeuralStatsAction.handleRequest(request, channel, client);

        ArgumentCaptor<NeuralStatsRequest> argumentCaptor = ArgumentCaptor.forClass(NeuralStatsRequest.class);
        verify(client, times(1)).execute(eq(NeuralStatsAction.INSTANCE), argumentCaptor.capture(), any());

        NeuralStatsInput capturedInput = argumentCaptor.getValue().getNeuralStatsInput();
        assertEquals(
            EnumSet.range(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, EventStatName.SEISMIC_QUERY_REQUESTS),
            capturedInput.getEventStatNames()
        );
        // The resolver's info stat is gated at 3.10, so a 3.4 cluster does not get it either.
        assertEquals(EnumSet.range(InfoStatName.CLUSTER_VERSION, InfoStatName.AGENTIC_CONTEXT_PROCESSORS), capturedInput.getInfoStatNames());
        assertEquals(EnumSet.allOf(MetricStatName.class), capturedInput.getMetricStatNames());
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
