/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.util.TestUtils;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.any;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.when;

public class AgenticQueryTranslatorProcessorTests extends OpenSearchTestCase {

    private static final String AGENT_ID = "test-agent";
    private static final String QUERY_TEXT = "find red cars";

    private MLCommonsClientAccessor mockMLClient;
    private NamedXContentRegistry mockXContentRegistry;
    private AgenticQueryTranslatorProcessor processor;
    private PipelineProcessingContext mockContext;
    private NeuralSearchSettingsAccessor mockSettingsAccessor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.initializeEventStatsManager();
        mockMLClient = mock(MLCommonsClientAccessor.class);
        mockXContentRegistry = mock(NamedXContentRegistry.class);
        mockContext = mock(PipelineProcessingContext.class);
        mockSettingsAccessor = mock(NeuralSearchSettingsAccessor.class);
        when(mockSettingsAccessor.isAgenticSearchEnabled()).thenReturn(true);
        EventStatsManager.instance().initialize(mockSettingsAccessor);

        // Mock cluster service
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.index(any(String.class))).thenReturn(null); // Return null to trigger the catch block
        NeuralSearchClusterUtil.instance().initialize(mockClusterService, null);

        // Use factory to create processor since constructor is private
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        processor = factory.create(null, "test-tag", "test-description", false, config, null);
    }

    public void testProcessRequestAsync_withNonAgenticQuery() {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        processor.processRequestAsync(request, mockContext, listener);

        verify(listener).onResponse(request);
        verifyNoInteractions(mockMLClient);
    }

    public void testProcessRequestAsync_withNullSource() {
        SearchRequest request = new SearchRequest();
        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        processor.processRequestAsync(request, mockContext, listener);

        verify(listener).onResponse(request);
        verifyNoInteractions(mockMLClient);
    }

    public void testProcessRequestAsync_withNullQuery() {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        processor.processRequestAsync(request, mockContext, listener);

        verify(listener).onResponse(request);
        verifyNoInteractions(mockMLClient);
    }

    public void testProcessRequestAsync_withAgenticQuery_callsMLClient() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .queryFields(Arrays.asList("title", "description"))
            .agentId(AGENT_ID);

        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock ML client to verify it gets called
        doAnswer(invocation -> {
            // Don't call the listener to avoid parsing issues in test
            return null;
        }).when(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));

        processor.processRequestAsync(request, mockContext, listener);

        // Verify ML client was called with correct parameters
        verify(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));
    }

    public void testProcessRequestAsync_withAgenticQuery_agentFailure() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<String> agentListener = invocation.getArgument(2);
            agentListener.onFailure(new RuntimeException("Agent failed"));
            return null;
        }).when(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));
        ArgumentCaptor<RuntimeException> exceptionCaptor = ArgumentCaptor.forClass(RuntimeException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search failed - Agent execution error:"));
    }

    public void testProcessRequestAsync_withAgenticQuery_parseFailure() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Invalid JSON response that will cause parsing to fail
        String invalidAgentResponse = "{\"query\": {\"match\": {\"field\": \"value\"}";  // Missing closing braces

        doAnswer(invocation -> {
            ActionListener<String> agentListener = invocation.getArgument(2);
            agentListener.onResponse(invalidAgentResponse);
            return null;
        }).when(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));
        ArgumentCaptor<RuntimeException> exceptionCaptor = ArgumentCaptor.forClass(RuntimeException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search failed - Agent execution error:"));
    }

    public void testProcessRequest_throwsException() {
        SearchRequest request = new SearchRequest();

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> processor.processRequest(request)
        );

        assertEquals("Use processRequestAsync for agentic search processor", exception.getMessage());
    }

    public void testGetType() {
        assertEquals("agentic_query_translator", processor.getType());
    }

    public void testFactory_create() {
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            mockSettingsAccessor
        );

        Map<String, Object> config = new HashMap<>();
        AgenticQueryTranslatorProcessor createdProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        assertNotNull(createdProcessor);
        assertEquals("agentic_query_translator", createdProcessor.getType());
    }

    public void testProcessRequestAsync_withAgenticQuery_andAggregations() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);
        SearchRequest request = new SearchRequest("test-index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(agenticQuery);
        sourceBuilder.aggregation(AggregationBuilders.terms("test_agg").field("field"));
        request.source(sourceBuilder);

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        processor.processRequestAsync(request, mockContext, listener);

        // Verify that onFailure was called with IllegalArgumentException
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search blocked - Invalid usage with other search features"));
        verifyNoInteractions(mockMLClient);
    }

    public void testFactory_create_feature_disabled() {
        NeuralSearchSettingsAccessor accessor = mock(NeuralSearchSettingsAccessor.class);
        when(accessor.isAgenticSearchEnabled()).thenReturn(false);
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            accessor
        );

        Map<String, Object> config = new HashMap<>();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> factory.create(null, "test-tag", "test-description", false, config, null)
        );

        assertTrue(exception.getMessage().contains("Agentic search is currently disabled"));
    }

    public void testProcessRequestAsync_withAgenticQuery_success() throws IOException {
        // Mock cluster service components
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        IndexMetadata mockIndexMetadata = mock(IndexMetadata.class);
        MappingMetadata mockMappingMetadata = mock(MappingMetadata.class);

        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.index("test-index")).thenReturn(mockIndexMetadata);
        when(mockIndexMetadata.mapping()).thenReturn(mockMappingMetadata);
        when(mockMappingMetadata.source()).thenReturn(new CompressedXContent("{\"properties\":{}}"));

        // Set the cluster service in NeuralSearchClusterUtil
        NeuralSearchClusterUtil.instance().initialize(mockClusterService, null);

        // Create processor with proper NamedXContentRegistry that includes query parsers
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match"), MatchQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);

        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Use match_all query which should parse correctly
        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";

        doAnswer(invocation -> {
            ActionListener<String> agentListener = invocation.getArgument(2);
            agentListener.onResponse(validAgentResponse);
            return null;
        }).when(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));
        verify(listener).onResponse(request);

        assertNotNull(request.source());
    }

    public void testProcessRequestAsync_withAgenticQuery_oversizedResponse() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Create a response larger than MAX_AGENT_RESPONSE_SIZE characters
        String oversizedResponse = "x".repeat(10_001);

        doAnswer(invocation -> {
            ActionListener<String> agentListener = invocation.getArgument(2);
            agentListener.onResponse(oversizedResponse);
            return null;
        }).when(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));
        ArgumentCaptor<RuntimeException> exceptionCaptor = ArgumentCaptor.forClass(RuntimeException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        RuntimeException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("Agentic search blocked - Response size exceeded limit"));
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }

    public void testProcessRequestAsync_withAgenticQuery_nullResponse() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT).agentId(AGENT_ID);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<String> agentListener = invocation.getArgument(2);
            agentListener.onResponse(null);
            return null;
        }).when(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).executeAgent(eq(AGENT_ID), any(Map.class), any(ActionListener.class));
        ArgumentCaptor<RuntimeException> exceptionCaptor = ArgumentCaptor.forClass(RuntimeException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        RuntimeException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("Agentic search failed - Null response from agent"));
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }

}
