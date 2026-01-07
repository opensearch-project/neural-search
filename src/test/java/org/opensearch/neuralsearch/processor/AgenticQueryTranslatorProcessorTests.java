/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.ml.dto.AgentExecutionDTO;
import org.opensearch.neuralsearch.ml.dto.AgentInfoDTO;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.SearchExtBuilder;
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
        config.put("agent_id", AGENT_ID);
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

    public void testProcessRequestAsync_withAgenticQuery_callsMLClient() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .queryFields(Arrays.asList("title", "description"));

        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        // Mock executeAgent call
        doAnswer(invocation -> {
            // Don't call the listener to avoid parsing issues in test
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                eq(mockXContentRegistry),
                any(ActionListener.class)
            );

        processor.processRequestAsync(request, mockContext, listener);

        // Verify ML client was called with correct parameters
        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            eq(mockXContentRegistry),
            any(ActionListener.class)
        );
    }

    public void testProcessRequestAsync_withAgenticQuery_agentFailure() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<String> agentListener = invocation.getArgument(5);
            agentListener.onFailure(new IllegalArgumentException("Agent failed"));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                eq(mockXContentRegistry),
                any(ActionListener.class)
            );

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            eq(mockXContentRegistry),
            any(ActionListener.class)
        );
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search failed - Agent execution error"));
    }

    public void testProcessRequestAsync_withAgenticQuery_parseFailure() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        // Invalid JSON response that will cause parsing to fail
        String invalidAgentResponse = "{invalid json}";

        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(invalidAgentResponse, null, null, null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                eq(mockXContentRegistry),
                any(ActionListener.class)
            );

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            eq(mockXContentRegistry),
            any(ActionListener.class)
        );
        ArgumentCaptor<IOException> exceptionCaptor = ArgumentCaptor.forClass(IOException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search failed - Parse error"));
    }

    public void testProcessRequestAsync_withAgenticQuery_getAgentDetailsFailure() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails failure
        doAnswer(invocation -> {
            ActionListener<Map<String, Object>> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onFailure(new IllegalArgumentException("Failed to get agent info"));
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search failed - Failed to get agent info"));
    }

    public void testProcessRequest_throwsException() {
        SearchRequest request = new SearchRequest();

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> processor.processRequest(request)
        );

        assertEquals("Use processRequestAsync for agentic search processor", exception.getMessage());
    }

    public void testIsIgnoreFailure() {
        assertFalse(processor.isIgnoreFailure());
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
        config.put("agent_id", AGENT_ID);

        AgenticQueryTranslatorProcessor createdProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        assertNotNull(createdProcessor);
        assertEquals("agentic_query_translator", createdProcessor.getType());
    }

    public void testFactory_create_missingAgentId() {
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            mockSettingsAccessor
        );

        Map<String, Object> config = new HashMap<>();

        OpenSearchParseException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, "test-tag", "test-description", false, config, null)
        );

        assertTrue(exception.getMessage().contains("agent_id"));
    }

    public void testFactory_create_emptyAgentId() {
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            mockSettingsAccessor
        );

        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", "");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(null, "test-tag", "test-description", false, config, null)
        );

        assertEquals("agent_id is required for agentic_query_translator processor", exception.getMessage());
    }

    public void testProcessRequestAsync_withAgenticQuery_andAggregations() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(agenticQuery);
        sourceBuilder.aggregation(AggregationBuilders.terms("test_agg").field("field"));
        request.source(sourceBuilder);

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        processor.processRequestAsync(request, mockContext, listener);

        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search blocked - Invalid usage with other search features"));
        verifyNoInteractions(mockMLClient);
    }

    public void testProcessRequestAsync_withAgenticQuery_andSort() {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(agenticQuery);
        sourceBuilder.sort("field");
        request.source(sourceBuilder);

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        processor.processRequestAsync(request, mockContext, listener);

        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Agentic search blocked - Invalid usage with other search features"));
        verifyNoInteractions(mockMLClient);
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
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);

        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        // Use match_all query which should parse correctly
        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";

        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            any(NamedXContentRegistry.class),
            any(ActionListener.class)
        );
        verify(listener).onResponse(request);

        assertNotNull(request.source());
    }

    public void testProcessRequestAsync_withAgenticQuery_oversizedResponse() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        // Create a response larger than MAX_AGENT_RESPONSE_SIZE characters
        String oversizedResponse = "x".repeat(10_001);

        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(oversizedResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                eq(mockXContentRegistry),
                any(ActionListener.class)
            );

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            eq(mockXContentRegistry),
            any(ActionListener.class)
        );
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        IllegalArgumentException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("Agentic search blocked - Response size exceeded limit"));
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }

    public void testProcessRequestAsync_withAgenticQuery_nullResponse() throws IOException {
        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(null, null, null, null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                eq(mockXContentRegistry),
                any(ActionListener.class)
            );

        processor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            eq(mockXContentRegistry),
            any(ActionListener.class)
        );
        ArgumentCaptor<IllegalArgumentException> exceptionCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        IllegalArgumentException exception = exceptionCaptor.getValue();
        assertTrue(exception.getMessage().contains("Agentic search failed - Null response from agent"));
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }

    public void testProcessRequestAsync_withFlowAgent() throws IOException {
        // Create processor with proper NamedXContentRegistry
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call for flow agent
        AgentInfoDTO agentInfo = new AgentInfoDTO("flow", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";

        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            any(NamedXContentRegistry.class),
            any(ActionListener.class)
        );
        verify(listener).onResponse(request);
    }

    public void testProcessRequestAsync_withSystemPromptAgent() throws IOException {
        // Create processor with proper NamedXContentRegistry
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call for agent with system prompt
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", true, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";
        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            any(NamedXContentRegistry.class),
            any(ActionListener.class)
        );
        verify(listener).onResponse(request);
    }

    public void testProcessRequestAsync_withQueryFields() throws IOException {
        // Create processor with proper NamedXContentRegistry
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT)
            .queryFields(Arrays.asList("title", "content"));
        SearchRequest request = new SearchRequest("test-index");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";
        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            any(NamedXContentRegistry.class),
            any(ActionListener.class)
        );
        verify(listener).onResponse(request);
    }

    public void testProcessRequestAsync_withMultipleIndices() throws IOException {
        // Create processor with proper NamedXContentRegistry
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("index1", "index2");
        request.source(new SearchSourceBuilder().query(agenticQuery));

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        // Mock getAgentDetails call
        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";

        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));
        verify(mockMLClient).executeAgent(
            any(SearchRequest.class),
            any(AgenticSearchQueryBuilder.class),
            eq(AGENT_ID),
            eq(agentInfo),
            any(NamedXContentRegistry.class),
            any(ActionListener.class)
        );
        verify(listener).onResponse(request);
    }

    public void testFactoryCreateAgentIdTooLong() {
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            mockSettingsAccessor
        );

        Map<String, Object> config = new HashMap<>();
        // Create agent ID longer than MAX_AGENT_ID_LENGTH (100 characters)
        String longAgentId = "a".repeat(101);
        config.put("agent_id", longAgentId);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(null, "test-tag", "test-description", false, config, null)
        );

        assertEquals("agent_id exceeds maximum length of 100 characters", exception.getMessage());
    }

    public void testFactoryCreateAgentIdInvalidFormat() {
        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            mockXContentRegistry,
            mockSettingsAccessor
        );

        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", "invalid@agent#id");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(null, "test-tag", "test-description", false, config, null)
        );

        assertEquals("agent_id must contain only alphanumeric characters, hyphens, and underscores", exception.getMessage());
    }

    public void testProcessRequestAsync_preservesOriginalExtBuilders() throws IOException {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");

        // Create original search source with ext builders
        SearchSourceBuilder originalSource = new SearchSourceBuilder().query(agenticQuery);
        List<SearchExtBuilder> originalExtBuilders = new ArrayList<>();
        originalExtBuilders.add(mock(SearchExtBuilder.class));
        originalSource.ext(originalExtBuilders);
        request.source(originalSource);

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";
        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, "test-memory-id", null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        verify(listener).onResponse(request);

        // Verify that original ext builders are preserved
        assertNotNull(request.source());
        assertNotNull(request.source().ext());
        assertEquals(1, request.source().ext().size());
        assertEquals(originalExtBuilders.get(0), request.source().ext().get(0));
    }

    public void testProcessRequestAsync_preservesSourceFields() throws IOException {
        // Create processor with proper NamedXContentRegistry
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("match_all"), MatchAllQueryBuilder::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        AgenticQueryTranslatorProcessor.Factory factory = new AgenticQueryTranslatorProcessor.Factory(
            mockMLClient,
            registry,
            mockSettingsAccessor
        );
        Map<String, Object> config = new HashMap<>();
        config.put("agent_id", AGENT_ID);
        AgenticQueryTranslatorProcessor testProcessor = factory.create(null, "test-tag", "test-description", false, config, null);

        AgenticSearchQueryBuilder agenticQuery = new AgenticSearchQueryBuilder().queryText(QUERY_TEXT);
        SearchRequest request = new SearchRequest("test-index");

        // Set up original source with field filtering
        SearchSourceBuilder originalSource = new SearchSourceBuilder().query(agenticQuery)
            .fetchSource(new String[] { "title", "description" }, new String[] { "internal_field" });
        request.source(originalSource);

        ActionListener<SearchRequest> listener = mock(ActionListener.class);

        AgentInfoDTO agentInfo = new AgentInfoDTO("conversational", false, false, "bedrock/converse/claude");
        doAnswer(invocation -> {
            ActionListener<AgentInfoDTO> agentInfoListener = invocation.getArgument(1);
            agentInfoListener.onResponse(agentInfo);
            return null;
        }).when(mockMLClient).getAgentDetails(eq(AGENT_ID), any(ActionListener.class));

        String validAgentResponse = "{\"query\": {\"match_all\": {}}}";
        doAnswer(invocation -> {
            ActionListener<AgentExecutionDTO> agentListener = invocation.getArgument(5);
            agentListener.onResponse(new AgentExecutionDTO(validAgentResponse, null, null, null));
            return null;
        }).when(mockMLClient)
            .executeAgent(
                any(SearchRequest.class),
                any(AgenticSearchQueryBuilder.class),
                eq(AGENT_ID),
                eq(agentInfo),
                any(NamedXContentRegistry.class),
                any(ActionListener.class)
            );

        testProcessor.processRequestAsync(request, mockContext, listener);

        // Verify source fields are preserved
        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(listener).onResponse(requestCaptor.capture());

        SearchRequest modifiedRequest = requestCaptor.getValue();
        assertNotNull(modifiedRequest.source().fetchSource());
        assertArrayEquals(new String[] { "title", "description" }, modifiedRequest.source().fetchSource().includes());
        assertArrayEquals(new String[] { "internal_field" }, modifiedRequest.source().fetchSource().excludes());
    }
}
