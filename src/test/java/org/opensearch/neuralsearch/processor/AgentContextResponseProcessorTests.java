/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class AgentContextResponseProcessorTests extends OpenSearchTestCase {

    private static final String PROCESSOR_TAG = "test-tag";
    private static final String DESCRIPTION = "test-description";

    public void testConstructor() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);

        assertEquals(PROCESSOR_TAG, processor.getTag());
        assertEquals(DESCRIPTION, processor.getDescription());
        assertTrue(processor.isIncludeAgentSteps());
        assertFalse(processor.isIncludeDslQuery());
        assertFalse(processor.isIgnoreFailure());
        assertEquals(AgentContextResponseProcessor.TYPE, processor.getType());
    }

    public void testProcessResponse_withoutContext() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();

        SearchResponse result = processor.processResponse(request, response);
        assertEquals(response, result);
    }

    public void testProcessResponse_withNullContext() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();

        SearchResponse result = processor.processResponse(request, response, null);
        assertEquals(response, result);
    }

    public void testProcessResponse_withEmptyContext() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withNullAgentSteps() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", null);
        context.setAttribute("memory_id", null);

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withEmptyAgentSteps() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", "");

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withWhitespaceAgentSteps() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", "   ");

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withNonStringAgentSteps() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", 123);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> { processor.processResponse(request, response, context); });
        assertEquals("agent_steps_summary must be a String, but got: Integer", exception.getMessage());
    }

    public void testProcessResponse_withValidAgentSteps() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String agentSteps = "Step 1: Query analysis\nStep 2: Search execution";
        context.setAttribute("agent_steps_summary", agentSteps);

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertEquals(agentSteps, extBuilder.getAgentStepsSummary());
        assertNull(extBuilder.getMemoryId());
    }

    public void testProcessResponse_withExistingExtensions() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);

        // Create response with existing extensions
        List<SearchExtBuilder> existingExtensions = new ArrayList<>();
        existingExtensions.add(mock(SearchExtBuilder.class));
        SearchResponse response = createMockSearchResponseWithExtensions(existingExtensions);

        PipelineProcessingContext context = new PipelineProcessingContext();
        String agentSteps = "Agent execution steps";
        context.setAttribute("agent_steps_summary", agentSteps);

        SearchResponse result = processor.processResponse(request, response, context);

        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(2, extensions.size());
        assertTrue(extensions.get(1) instanceof AgentStepsSearchExtBuilder);
    }

    public void testProcessResponse_withValidAgentStepsAndMemoryId() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String agentSteps = "Step 1: Query analysis\nStep 2: Search execution";
        String memoryId = "test-memory-123";
        context.setAttribute("agent_steps_summary", agentSteps);
        context.setAttribute("memory_id", memoryId);

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertEquals(agentSteps, extBuilder.getAgentStepsSummary());
        assertEquals(memoryId, extBuilder.getMemoryId());
    }

    public void testProcessResponse_withNonStringMemoryId() {
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, false, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("memory_id", 456); // Non-string value

        RuntimeException exception = assertThrows(RuntimeException.class, () -> { processor.processResponse(request, response, context); });
        assertEquals("memory_id must be a String, but got: Integer", exception.getMessage());
    }

    public void testProcessResponse_withOnlyMemoryId_AlwaysShown() {
        // Test that memory_id is always shown regardless of configuration
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, false, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String memoryId = "test-memory-456";
        context.setAttribute("memory_id", memoryId);

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertNull(extBuilder.getAgentStepsSummary());
        assertEquals(memoryId, extBuilder.getMemoryId());
        assertNull(extBuilder.getDslQuery());
    }

    public void testFactory() {
        AgentContextResponseProcessor.Factory factory = new AgentContextResponseProcessor.Factory();

        AgentContextResponseProcessor processor = factory.create(null, PROCESSOR_TAG, DESCRIPTION, false, new HashMap<>(), null);

        assertNotNull(processor);
        assertEquals(DESCRIPTION, processor.getDescription());
        assertEquals(PROCESSOR_TAG, processor.getTag());
        assertFalse(processor.isIgnoreFailure());
        // Test default values when no configuration is provided
        assertFalse(processor.isIncludeAgentSteps());
        assertFalse(processor.isIncludeDslQuery());
    }

    private SearchResponse createMockSearchResponse() {
        SearchHits hits = mock(SearchHits.class);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 0, new ArrayList<>());

        return new SearchResponse(sections, null, 1, 1, 0, 100, null, new ShardSearchFailure[0], SearchResponse.Clusters.EMPTY, null);
    }

    private SearchResponse createMockSearchResponseWithExtensions(List<SearchExtBuilder> extensions) {
        SearchHits hits = mock(SearchHits.class);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 0, extensions);

        return new SearchResponse(sections, null, 1, 1, 0, 100, null, new ShardSearchFailure[0], SearchResponse.Clusters.EMPTY, null);
    }

    public void testProcessResponse_withAgentStepsDisabled() {
        // Test that agent steps are not included when includeAgentSteps is false
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, false, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String agentSteps = "Step 1: Test";
        String memoryId = "test-memory-123";
        context.setAttribute("agent_steps_summary", agentSteps);
        context.setAttribute("memory_id", memoryId);

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertNull(extBuilder.getAgentStepsSummary()); // Should be null when disabled
        assertEquals(memoryId, extBuilder.getMemoryId()); // Memory ID always included
        assertNull(extBuilder.getDslQuery());
    }

    public void testProcessResponse_withDslQueryEnabled() {
        // Test that DSL query is included when includeDslQuery is true
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, false, true);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String dslQuery = "{\"query\":{\"match\":{\"field\":\"value\"}}}";
        String memoryId = "test-memory-456";
        context.setAttribute("dsl_query", dslQuery);
        context.setAttribute("memory_id", memoryId);

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertNull(extBuilder.getAgentStepsSummary());
        assertEquals(memoryId, extBuilder.getMemoryId());
        assertEquals(dslQuery, extBuilder.getDslQuery()); // Should be included when enabled
    }

    public void testProcessResponse_withAllFieldsEnabled() {
        // Test that all fields are included when both flags are true
        AgentContextResponseProcessor processor = new AgentContextResponseProcessor(PROCESSOR_TAG, DESCRIPTION, false, true, true);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String agentSteps = "Step 1: Analysis\nStep 2: Execution";
        String dslQuery = "{\"query\":{\"bool\":{\"must\":[]}}}";
        String memoryId = "test-memory-all";
        context.setAttribute("agent_steps_summary", agentSteps);
        context.setAttribute("dsl_query", dslQuery);
        context.setAttribute("memory_id", memoryId);

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertEquals(agentSteps, extBuilder.getAgentStepsSummary());
        assertEquals(memoryId, extBuilder.getMemoryId());
        assertEquals(dslQuery, extBuilder.getDslQuery());
    }

    public void testFactory_withConfiguration() {
        AgentContextResponseProcessor.Factory factory = new AgentContextResponseProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("agent_steps_summary", true);
        config.put("dsl_query", false);

        AgentContextResponseProcessor processor = factory.create(null, PROCESSOR_TAG, DESCRIPTION, false, config, null);

        assertNotNull(processor);
        assertEquals(DESCRIPTION, processor.getDescription());
        assertEquals(PROCESSOR_TAG, processor.getTag());
        assertFalse(processor.isIgnoreFailure());
        assertTrue(processor.isIncludeAgentSteps());
        assertFalse(processor.isIncludeDslQuery());
    }

    public void testFactory_withPartialConfiguration() {
        AgentContextResponseProcessor.Factory factory = new AgentContextResponseProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("dsl_query", true);

        AgentContextResponseProcessor processor = factory.create(null, PROCESSOR_TAG, DESCRIPTION, false, config, null);

        assertNotNull(processor);
        assertFalse(processor.isIncludeAgentSteps()); // Should use default false
        assertTrue(processor.isIncludeDslQuery()); // Should use configured true
    }
}
