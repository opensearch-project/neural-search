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
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class AgentStepsResponseProcessorTests extends OpenSearchTestCase {

    private static final String PROCESSOR_TAG = "test-tag";
    private static final String DESCRIPTION = "test-description";

    public void testConstructor() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);

        assertEquals(DESCRIPTION, processor.getDescription());
        assertEquals(PROCESSOR_TAG, processor.getTag());
        assertFalse(processor.isIgnoreFailure());
        assertEquals(AgentStepsResponseProcessor.TYPE, processor.getType());
    }

    public void testProcessResponse_withoutContext() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();

        SearchResponse result = processor.processResponse(request, response);
        assertEquals(response, result);
    }

    public void testProcessResponse_withNullContext() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();

        SearchResponse result = processor.processResponse(request, response, null);
        assertEquals(response, result);
    }

    public void testProcessResponse_withEmptyContext() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withNullAgentSteps() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", null);

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withEmptyAgentSteps() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", "");

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withWhitespaceAgentSteps() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", "   ");

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withNonStringAgentSteps() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        context.setAttribute("agent_steps_summary", 123);

        SearchResponse result = processor.processResponse(request, response, context);
        assertEquals(response, result);
    }

    public void testProcessResponse_withValidAgentSteps() {
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
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
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
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
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
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
        AgentStepsResponseProcessor processor = new AgentStepsResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest request = mock(SearchRequest.class);
        SearchResponse response = createMockSearchResponse();
        PipelineProcessingContext context = new PipelineProcessingContext();
        String agentSteps = "Step 1: Test";
        context.setAttribute("agent_steps_summary", agentSteps);
        context.setAttribute("memory_id", 456); // Non-string value

        SearchResponse result = processor.processResponse(request, response, context);

        assertNotEquals(response, result);
        List<SearchExtBuilder> extensions = result.getInternalResponse().getSearchExtBuilders();
        assertNotNull(extensions);
        assertEquals(1, extensions.size());
        assertTrue(extensions.get(0) instanceof AgentStepsSearchExtBuilder);

        AgentStepsSearchExtBuilder extBuilder = (AgentStepsSearchExtBuilder) extensions.get(0);
        assertEquals(agentSteps, extBuilder.getAgentStepsSummary());
        assertNull(extBuilder.getMemoryId()); // Should be null for non-string
    }

    public void testFactory() {
        AgentStepsResponseProcessor.Factory factory = new AgentStepsResponseProcessor.Factory();

        AgentStepsResponseProcessor processor = factory.create(null, PROCESSOR_TAG, DESCRIPTION, false, Map.of(), null);

        assertNotNull(processor);
        assertEquals(DESCRIPTION, processor.getTag());
        assertEquals(PROCESSOR_TAG, processor.getDescription());
        assertFalse(processor.isIgnoreFailure());
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
}
