/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.strategies;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.neuralsearch.highlight.HighlightContext;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SingleHighlighterTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClient;

    @Mock
    private HighlightResultApplier resultApplier;

    @Mock
    private ActionListener<SearchResponse> responseListener;

    private SingleHighlighter singleHighlighter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testSingleRequestCreation() {
        // Setup
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(1);
        List<SearchHit> validHits = createHits(1);
        HighlightContext context = createContext(requests, validHits);

        List<Map<String, Object>> mockResult = createSingleResult();

        // Mock ML client to capture the request
        ArgumentCaptor<SentenceHighlightingRequest> requestCaptor = ArgumentCaptor.forClass(SentenceHighlightingRequest.class);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(mockResult);
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(requestCaptor.capture(), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify
        SentenceHighlightingRequest capturedRequest = requestCaptor.getValue();
        assertNotNull(capturedRequest);
        assertEquals("test-model", capturedRequest.getModelId());
        assertEquals("query", capturedRequest.getQuestion());
        assertEquals("context0", capturedRequest.getContext());
    }

    public void testSingleResponseProcessing() {
        // Setup
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(1);
        List<SearchHit> validHits = createHits(1);
        HighlightContext context = createContext(requests, validHits);

        List<Map<String, Object>> mockResult = createSingleResult();

        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(mockResult);
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify result applier was called
        verify(resultApplier).applySingleResult(
            eq(validHits.get(0)),
            eq(mockResult),
            eq("content"),
            eq(SemanticHighlightingConstants.DEFAULT_PRE_TAG),
            eq(SemanticHighlightingConstants.DEFAULT_POST_TAG)
        );

        // Verify response was returned
        verify(responseListener).onResponse(any(SearchResponse.class));
    }

    public void testSequentialProcessing() {
        // Setup - 3 documents should be processed sequentially
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(3);
        List<SearchHit> validHits = createHits(3);
        HighlightContext context = createContext(requests, validHits);

        // Track the order of requests
        List<String> processedContexts = new ArrayList<>();

        doAnswer((Answer<Void>) invocation -> {
            SentenceHighlightingRequest request = invocation.getArgument(0);
            processedContexts.add(request.getContext());

            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(createSingleResult());
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify all 3 documents were processed in order
        assertEquals(3, processedContexts.size());
        assertEquals("context0", processedContexts.get(0));
        assertEquals("context1", processedContexts.get(1));
        assertEquals("context2", processedContexts.get(2));

        // Verify ML client was called 3 times
        verify(mlClient, times(3)).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Verify result applier was called 3 times
        verify(resultApplier, times(3)).applySingleResult(any(SearchHit.class), anyList(), anyString(), anyString(), anyString());
    }

    public void testErrorHandling() {
        // Setup with ignoreFailure = false
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(2);
        List<SearchHit> validHits = createHits(2);
        HighlightContext context = createContext(requests, validHits);

        // First request succeeds, second request fails
        doAnswer((Answer<Void>) invocation -> {
            SentenceHighlightingRequest request = invocation.getArgument(0);
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);

            if ("context0".equals(request.getContext())) {
                // First request succeeds
                listener.onResponse(createSingleResult());
            } else {
                // Second request fails
                listener.onFailure(new RuntimeException("ML service error"));
            }
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify error was propagated
        ArgumentCaptor<Exception> errorCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(errorCaptor.capture());
        assertEquals("ML service error", errorCaptor.getValue().getMessage());

        // Verify only first document was processed
        verify(resultApplier, times(1)).applySingleResult(any(SearchHit.class), anyList(), anyString(), anyString(), anyString());
    }

    public void testErrorHandlingWithIgnoreFailure() {
        // Setup with ignoreFailure = true
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, true);

        List<SentenceHighlightingRequest> requests = createRequests(1);
        List<SearchHit> validHits = createHits(1);
        HighlightContext context = createContext(requests, validHits);

        RuntimeException testError = new RuntimeException("ML service error");

        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onFailure(testError);
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify original response was returned (error ignored)
        verify(responseListener).onResponse(context.getOriginalResponse());
        verify(responseListener, times(0)).onFailure(any());
    }

    public void testEmptyContext() {
        // Setup
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        HighlightContext context = createContext(new ArrayList<>(), new ArrayList<>());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify ML client was never called
        verify(mlClient, times(0)).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Verify original response was returned
        verify(responseListener).onResponse(context.getOriginalResponse());
    }

    public void testProcessingWithMultipleHighlights() {
        // Setup - single document with multiple highlights
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(1);
        List<SearchHit> validHits = createHits(1);
        HighlightContext context = createContext(requests, validHits);

        // Create result with multiple highlights
        List<Map<String, Object>> multipleHighlights = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> highlight = new HashMap<>();
            highlight.put("score", 0.9f - (i * 0.1f));
            highlight.put("start_offset", i * 10);
            highlight.put("end_offset", (i * 10) + 8);
            highlight.put("text", "highlight" + i);
            multipleHighlights.add(highlight);
        }

        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(multipleHighlights);
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify result applier received all highlights
        ArgumentCaptor<List<Map<String, Object>>> highlightsCaptor = ArgumentCaptor.forClass(List.class);
        verify(resultApplier).applySingleResult(any(SearchHit.class), highlightsCaptor.capture(), anyString(), anyString(), anyString());

        List<Map<String, Object>> capturedHighlights = highlightsCaptor.getValue();
        assertEquals(3, capturedHighlights.size());
        assertEquals(0.9f, (float) capturedHighlights.get(0).get("score"), 0.001f);
        assertEquals(0.8f, (float) capturedHighlights.get(1).get("score"), 0.001f);
        assertEquals(0.7f, (float) capturedHighlights.get(2).get("score"), 0.001f);
    }

    public void testCompleteProcessingTiming() {
        // Setup
        singleHighlighter = new SingleHighlighter(mlClient, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(2);
        List<SearchHit> validHits = createHits(2);

        // Create context with specific start time
        long startTime = 1000L;
        HighlightContext context = createContextWithStartTime(requests, validHits, startTime);

        // Mock ML client with delayed responses
        doAnswer((Answer<Void>) invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            // Simulate processing time
            listener.onResponse(createSingleResult());
            return null;
        }).when(mlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Execute
        singleHighlighter.process(context, responseListener);

        // Verify response was returned
        verify(responseListener).onResponse(any(SearchResponse.class));
    }

    // Helper methods
    private List<SentenceHighlightingRequest> createRequests(int count) {
        List<SentenceHighlightingRequest> requests = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            requests.add(SentenceHighlightingRequest.builder().modelId("test-model").question("query").context("context" + i).build());
        }
        return requests;
    }

    private List<SearchHit> createHits(int count) {
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SearchHit hit = new SearchHit(i, "doc" + i, Collections.emptyMap(), Collections.emptyMap());
            hit.sourceRef(new BytesArray("{\"content\":\"content" + i + "\"}"));
            hits.add(hit);
        }
        return hits;
    }

    private List<Map<String, Object>> createSingleResult() {
        List<Map<String, Object>> results = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        result.put("score", 0.9f);
        result.put("start_offset", 0);
        result.put("end_offset", 10);
        result.put("text", "highlighted text");
        results.add(result);
        return results;
    }

    private HighlightContext createContext(List<SentenceHighlightingRequest> requests, List<SearchHit> validHits) {
        return createContextWithStartTime(requests, validHits, System.currentTimeMillis());
    }

    private HighlightContext createContextWithStartTime(
        List<SentenceHighlightingRequest> requests,
        List<SearchHit> validHits,
        long startTime
    ) {
        SearchHits searchHits = new SearchHits(validHits.toArray(new SearchHit[0]), null, 1.0f);

        InternalSearchResponse internalResponse = new InternalSearchResponse(searchHits, null, null, null, false, null, 0);

        SearchResponse originalResponse = new SearchResponse(internalResponse, null, 1, 1, 0, 100, null, null);

        return HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName("content")
            .originalResponse(originalResponse)
            .startTime(startTime)
            .preTag(SemanticHighlightingConstants.DEFAULT_PRE_TAG)
            .postTag(SemanticHighlightingConstants.DEFAULT_POST_TAG)
            .build();
    }
}
