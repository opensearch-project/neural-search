/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.strategies;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BatchHighlighterTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClient;

    @Mock
    private HighlightResultApplier resultApplier;

    @Mock
    private ActionListener<SearchResponse> responseListener;

    private BatchHighlighter batchHighlighter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testBatchRequestCreation() {
        // Setup
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 10, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(5);
        List<SearchHit> validHits = createHits(5);
        HighlightContext context = createContext(requests, validHits);

        List<List<Map<String, Object>>> mockResults = createResults(5);

        // Mock ML client to capture the batch request
        ArgumentCaptor<List<SentenceHighlightingRequest>> requestCaptor = ArgumentCaptor.forClass(List.class);
        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(mockResults);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(eq("test-model"), requestCaptor.capture(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify
        List<SentenceHighlightingRequest> capturedRequests = requestCaptor.getValue();
        assertEquals(5, capturedRequests.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("query", capturedRequests.get(i).getQuestion());
            assertEquals("context" + i, capturedRequests.get(i).getContext());
        }
    }

    public void testBatchResponseProcessing() {
        // Setup
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 10, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(3);
        List<SearchHit> validHits = createHits(3);
        HighlightContext context = createContext(requests, validHits);

        List<List<Map<String, Object>>> mockResults = createResults(3);

        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(mockResults);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify result applier was called with correct parameters
        verify(resultApplier).applyBatchResults(
            eq(validHits),
            eq(mockResults),
            eq("content"),
            eq(SemanticHighlightingConstants.DEFAULT_PRE_TAG),
            eq(SemanticHighlightingConstants.DEFAULT_POST_TAG)
        );

        // Verify response was returned
        verify(responseListener).onResponse(any(SearchResponse.class));
    }

    public void testBatchSizeLimit() {
        // Setup - batch size of 3, with 7 requests (should create 3 batches: 3, 3, 1)
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 3, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(7);
        List<SearchHit> validHits = createHits(7);
        HighlightContext context = createContext(requests, validHits);

        // Track batch calls
        List<Integer> batchSizes = new ArrayList<>();
        doAnswer(invocation -> {
            List<SentenceHighlightingRequest> batchRequests = invocation.getArgument(1);
            batchSizes.add(batchRequests.size());

            // Create results for this batch
            List<List<Map<String, Object>>> results = createResults(batchRequests.size());

            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(results);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify 3 batches were created with correct sizes
        assertEquals(3, batchSizes.size());
        assertEquals(3, batchSizes.get(0).intValue());
        assertEquals(3, batchSizes.get(1).intValue());
        assertEquals(1, batchSizes.get(2).intValue());

        // Verify ML client was called 3 times
        verify(mlClient, times(3)).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());
    }

    public void testPartialBatchHandling() {
        // Setup - batch size of 5, with 3 requests (partial batch)
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 5, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(3);
        List<SearchHit> validHits = createHits(3);
        HighlightContext context = createContext(requests, validHits);

        ArgumentCaptor<List<SentenceHighlightingRequest>> requestCaptor = ArgumentCaptor.forClass(List.class);
        doAnswer(invocation -> {
            List<List<Map<String, Object>>> results = createResults(3);
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(results);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(eq("test-model"), requestCaptor.capture(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify only one batch with 3 items
        verify(mlClient, times(1)).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());
        assertEquals(3, requestCaptor.getValue().size());
    }

    public void testEmptyBatchHandling() {
        // Setup
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 10, resultApplier, false);

        HighlightContext context = createContext(new ArrayList<>(), new ArrayList<>());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify ML client was never called
        verify(mlClient, times(0)).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Verify original response was returned
        verify(responseListener).onResponse(context.getOriginalResponse());
    }

    public void testErrorInBatchWithIgnoreFailure() {
        // Setup with ignoreFailure = true
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 10, resultApplier, true);

        List<SentenceHighlightingRequest> requests = createRequests(3);
        List<SearchHit> validHits = createHits(3);
        HighlightContext context = createContext(requests, validHits);

        RuntimeException testError = new RuntimeException("ML service error");

        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onFailure(testError);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify original response was returned (error ignored)
        verify(responseListener).onResponse(context.getOriginalResponse());
        verify(responseListener, times(0)).onFailure(any());
    }

    public void testErrorInBatchWithoutIgnoreFailure() {
        // Setup with ignoreFailure = false
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 10, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(3);
        List<SearchHit> validHits = createHits(3);
        HighlightContext context = createContext(requests, validHits);

        RuntimeException testError = new RuntimeException("ML service error");

        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onFailure(testError);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify error was propagated
        ArgumentCaptor<Exception> errorCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(errorCaptor.capture());
        assertEquals("ML service error", errorCaptor.getValue().getMessage());
    }

    public void testMultipleBatchExecution() {
        // Setup - batch size of 2, with 5 requests (should create 3 batches: 2, 2, 1)
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 2, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(5);
        List<SearchHit> validHits = createHits(5);
        HighlightContext context = createContext(requests, validHits);

        // Track which indices are used in applyBatchResultsWithIndices
        List<Integer> startIndices = new ArrayList<>();
        List<Integer> endIndices = new ArrayList<>();

        doAnswer(invocation -> {
            List<SentenceHighlightingRequest> batchRequests = invocation.getArgument(1);
            List<List<Map<String, Object>>> results = createResults(batchRequests.size());
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(results);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        doAnswer(invocation -> {
            startIndices.add(invocation.getArgument(2));
            endIndices.add(invocation.getArgument(3));
            return null;
        }).when(resultApplier)
            .applyBatchResultsWithIndices(anyList(), anyList(), anyInt(), anyInt(), anyString(), anyString(), anyString());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify correct number of batches
        verify(mlClient, times(3)).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Verify indices for each batch
        assertEquals(3, startIndices.size());
        assertEquals(0, startIndices.get(0).intValue());
        assertEquals(2, startIndices.get(1).intValue());
        assertEquals(4, startIndices.get(2).intValue());

        assertEquals(2, endIndices.get(0).intValue());
        assertEquals(4, endIndices.get(1).intValue());
        assertEquals(5, endIndices.get(2).intValue());
    }

    public void testSingleBatchOptimization() {
        // Setup - when all requests fit in one batch, should use optimized path
        batchHighlighter = new BatchHighlighter("test-model", mlClient, 10, resultApplier, false);

        List<SentenceHighlightingRequest> requests = createRequests(5);
        List<SearchHit> validHits = createHits(5);
        HighlightContext context = createContext(requests, validHits);

        List<List<Map<String, Object>>> mockResults = createResults(5);

        doAnswer(invocation -> {
            ActionListener<List<List<Map<String, Object>>>> listener = invocation.getArgument(3);
            listener.onResponse(mockResults);
            return null;
        }).when(mlClient).batchInferenceSentenceHighlighting(anyString(), anyList(), any(), any());

        // Execute
        batchHighlighter.process(context, responseListener);

        // Verify applyBatchResults (not applyBatchResultsWithIndices) was called for single batch
        verify(resultApplier).applyBatchResults(anyList(), anyList(), anyString(), anyString(), anyString());

        // Verify applyBatchResultsWithIndices was not called
        verify(resultApplier, times(0)).applyBatchResultsWithIndices(
            anyList(),
            anyList(),
            anyInt(),
            anyInt(),
            anyString(),
            anyString(),
            anyString()
        );
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

    private List<List<Map<String, Object>>> createResults(int count) {
        List<List<Map<String, Object>>> results = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            List<Map<String, Object>> docResults = new ArrayList<>();
            Map<String, Object> result = new HashMap<>();
            result.put("score", 0.9f);
            result.put("start_offset", 0);
            result.put("end_offset", 10);
            result.put("text", "highlight" + i);
            docResults.add(result);
            results.add(docResults);
        }
        return results;
    }

    private HighlightContext createContext(List<SentenceHighlightingRequest> requests, List<SearchHit> validHits) {
        SearchHits searchHits = new SearchHits(validHits.toArray(new SearchHit[0]), null, 1.0f);

        InternalSearchResponse internalResponse = new InternalSearchResponse(searchHits, null, null, null, false, null, 0);

        SearchResponse originalResponse = new SearchResponse(internalResponse, null, 1, 1, 0, 100, null, null);

        return HighlightContext.builder()
            .requests(requests)
            .validHits(validHits)
            .fieldName("content")
            .originalResponse(originalResponse)
            .startTime(System.currentTimeMillis())
            .preTag(SemanticHighlightingConstants.DEFAULT_PRE_TAG)
            .postTag(SemanticHighlightingConstants.DEFAULT_POST_TAG)
            .build();
    }
}
