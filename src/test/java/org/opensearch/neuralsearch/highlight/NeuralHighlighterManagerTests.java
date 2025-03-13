/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the NeuralHighlighterManager class
 */
public class NeuralHighlighterManagerTests extends OpenSearchTestCase {

    private static final String TEST_FIELD = "test_field";
    private static final String MODEL_ID = "test_model_id";
    private static final String TEST_CONTENT = "This is a test content. For highlighting purposes. With multiple sentences.";
    private static final String TEST_QUERY = "test content";

    private NeuralHighlighterManager manager;
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        manager = new NeuralHighlighterManager(mlCommonsClientAccessor);

        // Setup default mock behavior
        setupDefaultMockBehavior();
    }

    private void setupDefaultMockBehavior() {
        // Mock the ML client response
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);

            // Create mock response with highlights
            List<Map<String, Object>> mockResponse = new ArrayList<>();
            Map<String, Object> resultMap = new HashMap<>();
            List<Map<String, Object>> highlights = new ArrayList<>();

            Map<String, Object> highlight = new HashMap<>();
            highlight.put("start", 0);
            highlight.put("end", 10);
            highlights.add(highlight);

            resultMap.put("highlights", highlights);
            mockResponse.add(resultMap);

            // Return mock response
            listener.onResponse(mockResponse);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());
    }

    public void testGetModelId() {
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", MODEL_ID);

        String modelId = manager.getModelId(options);
        assertEquals("Should extract model ID correctly", MODEL_ID, modelId);
    }

    public void testGetModelIdMissing() {
        Map<String, Object> options = new HashMap<>();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> manager.getModelId(options));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Missing required option: model_id"));
    }

    public void testExtractOriginalQuery() {
        // Test with TermQuery
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "term"));
        String queryText = manager.extractOriginalQuery(termQuery, TEST_FIELD);
        assertEquals("Should extract term text", "term", queryText);

        // Test with BooleanQuery
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term(TEST_FIELD, "term1")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term(TEST_FIELD, "term2")), BooleanClause.Occur.MUST);
        BooleanQuery booleanQuery = builder.build();

        queryText = manager.extractOriginalQuery(booleanQuery, TEST_FIELD);
        assertEquals("Should extract combined terms", "term1 term2", queryText);

        // Test with NeuralKNNQuery
        NeuralKNNQuery neuralQuery = mock(NeuralKNNQuery.class);
        when(neuralQuery.getOriginalQueryText()).thenReturn("neural query");

        queryText = manager.extractOriginalQuery(neuralQuery, TEST_FIELD);
        assertEquals("Should extract neural query text", "neural query", queryText);
    }

    public void testGetHighlightedSentences() {
        String result = manager.getHighlightedSentences(MODEL_ID, TEST_QUERY, TEST_CONTENT);

        assertNotNull("Should return highlighted text", result);
        assertTrue("Should contain highlighting tags", result.contains("<em>") && result.contains("</em>"));
    }

    public void testApplyHighlighting() {
        // Create test highlights
        List<Map<String, Object>> highlights = new ArrayList<>();
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        // Add a valid highlight
        Map<String, Object> highlight1 = new HashMap<>();
        highlight1.put("start", 0);
        highlight1.put("end", 4);
        highlightsList.add(highlight1);

        // Add another valid highlight
        Map<String, Object> highlight2 = new HashMap<>();
        highlight2.put("start", 8);
        highlight2.put("end", 12);
        highlightsList.add(highlight2);

        resultMap.put("highlights", highlightsList);
        highlights.add(resultMap);

        String text = "This is a test string";
        String result = manager.applyHighlighting(text, highlights);

        assertEquals("Should apply highlights correctly", "<em>This</em> is <em>a te</em>st string", result);
    }

    public void testApplyHighlightingWithOverlaps() {
        // Create test highlights with overlapping regions
        List<Map<String, Object>> highlights = new ArrayList<>();
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        // Add overlapping highlights
        Map<String, Object> highlight1 = new HashMap<>();
        highlight1.put("start", 0);
        highlight1.put("end", 6);
        highlightsList.add(highlight1);

        Map<String, Object> highlight2 = new HashMap<>();
        highlight2.put("start", 4);
        highlight2.put("end", 10);
        highlightsList.add(highlight2);

        resultMap.put("highlights", highlightsList);
        highlights.add(resultMap);

        String text = "This is a test string";
        String result = manager.applyHighlighting(text, highlights);

        // Should merge the overlapping highlights
        assertEquals("Should merge overlapping highlights", "<em>This is a </em>test string", result);
    }

    public void testApplyHighlightingWithInvalidPositions() {
        // Create test highlights with invalid positions
        List<Map<String, Object>> highlights = new ArrayList<>();
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        // Add invalid highlight (negative start)
        Map<String, Object> highlight1 = new HashMap<>();
        highlight1.put("start", -1);
        highlight1.put("end", 4);
        highlightsList.add(highlight1);

        // Add invalid highlight (end > text length)
        Map<String, Object> highlight2 = new HashMap<>();
        highlight2.put("start", 0);
        highlight2.put("end", 100);
        highlightsList.add(highlight2);

        // Add invalid highlight (start > end)
        Map<String, Object> highlight3 = new HashMap<>();
        highlight3.put("start", 10);
        highlight3.put("end", 5);
        highlightsList.add(highlight3);

        // Add valid highlight
        Map<String, Object> highlight4 = new HashMap<>();
        highlight4.put("start", 0);
        highlight4.put("end", 4);
        highlightsList.add(highlight4);

        resultMap.put("highlights", highlightsList);
        highlights.add(resultMap);

        String text = "This is a test string";
        String result = manager.applyHighlighting(text, highlights);

        // Should only apply the valid highlight
        assertEquals("Should only apply valid highlights", "<em>This</em> is a test string", result);
    }

    public void testFetchHighlightingResultsWithTimeout() throws Exception {
        // Create a custom mock that delays the response
        MLCommonsClientAccessor delayedMlClient = mock(MLCommonsClientAccessor.class);
        NeuralHighlighterManager customManager = new NeuralHighlighterManager(delayedMlClient);

        // Use a CountDownLatch to control the test timing
        CountDownLatch latch = new CountDownLatch(1);

        // Mock response with delay
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);

            // Start a new thread to delay the response
            new Thread(() -> {
                try {
                    // Simulate a delay longer than any reasonable timeout
                    Thread.sleep(500);

                    // Create mock response
                    List<Map<String, Object>> mockResponse = new ArrayList<>();
                    Map<String, Object> resultMap = new HashMap<>();
                    List<Map<String, Object>> highlights = new ArrayList<>();

                    Map<String, Object> highlight = new HashMap<>();
                    highlight.put("start", 0);
                    highlight.put("end", 4);
                    highlights.add(highlight);

                    resultMap.put("highlights", highlights);
                    mockResponse.add(resultMap);

                    listener.onResponse(mockResponse);
                } catch (InterruptedException e) {
                    listener.onFailure(e);
                } finally {
                    latch.countDown();
                }
            }).start();

            return null;
        }).when(delayedMlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Call the method in a separate thread so we can interrupt it
        Thread testThread = new Thread(() -> {
            try {
                customManager.fetchHighlightingResults(MODEL_ID, TEST_QUERY, TEST_CONTENT);
                fail("Should have been interrupted");
            } catch (OpenSearchException e) {
                // Expected exception
                assertTrue(e.getMessage().contains("Interrupted while waiting"));
            }
        });

        testThread.start();

        // Wait a bit and then interrupt the thread
        Thread.sleep(100);
        testThread.interrupt();

        // Wait for the mock to finish
        latch.await(1, TimeUnit.SECONDS);
    }
}
