/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
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
 * Tests for the neural highlighting functionality
 */
public class NeuralHighlighterTests extends OpenSearchTestCase {

    private static final String TEST_FIELD = "test_field";
    private static final String MODEL_ID = "test_model_id";
    private static final String TEST_CONTENT = "This is a test content. For highlighting purposes. With multiple sentences.";
    private static final String TEST_QUERY = "test content";

    private NeuralHighlighter highlighter;
    private MLCommonsClientAccessor mlCommonsClientAccessor;
    private MappedFieldType fieldType;
    private NeuralHighlighterManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        highlighter = new NeuralHighlighter();
        highlighter.initialize(mlCommonsClientAccessor);
        fieldType = new TextFieldMapper.TextFieldType(TEST_FIELD);
        manager = new NeuralHighlighterManager(mlCommonsClientAccessor);

        // Setup default mock behavior
        setupDefaultMockBehavior();
    }

    private void setupDefaultMockBehavior() {
        // Mock the ML client response with highlight format
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            SentenceHighlightingRequest request = invocation.getArgument(0);

            // Create mock response with highlights
            List<Map<String, Object>> mockResponse = createMockHighlightResponse(request.getContext());

            // Return mock response
            listener.onResponse(mockResponse);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());
    }

    private List<Map<String, Object>> createMockHighlightResponse(String context) {
        List<Map<String, Object>> mockResponse = new ArrayList<>();
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlights = new ArrayList<>();

        // Add highlight spans based on the input context
        if (context != null && !context.isEmpty()) {
            // For testing, highlight the first 10 characters if context is long enough
            if (context.length() > 10) {
                Map<String, Object> highlight = new HashMap<>();
                highlight.put("start", 0);
                highlight.put("end", 10);
                highlights.add(highlight);
            }

            // If there are multiple sentences, highlight part of the last sentence too
            String[] sentences = context.split("\\. ");
            if (sentences.length > 1) {
                int lastSentencePos = context.lastIndexOf(sentences[sentences.length - 1]);
                if (lastSentencePos > 0 && sentences[sentences.length - 1].length() > 5) {
                    Map<String, Object> highlight = new HashMap<>();
                    highlight.put("start", lastSentencePos);
                    highlight.put("end", lastSentencePos + 5);
                    highlights.add(highlight);
                }
            }
        }

        resultMap.put("highlights", highlights);
        mockResponse.add(resultMap);
        return mockResponse;
    }

    // Tests for the NeuralHighlighter class

    public void testCanHighlight() {
        assertTrue("Should be able to highlight text fields", highlighter.canHighlight(fieldType));
    }

    public void testHighlightWithEmptyField() {
        FieldHighlightContext context = createHighlightContext("", TEST_QUERY);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("is empty"));
    }

    public void testHighlightWithValidInput() {
        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);

        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field", result);
        assertEquals("Should have correct field name", TEST_FIELD, result.name());
        assertNotNull("Should have fragments", result.fragments());
        assertTrue("Should have at least one fragment", result.fragments().length > 0);

        // Check that the highlighting tags are present
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    public void testHighlightWithMissingModelId() {
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, TEST_QUERY);
        context.field.fieldOptions().options().remove("model_id");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("model_id"));
    }

    public void testHighlightWithNeuralKNNQuery() {
        String queryText = "neural query";
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(mock(Query.class), queryText);

        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, neuralQuery);
        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field with neural query", result);
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    public void testHighlightWithModelError() {
        // Setup a custom mock for this test that simulates an error
        MLCommonsClientAccessor errorMlClient = mock(MLCommonsClientAccessor.class);
        NeuralHighlighter customHighlighter = new NeuralHighlighter();
        customHighlighter.initialize(errorMlClient);

        // Mock response that calls the listener with an error
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Simulated model inference error"));
            return null;
        }).when(errorMlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);

        // Should throw an exception due to the model error
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> customHighlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Error during sentence highlighting inference"));
    }

    public void testMultipleInitialization() {
        MLCommonsClientAccessor mlClient1 = mock(MLCommonsClientAccessor.class);
        MLCommonsClientAccessor mlClient2 = mock(MLCommonsClientAccessor.class);

        NeuralHighlighter testHighlighter = new NeuralHighlighter();
        testHighlighter.initialize(mlClient1);

        // Attempting to initialize again should throw an exception
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> testHighlighter.initialize(mlClient2));

        assertNotNull(exception);
        assertEquals("NeuralHighlighter has already been initialized. Multiple initializations are not permitted.", exception.getMessage());
    }

    public void testUninitializedHighlighter() {
        NeuralHighlighter uninitializedHighlighter = new NeuralHighlighter();
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, TEST_QUERY);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> uninitializedHighlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("has not been initialized"));
    }

    // Tests for the NeuralHighlighterManager class

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

    // Integration and error handling tests

    public void testIntegrationWithTermQuery() {
        // Integration test logic for term queries
        TermQuery query = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, query);

        HighlightField result = highlighter.highlight(context);
        assertNotNull("Should return highlight field", result);
        assertTrue(
            "Should contain highlighting tags",
            result.fragments()[0].string().contains("<em>") && result.fragments()[0].string().contains("</em>")
        );
    }

    public void testIntegrationWithBooleanQuery() {
        // Integration test logic for boolean queries
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term(TEST_FIELD, "test")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term(TEST_FIELD, "content")), BooleanClause.Occur.MUST);
        BooleanQuery query = builder.build();

        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, query);

        HighlightField result = highlighter.highlight(context);
        assertNotNull("Should return highlight field", result);
        assertTrue(
            "Should contain highlighting tags",
            result.fragments()[0].string().contains("<em>") && result.fragments()[0].string().contains("</em>")
        );
    }

    public void testIntegrationWithNeuralQuery() {
        // Integration test logic for neural queries
        NeuralKNNQuery query = new NeuralKNNQuery(mock(Query.class), "neural test");

        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, query);

        HighlightField result = highlighter.highlight(context);
        assertNotNull("Should return highlight field", result);
        assertTrue(
            "Should contain highlighting tags",
            result.fragments()[0].string().contains("<em>") && result.fragments()[0].string().contains("</em>")
        );
    }

    public void testNullModelResponse() {
        // Error handling logic for null model response
        MLCommonsClientAccessor nullResponseClient = mock(MLCommonsClientAccessor.class);
        NeuralHighlighter testHighlighter = new NeuralHighlighter();
        testHighlighter.initialize(nullResponseClient);

        // Setup the mock to return null
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(nullResponseClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);

        // The highlight method should return a non-null result with empty text
        HighlightField result = testHighlighter.highlight(context);
        assertNotNull("Result should not be null", result);
        assertEquals("Field name should match", TEST_FIELD, result.name());
        assertEquals("Should have one fragment", 1, result.fragments().length);
        assertEquals("Fragment should be empty", "", result.fragments()[0].string());
    }

    public void testEmptyModelResponse() {
        // Error handling logic for empty model response
        MLCommonsClientAccessor emptyResponseClient = mock(MLCommonsClientAccessor.class);
        NeuralHighlighter testHighlighter = new NeuralHighlighter();
        testHighlighter.initialize(emptyResponseClient);

        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(new ArrayList<>());
            return null;
        }).when(emptyResponseClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);

        HighlightField result = testHighlighter.highlight(context);
        assertNotNull(result);
        assertEquals("Should return original text when no highlights", TEST_CONTENT, result.fragments()[0].string());
    }

    private FieldHighlightContext createHighlightContext(String fieldContent, String queryText) {
        Query query = mock(Query.class);
        when(query.toString()).thenReturn(queryText);
        return createHighlightContext(fieldContent, query);
    }

    private FieldHighlightContext createHighlightContext(String fieldContent, Query query) {
        // Create source lookup with field content
        SourceLookup sourceLookup = new SourceLookup();
        Map<String, Object> source = new HashMap<>();
        source.put(TEST_FIELD, fieldContent);
        sourceLookup.setSource(source);

        // Create highlight options
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", MODEL_ID);

        // Create highlight field context
        SearchHighlightContext.Field field = mock(SearchHighlightContext.Field.class);
        SearchHighlightContext.FieldOptions fieldOptions = mock(SearchHighlightContext.FieldOptions.class);
        when(field.fieldOptions()).thenReturn(fieldOptions);
        when(fieldOptions.options()).thenReturn(options);

        // Create fetch context and search lookup
        SearchLookup searchLookup = mock(SearchLookup.class);
        when(searchLookup.source()).thenReturn(sourceLookup);
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.searchLookup()).thenReturn(searchLookup);

        // Create hitContext
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);

        // Create cache map
        Map<String, Object> cache = new HashMap<>();

        return new FieldHighlightContext(TEST_FIELD, field, fieldType, fetchContext, hitContext, query, false, cache);
    }
}
