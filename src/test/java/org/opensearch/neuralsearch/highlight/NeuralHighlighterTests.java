/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.apache.lucene.search.Query;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.OpenSearchException;
import org.opensearch.neuralsearch.processor.SentenceHighlightingRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

public class NeuralHighlighterTests extends OpenSearchTestCase {

    private NeuralHighlighter highlighter;
    private MappedFieldType fieldType;
    private static final String TEST_FIELD = "test_field";
    private static final String MODEL_ID = "test_model_id";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MLCommonsClientAccessor mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        highlighter = new NeuralHighlighter();
        NeuralHighlighter.initialize(mlCommonsClientAccessor);
        fieldType = new TextFieldMapper.TextFieldType(TEST_FIELD);

        // Mock the ML client response with the new highlight format
        doAnswer(invocation -> {
            // Get the ActionListener from the method call
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            SentenceHighlightingRequest request = invocation.getArgument(0);
            String context = request.getContext();

            // Create mock response with highlights containing start and end positions
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

            // Return mock response
            listener.onResponse(mockResponse);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());
    }

    public void testCanHighlight() {
        assertTrue("Should be able to highlight text fields", highlighter.canHighlight(fieldType));
    }

    public void testHighlightWithEmptyField() {
        FieldHighlightContext context = createHighlightContext("", "test query");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("is empty"));
    }

    public void testHighlightWithEmptyQuery() {
        FieldHighlightContext context = createHighlightContext("test content", "");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("query text is empty"));
    }

    public void testHighlightWithValidInput() {
        String fieldContent = "This is a test content. For highlighting purposes. With multiple sentences.";
        String queryText = "test content";
        FieldHighlightContext context = createHighlightContext(fieldContent, queryText);

        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field", result);
        assertEquals("Should have correct field name", TEST_FIELD, result.name());
        assertNotNull("Should have fragments", result.fragments());
        assertTrue("Should have at least one fragment", result.fragments().length > 0);

        // Check that the highlighting tags are present
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    public void testHighlightWithSingleSentence() throws IOException {
        String fieldContent = "This is a single sentence without period";
        String queryText = "single sentence";
        FieldHighlightContext context = createHighlightContext(fieldContent, queryText);

        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field", result);
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    public void testHighlightWithMissingModelId() {
        FieldHighlightContext context = createHighlightContext("test content", "query");
        context.field.fieldOptions().options().remove("model_id");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("model_id"));
    }

    public void testHighlightWithNeuralKNNQuery() {
        String fieldContent = "This is a test content. For highlighting purposes. With multiple sentences.";
        String queryText = "neural query";
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(mock(Query.class), queryText);

        FieldHighlightContext context = createHighlightContext(fieldContent, neuralQuery);
        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field with neural query", result);
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    /**
     * Tests that the highlighter gracefully handles invalid position values
     * without throwing exceptions.
     */
    public void testHighlightWithInvalidPositions() throws IOException {
        // Setup a custom mock for this test
        MLCommonsClientAccessor customMlClient = mock(MLCommonsClientAccessor.class);
        NeuralHighlighter customHighlighter = new NeuralHighlighter();
        NeuralHighlighter.initialize(customMlClient);

        // Mock response with invalid positions
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);

            List<Map<String, Object>> mockResponse = new ArrayList<>();
            Map<String, Object> resultMap = new HashMap<>();
            List<Map<String, Object>> highlights = new ArrayList<>();

            // Add highlight with negative start position
            Map<String, Object> highlight1 = new HashMap<>();
            highlight1.put("start", -5);
            highlight1.put("end", 10);
            highlights.add(highlight1);

            // Add highlight with end position beyond text length
            Map<String, Object> highlight2 = new HashMap<>();
            highlight2.put("start", 5);
            highlight2.put("end", 1000);
            highlights.add(highlight2);

            // Add highlight with start > end
            Map<String, Object> highlight3 = new HashMap<>();
            highlight3.put("start", 20);
            highlight3.put("end", 15);
            highlights.add(highlight3);

            // Add valid highlight to ensure some highlighting occurs
            Map<String, Object> highlight4 = new HashMap<>();
            highlight4.put("start", 5);
            highlight4.put("end", 10);
            highlights.add(highlight4);

            resultMap.put("highlights", highlights);
            mockResponse.add(resultMap);

            listener.onResponse(mockResponse);
            return null;
        }).when(customMlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        String fieldContent = "This is a test content for invalid positions.";
        String queryText = "test content";
        FieldHighlightContext context = createHighlightContext(fieldContent, queryText);

        // Should not throw exception despite invalid positions
        HighlightField result = customHighlighter.highlight(context);

        assertNotNull("Should return highlight field", result);
        String highlightedText = result.fragments()[0].string();

        // Should still have highlighting from the valid position
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    /**
     * Tests that the highlighter properly handles errors in the ML model inference.
     * This test verifies that an appropriate exception is thrown when the inference
     * operation fails.
     */
    public void testHighlightWithModelError() {
        // Setup a custom mock for this test that simulates an error
        MLCommonsClientAccessor errorMlClient = mock(MLCommonsClientAccessor.class);
        NeuralHighlighter customHighlighter = new NeuralHighlighter();
        NeuralHighlighter.initialize(errorMlClient);

        // Mock response that calls the listener with an error
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Simulated model inference error"));
            return null;
        }).when(errorMlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        String fieldContent = "This is a test content for error handling.";
        String queryText = "test content";
        FieldHighlightContext context = createHighlightContext(fieldContent, queryText);

        // Should throw an exception due to the model error
        OpenSearchException ose = expectThrows(OpenSearchException.class, () -> customHighlighter.highlight(context));
        // Just verify that we get an exception, the exact message might vary
        assertNotNull("Should throw an exception with a message", ose.getMessage());
    }

    private FieldHighlightContext createHighlightContext(String fieldContent, String queryText) {
        return createHighlightContext(fieldContent, new NeuralKNNQuery(mock(Query.class), queryText));
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
