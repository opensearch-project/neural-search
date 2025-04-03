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
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the semantic highlighting functionality
 */
public class SemanticHighlighterTests extends OpenSearchTestCase {

    private static final String TEST_FIELD = "test_field";
    private static final String MODEL_ID = "test_model_id";
    private static final String TEST_CONTENT = "This is a test content. For highlighting purposes. With multiple sentences.";
    private static final String TEST_QUERY = "test content";

    private SemanticHighlighter highlighter;
    private MLCommonsClientAccessor mlCommonsClientAccessor;
    private MappedFieldType fieldType;
    private SemanticHighlighterEngine highlighterEngine;
    private QueryTextExtractorRegistry queryTextExtractorRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mlCommonsClientAccessor = mock(MLCommonsClientAccessor.class);
        queryTextExtractorRegistry = new QueryTextExtractorRegistry();
        highlighterEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(mlCommonsClientAccessor)
            .queryTextExtractorRegistry(queryTextExtractorRegistry)
            .build();

        highlighter = new SemanticHighlighter();
        highlighter.initialize(highlighterEngine);

        fieldType = new TextFieldMapper.TextFieldType(TEST_FIELD);

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

    private void setupDefaultHighlightTags(FieldHighlightContext context) {
        // In the real OpenSearch environment, these default tags are provided by the core
        when(context.field.fieldOptions().preTags()).thenReturn(new String[] { "<em>" });
        when(context.field.fieldOptions().postTags()).thenReturn(new String[] { "</em>" });
    }

    // Tests for the SemanticHighlighter class

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
        setupDefaultHighlightTags(context);

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
        String queryText = "semantic query";
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term(TEST_FIELD, "dummy")), queryText);

        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, neuralQuery);
        setupDefaultHighlightTags(context);

        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field with semantic query", result);
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain highlighting tags", highlightedText.contains("<em>") && highlightedText.contains("</em>"));
    }

    public void testHighlightWithModelError() {
        // Setup a custom mock for this test that simulates an error
        MLCommonsClientAccessor errorMlClient = mock(MLCommonsClientAccessor.class);
        QueryTextExtractorRegistry errorRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine errorEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(errorMlClient)
            .queryTextExtractorRegistry(errorRegistry)
            .build();

        SemanticHighlighter customHighlighter = new SemanticHighlighter();
        customHighlighter.initialize(errorEngine);

        // Mock response that calls the listener with an error
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Simulated model inference error"));
            return null;
        }).when(errorMlClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);
        setupDefaultHighlightTags(context);

        // Should throw an exception due to the model error
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> customHighlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Error during sentence highlighting inference"));
    }

    public void testMultipleInitialization() {
        MLCommonsClientAccessor mlClient1 = mock(MLCommonsClientAccessor.class);
        QueryTextExtractorRegistry registry1 = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine engine1 = SemanticHighlighterEngine.builder()
            .mlCommonsClient(mlClient1)
            .queryTextExtractorRegistry(registry1)
            .build();

        MLCommonsClientAccessor mlClient2 = mock(MLCommonsClientAccessor.class);
        QueryTextExtractorRegistry registry2 = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine engine2 = SemanticHighlighterEngine.builder()
            .mlCommonsClient(mlClient2)
            .queryTextExtractorRegistry(registry2)
            .build();

        SemanticHighlighter testHighlighter = new SemanticHighlighter();
        testHighlighter.initialize(engine1);

        // Attempting to initialize again should throw an exception
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> testHighlighter.initialize(engine2));

        assertNotNull(exception);
        assertEquals(
            "SemanticHighlighterEngine has already been initialized. Multiple initializations are not permitted.",
            exception.getMessage()
        );
    }

    public void testUninitializedHighlighter() {
        SemanticHighlighter uninitializedHighlighter = new SemanticHighlighter();
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, TEST_QUERY);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> uninitializedHighlighter.highlight(context));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("has not been initialized"));
    }

    // Tests for the SemanticHighlighterEngine class

    public void testGetModelId() {
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", MODEL_ID);

        String modelId = highlighterEngine.getModelId(options);
        assertEquals("Should extract model ID correctly", MODEL_ID, modelId);
    }

    public void testExtractOriginalQuery() {
        // Test with TermQuery
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "term"));
        String queryText = highlighterEngine.extractOriginalQuery(termQuery, TEST_FIELD);
        assertEquals("Should extract term text", "term", queryText);

        // Test with BooleanQuery
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term(TEST_FIELD, "term1")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term(TEST_FIELD, "term2")), BooleanClause.Occur.MUST);
        BooleanQuery booleanQuery = builder.build();

        queryText = highlighterEngine.extractOriginalQuery(booleanQuery, TEST_FIELD);
        assertEquals("Should extract combined terms", "term1 term2", queryText);

        // Test with NeuralKNNQuery
        NeuralKNNQuery neuralQuery = new NeuralKNNQuery(new TermQuery(new Term(TEST_FIELD, "dummy")), "semantic query");
        queryText = highlighterEngine.extractOriginalQuery(neuralQuery, TEST_FIELD);
        assertEquals("Should extract semantic query text", "semantic query", queryText);
    }

    public void testGetHighlightedSentences() {
        String result = highlighterEngine.getHighlightedSentences(MODEL_ID, TEST_QUERY, TEST_CONTENT, "<em>", "</em>");

        assertNotNull("Should return highlighted text", result);
        assertTrue("Should contain highlighting tags", result.contains("<em>") && result.contains("</em>"));
    }

    public void testApplyHighlighting() {
        // Create test highlights
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

        String text = "This is a test string";
        String result = highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>");

        assertEquals("Should apply highlights correctly", "<em>This</em> is <em>a te</em>st string", result);
    }

    public void testCustomTags() {
        // Test with custom tags
        String result = highlighterEngine.getHighlightedSentences(MODEL_ID, TEST_QUERY, TEST_CONTENT, "<mark>", "</mark>");
        assertNotNull("Should return highlighted text", result);
        assertTrue("Should contain custom highlighting tags", result.contains("<mark>") && result.contains("</mark>"));
        assertFalse("Should not contain default highlighting tags", result.contains("<em>") || result.contains("</em>"));

        // Test with different custom tags
        result = highlighterEngine.getHighlightedSentences(MODEL_ID, TEST_QUERY, TEST_CONTENT, "<span class='highlight'>", "</span>");
        assertNotNull("Should return highlighted text", result);
        assertTrue(
            "Should contain new custom highlighting tags",
            result.contains("<span class='highlight'>") && result.contains("</span>")
        );
        assertFalse("Should not contain previous highlighting tags", result.contains("<mark>") || result.contains("</mark>"));
    }

    public void testIntegrationWithTermQuery() {
        // Integration test logic for term queries
        TermQuery query = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, query);
        setupDefaultHighlightTags(context);

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
        setupDefaultHighlightTags(context);

        HighlightField result = highlighter.highlight(context);
        assertNotNull("Should return highlight field", result);
        assertTrue(
            "Should contain highlighting tags",
            result.fragments()[0].string().contains("<em>") && result.fragments()[0].string().contains("</em>")
        );
    }

    public void testIntegrationWithNeuralQuery() {
        // Integration test logic for neural queries
        NeuralKNNQuery query = new NeuralKNNQuery(mock(Query.class), "semantic test");

        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, query);
        setupDefaultHighlightTags(context);

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
        QueryTextExtractorRegistry nullRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine nullResponseEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(nullResponseClient)
            .queryTextExtractorRegistry(nullRegistry)
            .build();

        SemanticHighlighter testHighlighter = new SemanticHighlighter();
        testHighlighter.initialize(nullResponseEngine);

        // Setup the mock to return null
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(nullResponseClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);
        setupDefaultHighlightTags(context);

        // The highlight method should return null when model returns null
        HighlightField result = testHighlighter.highlight(context);
        assertNull("Result should be null when model returns null", result);
    }

    public void testEmptyModelResponse() {
        // Error handling logic for empty model response
        MLCommonsClientAccessor emptyResponseClient = mock(MLCommonsClientAccessor.class);
        QueryTextExtractorRegistry emptyRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine emptyResponseEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(emptyResponseClient)
            .queryTextExtractorRegistry(emptyRegistry)
            .build();

        SemanticHighlighter testHighlighter = new SemanticHighlighter();
        testHighlighter.initialize(emptyResponseEngine);

        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(new ArrayList<>());
            return null;
        }).when(emptyResponseClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);
        setupDefaultHighlightTags(context);

        // The highlight method should return null when model returns empty list
        HighlightField result = testHighlighter.highlight(context);
        assertNull("Result should be null when model returns empty list", result);
    }

    public void testHighlightWithCustomTags() {
        // Create a context with custom tags
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);
        when(context.field.fieldOptions().preTags()).thenReturn(new String[] { "<mark>" });
        when(context.field.fieldOptions().postTags()).thenReturn(new String[] { "</mark>" });

        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field", result);
        String highlightedText = result.fragments()[0].string();
        assertTrue("Text should contain custom tags", highlightedText.contains("<mark>") && highlightedText.contains("</mark>"));
        assertFalse("Text should not contain default tags", highlightedText.contains("<em>") || highlightedText.contains("</em>"));
    }

    public void testHighlightWithPartialTags() {
        // Since OpenSearch core doesn't allow setting only one of preTags or postTags,
        // we're skipping this test as it's not a valid use case.
    }

    public void testHighlightWithNullResponse() {
        // Setup a custom mock for this test that returns null
        MLCommonsClientAccessor nullResponseClient = mock(MLCommonsClientAccessor.class);
        QueryTextExtractorRegistry nullRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine nullResponseEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(nullResponseClient)
            .queryTextExtractorRegistry(nullRegistry)
            .build();

        SemanticHighlighter customHighlighter = new SemanticHighlighter();
        customHighlighter.initialize(nullResponseEngine);

        // Mock response that calls the listener with null
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(nullResponseClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);
        setupDefaultHighlightTags(context);

        // Should return null due to null response
        HighlightField result = customHighlighter.highlight(context);
        assertNull("Should return null for null response", result);
    }

    public void testHighlightWithEmptyResponse() {
        // Setup a custom mock for this test that returns empty list
        MLCommonsClientAccessor emptyResponseClient = mock(MLCommonsClientAccessor.class);
        QueryTextExtractorRegistry emptyRegistry = new QueryTextExtractorRegistry();
        SemanticHighlighterEngine emptyResponseEngine = SemanticHighlighterEngine.builder()
            .mlCommonsClient(emptyResponseClient)
            .queryTextExtractorRegistry(emptyRegistry)
            .build();

        SemanticHighlighter customHighlighter = new SemanticHighlighter();
        customHighlighter.initialize(emptyResponseEngine);

        // Mock response that calls the listener with empty list
        doAnswer(invocation -> {
            ActionListener<List<Map<String, Object>>> listener = invocation.getArgument(1);
            listener.onResponse(new ArrayList<>());
            return null;
        }).when(emptyResponseClient).inferenceSentenceHighlighting(any(SentenceHighlightingRequest.class), any());

        // Create a context with a TermQuery instead of a mocked Query
        TermQuery termQuery = new TermQuery(new Term(TEST_FIELD, "test"));
        FieldHighlightContext context = createHighlightContext(TEST_CONTENT, termQuery);
        setupDefaultHighlightTags(context);

        // Should return null due to empty response
        HighlightField result = customHighlighter.highlight(context);
        assertNull("Should return null for empty response", result);
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
