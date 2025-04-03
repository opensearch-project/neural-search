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
import org.opensearch.neuralsearch.processor.highlight.SentenceHighlightingRequest;
import org.opensearch.neuralsearch.query.NeuralKNNQuery;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.neuralsearch.highlight.extractor.QueryTextExtractorRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for the SemanticHighlighterEngine class
 */
public class SemanticHighlighterEngineTests extends OpenSearchTestCase {

    private static final String TEST_FIELD = "test_field";
    private static final String MODEL_ID = "test_model_id";
    private static final String TEST_CONTENT = "This is a test content. For highlighting purposes. With multiple sentences.";
    private static final String TEST_QUERY = "test content";

    private SemanticHighlighterEngine highlighterEngine;
    private MLCommonsClientAccessor mlCommonsClientAccessor;
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

        String modelId = highlighterEngine.getModelId(options);
        assertEquals("Should extract model ID correctly", MODEL_ID, modelId);
    }

    public void testGetModelIdMissing() {
        Map<String, Object> options = new HashMap<>();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighterEngine.getModelId(options));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("must be a non-null string"));
    }

    public void testGetModelIdWrongType() {
        Map<String, Object> options = new HashMap<>();
        options.put("model_id", 123); // Put an Integer instead of String

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> highlighterEngine.getModelId(options));
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("must be a non-null string"));
        assertTrue(exception.getMessage().contains("Integer"));
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

    public void testApplyHighlightingWithInvalidPositions() {
        // Create test highlights with invalid positions
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        // Define test string and get its length
        String text = "This is a test string";
        int textLength = text.length();  // This will be 21

        // Test case 1: negative start position
        Map<String, Object> highlight1 = new HashMap<>();
        highlight1.put("start", -1);
        highlight1.put("end", 4);
        highlightsList.add(highlight1);
        resultMap.put("highlights", highlightsList);

        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>")
        );
        assertEquals(
            "Should throw correct error message for invalid positions",
            String.format(
                Locale.ROOT,
                "Invalid highlight positions: start=-1, end=4, textLength=%d. Positions must satisfy: 0 <= start < end <= textLength",
                textLength
            ),
            exception.getMessage()
        );

        // Test case 2: end position exceeds text length
        highlightsList.clear();
        Map<String, Object> highlight2 = new HashMap<>();
        highlight2.put("start", 0);
        highlight2.put("end", 100);
        highlightsList.add(highlight2);

        exception = expectThrows(OpenSearchException.class, () -> highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>"));
        assertEquals(
            "Should throw correct error message for invalid positions",
            String.format(
                Locale.ROOT,
                "Invalid highlight positions: start=0, end=100, textLength=%d. Positions must satisfy: 0 <= start < end <= textLength",
                textLength
            ),
            exception.getMessage()
        );

        // Test case 3: start position >= end position
        highlightsList.clear();
        Map<String, Object> highlight3 = new HashMap<>();
        highlight3.put("start", 10);
        highlight3.put("end", 5);
        highlightsList.add(highlight3);

        exception = expectThrows(OpenSearchException.class, () -> highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>"));
        assertEquals(
            "Should throw correct error message for invalid positions",
            String.format(
                Locale.ROOT,
                "Invalid highlight positions: start=10, end=5, textLength=%d. Positions must satisfy: 0 <= start < end <= textLength",
                textLength
            ),
            exception.getMessage()
        );
    }

    public void testApplyHighlightingWithUnsortedPositions() {
        // Create test highlights with unsorted positions
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        String text = "This is a test string";

        // Add highlights in unsorted order
        Map<String, Object> highlight1 = new HashMap<>();
        highlight1.put("start", 8);  // "a"
        highlight1.put("end", 9);
        highlightsList.add(highlight1);

        Map<String, Object> highlight2 = new HashMap<>();
        highlight2.put("start", 0);  // "This"
        highlight2.put("end", 4);
        highlightsList.add(highlight2);

        resultMap.put("highlights", highlightsList);

        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>")
        );
        assertEquals(
            "Should throw correct error message for unsorted positions",
            "Internal error while applying semantic highlight: received unsorted highlights from model",
            exception.getMessage()
        );
    }

    public void testApplyHighlightingWithInvalidHighlightMap() {
        // Test with invalid highlight map format
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        // Add an invalid highlight (not a Map<String, Number>)
        Map<String, Object> invalidHighlight = new HashMap<>();
        invalidHighlight.put("start", "not a number");
        invalidHighlight.put("end", "not a number");
        highlightsList.add(invalidHighlight);

        resultMap.put("highlights", highlightsList);

        String text = "This is a test string";
        ClassCastException exception = expectThrows(
            ClassCastException.class,
            () -> highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>")
        );
        assertTrue(exception.getMessage().contains("cannot be cast to class java.lang.Number"));
    }

    public void testApplyHighlightingWithMissingPositions() {
        // Test with missing start/end positions
        Map<String, Object> resultMap = new HashMap<>();
        List<Map<String, Object>> highlightsList = new ArrayList<>();

        // Add highlight with missing end position
        Map<String, Object> invalidHighlight = new HashMap<>();
        invalidHighlight.put("start", 0);
        // end position is missing
        highlightsList.add(invalidHighlight);

        resultMap.put("highlights", highlightsList);

        String text = "This is a test string";
        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>")
        );
        assertTrue(exception.getMessage().contains("Missing start or end position"));
    }

    public void testApplyHighlightingWithEmptyHighlights() {
        // Test with empty highlights list
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("highlights", new ArrayList<>());

        String text = "This is a test string";
        String result = highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>");
        assertEquals("Should return original text when no highlights", text, result);
    }

    public void testApplyHighlightingWithMissingHighlightsKey() {
        Map<String, Object> resultMap = new HashMap<>();
        String text = "This is a test string";
        String result = highlighterEngine.applyHighlighting(text, resultMap, "<em>", "</em>");
        assertNull("Should return null when highlights key is missing", result);
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
}
