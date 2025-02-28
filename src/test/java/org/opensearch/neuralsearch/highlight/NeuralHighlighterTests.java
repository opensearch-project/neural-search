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
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.OpenSearchException;

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

        // Setup mock behavior for MLCommonsClientAccessor
        doAnswer(invocation -> {
            // TextInferenceRequest request = invocation.getArgument(0);
            ActionListener<List<List<Number>>> listener = invocation.getArgument(1);
            List<List<Number>> mockResponse = new ArrayList<>();
            mockResponse.add(List.of(0.1f, 0.2f, 0.3f));
            listener.onResponse(mockResponse);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(any(TextInferenceRequest.class), any());
    }

    public void testCanHighlight() {
        assertTrue("Should be able to highlight text fields", highlighter.canHighlight(fieldType));
    }

    public void testHighlightWithEmptyField() throws IOException {
        FieldHighlightContext context = createHighlightContext("", "test query");
        HighlightField result = highlighter.highlight(context);
        assertNull("Should return null for empty field", result);
    }

    public void testHighlightWithEmptyQuery() throws IOException {
        FieldHighlightContext context = createHighlightContext("test content", "");
        HighlightField result = highlighter.highlight(context);
        assertNull("Should return null for empty query", result);
    }

    public void testHighlightWithValidInput() throws IOException {
        String fieldContent = "This is a test content for highlighting";
        String queryText = "test content";
        FieldHighlightContext context = createHighlightContext(fieldContent, queryText);

        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field", result);
        assertEquals("Should have correct field name", TEST_FIELD, result.name());
        assertNotNull("Should have fragments", result.fragments());
        assertTrue("Should have at least one fragment", result.fragments().length > 0);
    }

    public void testHighlightWithMissingModelId() {
        FieldHighlightContext context = createHighlightContext("test content", "query");
        context.field.fieldOptions().options().remove("model_id");

        OpenSearchException ose = expectThrows(OpenSearchException.class, () -> highlighter.highlight(context));
        assertNotNull(ose.getCause());
        assertTrue(ose.getCause() instanceof IllegalArgumentException);
        assertTrue(ose.getCause().getMessage().contains("model_id"));
    }

    public void testHighlightWithNonNeuralQuery() throws IOException {
        String fieldContent = "This is a test content";
        Query nonNeuralQuery = mock(Query.class);
        when(nonNeuralQuery.toString()).thenReturn("test");

        FieldHighlightContext context = createHighlightContext(fieldContent, nonNeuralQuery);
        HighlightField result = highlighter.highlight(context);

        assertNotNull("Should return highlight field even with non-neural query", result);
        assertFalse("Should contain highlighted content", result.fragments()[0].string().isEmpty());
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

        // Instead, directly create hitContext
        FetchSubPhase.HitContext hitContext = mock(FetchSubPhase.HitContext.class);
        when(hitContext.sourceLookup()).thenReturn(sourceLookup);

        // Create cache map
        Map<String, Object> cache = new HashMap<>();

        return new FieldHighlightContext(TEST_FIELD, field, fieldType, fetchContext, hitContext, query, false, cache);
    }
}
