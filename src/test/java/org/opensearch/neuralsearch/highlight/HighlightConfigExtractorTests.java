/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

public class HighlightConfigExtractorTests extends OpenSearchTestCase {

    @Mock
    private SearchResponse searchResponse;

    private HighlightConfigExtractor extractor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        extractor = new HighlightConfigExtractor();
    }

    public void testExtractWithValidSemanticField() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("content", "test query"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);

        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, "test-model-id");
        highlightBuilder.options(options);
        highlightBuilder.field(field);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        try (var mockedStatic = mockStatic(ProcessorUtils.class)) {
            mockedStatic.when(() -> ProcessorUtils.extractQueryTextFromBuilder(any())).thenReturn("test query");

            HighlightConfig config = extractor.extract(request, searchResponse);

            // Verify
            assertNotNull(config);
            assertEquals("content", config.getFieldName());
            assertEquals("test-model-id", config.getModelId());
            assertEquals("test query", config.getQueryText());
            assertNull(config.getValidationError());
        }
    }

    public void testExtractWithMultipleFields() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("content", "search text"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();

        // Add non-semantic field
        HighlightBuilder.Field field1 = new HighlightBuilder.Field("title");
        field1.highlighterType("plain");
        highlightBuilder.field(field1);

        // Add semantic field
        HighlightBuilder.Field field2 = new HighlightBuilder.Field("description");
        field2.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        Map<String, Object> fieldOptions = new HashMap<>();
        fieldOptions.put(SemanticHighlightingConstants.MODEL_ID, "field-model-id");
        field2.options(fieldOptions);
        highlightBuilder.field(field2);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        try (var mockedStatic = mockStatic(ProcessorUtils.class)) {
            mockedStatic.when(() -> ProcessorUtils.extractQueryTextFromBuilder(any())).thenReturn("search text");

            HighlightConfig config = extractor.extract(request, searchResponse);

            // Verify - should extract only semantic field
            assertNotNull(config);
            assertEquals("description", config.getFieldName());
            assertEquals("field-model-id", config.getModelId());
            assertEquals("search text", config.getQueryText());
        }
    }

    public void testExtractWithMissingModelId() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("content", "test"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        highlightBuilder.field(field);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        try (var mockedStatic = mockStatic(ProcessorUtils.class)) {
            mockedStatic.when(() -> ProcessorUtils.extractQueryTextFromBuilder(any())).thenReturn("test");

            HighlightConfig config = extractor.extract(request, searchResponse);

            // Verify - extraction succeeds but modelId is null
            assertNotNull(config);
            assertEquals("content", config.getFieldName());
            assertNull(config.getModelId());  // No model ID provided
            assertEquals("test", config.getQueryText());
        }
    }

    public void testExtractWithMissingQueryText() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // No query set

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);

        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, "test-model");
        highlightBuilder.options(options);
        highlightBuilder.field(field);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify
        assertNotNull(config);
        assertEquals("content", config.getFieldName());
        assertEquals("test-model", config.getModelId());
        assertNull(config.getQueryText());  // No query text extracted
    }

    public void testExtractCustomTags() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<mark>");
        highlightBuilder.postTags("</mark>");

        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);

        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, "model");
        highlightBuilder.options(options);
        highlightBuilder.field(field);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify
        assertNotNull(config);
        assertEquals("<mark>", config.getPreTag());
        assertEquals("</mark>", config.getPostTag());
    }

    public void testExtractWithFieldSpecificOptions() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);

        // Set field-specific options
        Map<String, Object> fieldOptions = new HashMap<>();
        fieldOptions.put(SemanticHighlightingConstants.MODEL_ID, "field-specific-model");
        fieldOptions.put(SemanticHighlightingConstants.PRE_TAG, "<b>");
        fieldOptions.put(SemanticHighlightingConstants.POST_TAG, "</b>");
        field.options(fieldOptions);

        highlightBuilder.field(field);
        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify
        assertNotNull(config);
        assertEquals("field-specific-model", config.getModelId());
        assertEquals("<b>", config.getPreTag());
        assertEquals("</b>", config.getPostTag());
    }

    public void testExtractWithNullHighlighter() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // No highlighter set
        request.source(sourceBuilder);

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify - should return empty config with validation error
        assertNotNull(config);
        assertNull(config.getFieldName());
        assertNull(config.getModelId());
        assertNull(config.getQueryText());
        assertEquals("No semantic highlight field found", config.getValidationError());
    }

    public void testExtractWithNullRequest() {
        // Execute
        HighlightConfig config = extractor.extract(null, searchResponse);

        // Verify - should return empty config with validation error
        assertNotNull(config);
        assertNull(config.getFieldName());
        assertNull(config.getModelId());
        assertNull(config.getQueryText());
        assertEquals("No semantic highlight field found", config.getValidationError());
    }

    public void testExtractWithNullSearchSource() {
        // Setup
        SearchRequest request = new SearchRequest();
        // No source set

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify - should return empty config with validation error
        assertNotNull(config);
        assertNull(config.getFieldName());
        assertNull(config.getModelId());
        assertNull(config.getQueryText());
        assertEquals("No semantic highlight field found", config.getValidationError());
    }

    public void testExtractDefaults() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);

        // Minimal config - should use defaults
        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, "model");
        highlightBuilder.options(options);
        highlightBuilder.field(field);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify defaults
        assertNotNull(config);
        assertEquals(SemanticHighlightingConstants.DEFAULT_PRE_TAG, config.getPreTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_POST_TAG, config.getPostTag());
        assertFalse(config.isBatchInference());  // Default is false
        assertEquals(SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE, config.getMaxBatchSize());
    }

    public void testExtractWithInvalidOptionTypes() {
        // Setup
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);

        Map<String, Object> options = new HashMap<>();
        options.put(SemanticHighlightingConstants.MODEL_ID, 123);  // Wrong type - should be ignored
        options.put(SemanticHighlightingConstants.BATCH_INFERENCE, "true");  // Wrong type - should default to false
        options.put(SemanticHighlightingConstants.MAX_INFERENCE_BATCH_SIZE, "100");  // Wrong type - should use default
        highlightBuilder.options(options);
        highlightBuilder.field(field);

        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);

        // Execute
        HighlightConfig config = extractor.extract(request, searchResponse);

        // Verify - invalid types should be ignored or use defaults
        assertNotNull(config);
        assertNull(config.getModelId());  // Invalid type, so null
        assertFalse(config.isBatchInference());  // Invalid type, so default false
        assertEquals(SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE, config.getMaxBatchSize());
    }
}
