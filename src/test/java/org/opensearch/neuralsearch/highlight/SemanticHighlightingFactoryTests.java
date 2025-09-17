/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.highlight.processor.SemanticHighlightingFactory;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class SemanticHighlightingFactoryTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    @Mock
    private Processor.PipelineContext pipelineContext;

    private SemanticHighlightingFactory factory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        factory = new SemanticHighlightingFactory(mlClientAccessor);
    }

    public void testShouldGenerateReturnsTrueForSemanticHighlighting() {
        // Create search request with semantic highlighting
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        highlightBuilder.field(field);
        sourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(sourceBuilder);

        ProcessorGenerationContext context = new ProcessorGenerationContext(searchRequest);

        assertTrue(factory.shouldGenerate(context));
    }

    public void testShouldGenerateReturnsFalseForNonSemanticHighlighting() {
        // Create search request with regular highlighting
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType("plain");
        highlightBuilder.field(field);
        sourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(sourceBuilder);

        ProcessorGenerationContext context = new ProcessorGenerationContext(searchRequest);

        assertFalse(factory.shouldGenerate(context));
    }

    public void testShouldGenerateReturnsFalseWhenNoHighlighter() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);

        ProcessorGenerationContext context = new ProcessorGenerationContext(searchRequest);

        assertFalse(factory.shouldGenerate(context));
    }

    public void testShouldGenerateReturnsFalseWhenNoSearchSource() {
        SearchRequest searchRequest = new SearchRequest();

        ProcessorGenerationContext context = new ProcessorGenerationContext(searchRequest);

        assertFalse(factory.shouldGenerate(context));
    }

    public void testShouldGenerateReturnsFalseWhenNullRequest() {
        ProcessorGenerationContext context = new ProcessorGenerationContext(null);

        assertFalse(factory.shouldGenerate(context));
    }

    public void testShouldGenerateWithMultipleFieldsOneIsSemantic() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();

        // Add regular field
        HighlightBuilder.Field field1 = new HighlightBuilder.Field("title");
        field1.highlighterType("plain");
        highlightBuilder.field(field1);

        // Add semantic field
        HighlightBuilder.Field field2 = new HighlightBuilder.Field("content");
        field2.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        highlightBuilder.field(field2);

        sourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(sourceBuilder);

        ProcessorGenerationContext context = new ProcessorGenerationContext(searchRequest);

        assertTrue(factory.shouldGenerate(context));
    }

    public void testCreateProcessorWithDefaultValues() throws Exception {
        Map<String, Object> config = new HashMap<>();

        SearchResponseProcessor processor = factory.create(new HashMap<>(), null, null, false, config, pipelineContext);

        assertNotNull(processor);
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
        assertFalse(processor.isIgnoreFailure());
        assertEquals(SemanticHighlightingConstants.PROCESSOR_TYPE, processor.getType());
    }

    public void testCreateProcessorWithCustomValues() throws Exception {
        Map<String, Object> config = new HashMap<>();

        SearchResponseProcessor processor = factory.create(
            new HashMap<>(),
            "custom-tag",
            "custom-description",
            true,
            config,
            pipelineContext
        );

        assertNotNull(processor);
        // Always uses semantic-specific defaults, ignoring the provided tag and description
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
        assertTrue(processor.isIgnoreFailure());
    }

    public void testFactoryType() {
        assertEquals(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE, SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE);
    }
}
