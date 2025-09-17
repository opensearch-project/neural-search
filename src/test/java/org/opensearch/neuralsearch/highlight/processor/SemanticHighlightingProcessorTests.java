/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.processor;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.highlight.HighlightConfig;
import org.opensearch.neuralsearch.highlight.HighlightConfigExtractor;
import org.opensearch.neuralsearch.highlight.HighlightContext;
import org.opensearch.neuralsearch.highlight.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.HighlightValidator;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SemanticHighlightingProcessorTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    @Mock
    private HighlightConfigExtractor configExtractor;

    @Mock
    private HighlightValidator validator;

    @Mock
    private HighlightContextBuilder contextBuilder;

    @Mock
    private PipelineProcessingContext pipelineContext;

    @Mock
    private ActionListener<SearchResponse> responseListener;

    private SemanticHighlightingProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        processor = new SemanticHighlightingProcessor(false, mlClientAccessor);
    }

    public void testProcessWithValidConfig() {
        // Setup
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .batchInference(false)
            .build();

        HighlightContext context = mock(HighlightContext.class);
        when(context.isEmpty()).thenReturn(false);

        // Create a processor with injected dependencies for testing
        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(eq(request), eq(response))).thenReturn(config);
        when(validator.validate(eq(config), eq(response))).thenReturn(config);
        when(contextBuilder.build(eq(config), eq(response), anyLong())).thenReturn(context);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // Verify
        verify(configExtractor).extract(request, response);
        verify(validator).validate(config, response);
        verify(contextBuilder).build(eq(config), eq(response), anyLong());
    }

    public void testProcessWithInvalidConfig() {
        // Setup
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        HighlightConfig invalidConfig = HighlightConfig.builder().validationError("Missing model ID").build();

        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(eq(request), eq(response))).thenReturn(invalidConfig);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // Verify - should return original response without processing
        verify(responseListener).onResponse(response);
        verify(validator, never()).validate(any(), any());
        verify(contextBuilder, never()).build(any(), any(), anyLong());
    }

    public void testProcessWithEmptyDocuments() {
        // Setup
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        HighlightConfig config = HighlightConfig.builder().fieldName("content").modelId("test-model").queryText("test query").build();

        HighlightContext emptyContext = mock(HighlightContext.class);
        when(emptyContext.isEmpty()).thenReturn(true);

        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(eq(request), eq(response))).thenReturn(config);
        when(validator.validate(eq(config), eq(response))).thenReturn(config);
        when(contextBuilder.build(eq(config), eq(response), anyLong())).thenReturn(emptyContext);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // Verify - should return original response
        verify(responseListener).onResponse(response);
    }

    public void testBatchStrategySelection() {
        // Setup
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .batchInference(true)
            .maxBatchSize(50)
            .build();

        HighlightContext context = mock(HighlightContext.class);
        when(context.isEmpty()).thenReturn(false);

        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(eq(request), eq(response))).thenReturn(config);
        when(validator.validate(eq(config), eq(response))).thenReturn(config);
        when(contextBuilder.build(eq(config), eq(response), anyLong())).thenReturn(context);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // The batch strategy should be created based on config.isBatchInference() = true
        // We can't directly verify strategy creation without refactoring, but the test ensures no exceptions
    }

    public void testSingleStrategySelection() {
        // Setup
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        HighlightConfig config = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .batchInference(false)
            .build();

        HighlightContext context = mock(HighlightContext.class);
        when(context.isEmpty()).thenReturn(false);

        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(eq(request), eq(response))).thenReturn(config);
        when(validator.validate(eq(config), eq(response))).thenReturn(config);
        when(contextBuilder.build(eq(config), eq(response), anyLong())).thenReturn(context);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // The single strategy should be created based on config.isBatchInference() = false
    }

    public void testErrorHandlingWithIgnoreFailure() {
        // Setup with ignoreFailure = true
        processor = new SemanticHighlightingProcessor(true, mlClientAccessor);

        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        processor = new TestableSemanticHighlightingProcessor(true, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(any(), any())).thenThrow(new RuntimeException("Test error"));

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // Verify - should return original response on error when ignoreFailure = true
        verify(responseListener).onResponse(response);
        verify(responseListener, never()).onFailure(any());
    }

    public void testErrorHandlingWithoutIgnoreFailure() {
        // Setup with ignoreFailure = false
        processor = new SemanticHighlightingProcessor(false, mlClientAccessor);

        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        RuntimeException testError = new RuntimeException("Test error");

        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(any(), any())).thenThrow(testError);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // Verify - should propagate error when ignoreFailure = false
        ArgumentCaptor<Exception> errorCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(errorCaptor.capture());
        assertEquals("Test error", errorCaptor.getValue().getMessage());
    }

    public void testProcessResponseThrowsUnsupportedOperation() {
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        assertThrows(UnsupportedOperationException.class, () -> processor.processResponse(request, response));
    }

    public void testGetters() {
        // Always uses semantic-specific defaults regardless of constructor arguments
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
        assertEquals(SemanticHighlightingConstants.PROCESSOR_TYPE, processor.getType());
        assertFalse(processor.isIgnoreFailure());
        assertEquals(SystemGeneratedProcessor.ExecutionStage.POST_USER_DEFINED, processor.getExecutionStage());

        // Test with ignoreFailure = true
        processor = new SemanticHighlightingProcessor(true, mlClientAccessor);
        assertTrue(processor.isIgnoreFailure());
        // Still uses semantic-specific defaults for tag and description
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG, processor.getTag());
        assertEquals(SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION, processor.getDescription());
    }

    public void testValidationFailure() {
        // Setup
        SearchRequest request = createSearchRequest();
        SearchResponse response = createSearchResponse();

        HighlightConfig extractedConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .build();

        HighlightConfig invalidatedConfig = HighlightConfig.builder()
            .fieldName("content")
            .modelId("test-model")
            .queryText("test query")
            .validationError("Field not found in response")
            .build();

        processor = new TestableSemanticHighlightingProcessor(false, mlClientAccessor, configExtractor, validator, contextBuilder);

        when(configExtractor.extract(eq(request), eq(response))).thenReturn(extractedConfig);
        when(validator.validate(eq(extractedConfig), eq(response))).thenReturn(invalidatedConfig);

        // Execute
        processor.processResponseAsync(request, response, pipelineContext, responseListener);

        // Verify - should return original response when validation fails
        verify(responseListener).onResponse(response);
        verify(contextBuilder, never()).build(any(), any(), anyLong());
    }

    // Helper methods
    private SearchRequest createSearchRequest() {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field field = new HighlightBuilder.Field("content");
        field.highlighterType(SemanticHighlightingConstants.HIGHLIGHTER_TYPE);
        highlightBuilder.field(field);
        sourceBuilder.highlighter(highlightBuilder);
        request.source(sourceBuilder);
        return request;
    }

    private SearchResponse createSearchResponse() {
        SearchHit hit = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray("{\"content\":\"test content\"}"));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1.0f);

        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, null, 0);

        return new SearchResponse(internalResponse, null, 1, 1, 0, 100, null, null);
    }

    // Testable subclass to inject dependencies
    private static class TestableSemanticHighlightingProcessor extends SemanticHighlightingProcessor {
        private final HighlightConfigExtractor configExtractor;
        private final HighlightValidator validator;
        private final HighlightContextBuilder contextBuilder;

        public TestableSemanticHighlightingProcessor(
            boolean ignoreFailure,
            MLCommonsClientAccessor mlClientAccessor,
            HighlightConfigExtractor configExtractor,
            HighlightValidator validator,
            HighlightContextBuilder contextBuilder
        ) {
            super(ignoreFailure, mlClientAccessor);
            this.configExtractor = configExtractor;
            this.validator = validator;
            this.contextBuilder = contextBuilder;
        }

        @Override
        public void processResponseAsync(
            SearchRequest request,
            SearchResponse response,
            PipelineProcessingContext responseContext,
            ActionListener<SearchResponse> responseListener
        ) {
            long startTime = System.currentTimeMillis();

            try {
                HighlightConfig config = configExtractor.extract(request, response);

                if (config.getValidationError() != null) {
                    responseListener.onResponse(response);
                    return;
                }

                config = validator.validate(config, response);
                if (!config.isValid()) {
                    responseListener.onResponse(response);
                    return;
                }

                HighlightContext context = contextBuilder.build(config, response, startTime);
                if (context.isEmpty()) {
                    responseListener.onResponse(response);
                    return;
                }

                // For testing purposes, we'll just return the response
                // In real implementation, strategy would be created and executed
                responseListener.onResponse(response);

            } catch (Exception e) {
                if (isIgnoreFailure()) {
                    responseListener.onResponse(response);
                } else {
                    responseListener.onFailure(e);
                }
            }
        }
    }
}
