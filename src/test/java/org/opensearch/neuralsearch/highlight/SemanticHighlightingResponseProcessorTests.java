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
import org.opensearch.core.action.ActionListener;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

public class SemanticHighlightingResponseProcessorTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    @Mock
    private SearchRequest searchRequest;

    @Mock
    private SearchResponse searchResponse;

    @Mock
    private PipelineProcessingContext pipelineProcessingContext;

    @Mock
    private ActionListener<SearchResponse> actionListener;

    private SemanticHighlightingResponseProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        processor = new SemanticHighlightingResponseProcessor(
            "test-tag",
            "test-description",
            false,
            "test-model-id",
            mlClientAccessor,
            false,
            "<em>",
            "</em>"
        );
    }

    public void testGetType() {
        assertEquals(SemanticHighlightingConstants.PROCESSOR_TYPE, processor.getType());
    }

    public void testGetTag() {
        assertEquals("test-tag", processor.getTag());
    }

    public void testGetDescription() {
        assertEquals("test-description", processor.getDescription());
    }

    public void testIsIgnoreFailure() {
        assertFalse(processor.isIgnoreFailure());
    }

    public void testProcessResponseThrowsUnsupportedOperationException() {
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> processor.processResponse(searchRequest, searchResponse)
        );

        assertEquals(
            String.format(Locale.ROOT, "%s processor requires async processing", SemanticHighlightingConstants.PROCESSOR_TYPE),
            exception.getMessage()
        );
    }

    public void testConstructorWithIgnoreFailureTrue() {
        SemanticHighlightingResponseProcessor processorWithIgnoreFailure = new SemanticHighlightingResponseProcessor(
            "test-tag",
            "test-description",
            true,
            "test-model-id",
            mlClientAccessor,
            true,
            "<mark>",
            "</mark>"
        );

        assertTrue(processorWithIgnoreFailure.isIgnoreFailure());
    }
}
