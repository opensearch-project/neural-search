/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.highlight.processor.SemanticHighlightingResponseProcessor;
import org.opensearch.neuralsearch.highlight.processor.SemanticHighlightingResponseProcessorFactory;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SemanticHighlightingResponseProcessorFactoryTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlClientAccessor;

    @Mock
    private Processor.PipelineContext pipelineContext;

    private SemanticHighlightingResponseProcessorFactory factory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        factory = new SemanticHighlightingResponseProcessorFactory(mlClientAccessor);
    }

    public void testCreateWithModelId() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put(SemanticHighlightingConstants.MODEL_ID, "test-model-id");

        SemanticHighlightingResponseProcessor processor = factory.create(
            new HashMap<>(),
            "test-tag",
            "test-description",
            false,
            config,
            pipelineContext
        );

        assertNotNull(processor);
        assertEquals("test-tag", processor.getTag());
        assertEquals("test-description", processor.getDescription());
        assertFalse(processor.isIgnoreFailure());
    }

    public void testCreateWithAllConfig() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put(SemanticHighlightingConstants.MODEL_ID, "test-model-id");
        config.put(SemanticHighlightingConstants.BATCH_INFERENCE, true);
        config.put(SemanticHighlightingConstants.PRE_TAG, "<mark>");
        config.put(SemanticHighlightingConstants.POST_TAG, "</mark>");

        SemanticHighlightingResponseProcessor processor = factory.create(
            new HashMap<>(),
            "test-tag",
            "test-description",
            true,
            config,
            pipelineContext
        );

        assertNotNull(processor);
        assertTrue(processor.isIgnoreFailure());
    }

    public void testCreateWithoutModelIdSucceeds() throws IOException {
        Map<String, Object> config = new HashMap<>();

        SemanticHighlightingResponseProcessor processor = factory.create(
            new HashMap<>(),
            "test-tag",
            "test-description",
            false,
            config,
            pipelineContext
        );

        assertNotNull(processor);
        assertEquals("test-tag", processor.getTag());
        assertEquals("test-description", processor.getDescription());
    }

    public void testCreateWithDefaultValues() throws IOException {
        Map<String, Object> config = new HashMap<>();

        SemanticHighlightingResponseProcessor processor = factory.create(
            new HashMap<>(),
            "test-tag",
            "test-description",
            false,
            config,
            pipelineContext
        );

        assertNotNull(processor);
        // Verify defaults are applied (we can't directly test private fields, but processor creation succeeds)
    }
}
