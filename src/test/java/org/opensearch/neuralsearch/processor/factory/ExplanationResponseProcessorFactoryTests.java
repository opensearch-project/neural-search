/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.processor.ExplanationResponseProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class ExplanationResponseProcessorFactoryTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testDefaults_whenNoParams_thenSuccessful() {
        // Setup
        ExplanationResponseProcessorFactory explanationResponseProcessorFactory = new ExplanationResponseProcessorFactory();
        final Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        // Act
        SearchResponseProcessor responseProcessor = explanationResponseProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        // Assert
        assertProcessor(responseProcessor, tag, description, ignoreFailure);
    }

    @SneakyThrows
    public void testInvalidInput_whenParamsPassedToFactory_thenSuccessful() {
        // Setup
        ExplanationResponseProcessorFactory explanationResponseProcessorFactory = new ExplanationResponseProcessorFactory();
        final Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        // create map of random parameters
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < randomInt(1_000); i++) {
            config.put(randomAlphaOfLength(10) + i, randomAlphaOfLength(100));
        }
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        // Act
        SearchResponseProcessor responseProcessor = explanationResponseProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        // Assert
        assertProcessor(responseProcessor, tag, description, ignoreFailure);
    }

    @SneakyThrows
    public void testNewInstanceCreation_whenCreateMultipleTimes_thenNewInstanceReturned() {
        // Setup
        ExplanationResponseProcessorFactory explanationResponseProcessorFactory = new ExplanationResponseProcessorFactory();
        final Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        // Act
        SearchResponseProcessor responseProcessorOne = explanationResponseProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );

        SearchResponseProcessor responseProcessorTwo = explanationResponseProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );

        // Assert
        assertNotEquals(responseProcessorOne, responseProcessorTwo);
    }

    private static void assertProcessor(SearchResponseProcessor responseProcessor, String tag, String description, boolean ignoreFailure) {
        assertNotNull(responseProcessor);
        assertTrue(responseProcessor instanceof ExplanationResponseProcessor);
        ExplanationResponseProcessor explanationResponseProcessor = (ExplanationResponseProcessor) responseProcessor;
        assertEquals("explanation_response_processor", explanationResponseProcessor.getType());
        assertEquals(tag, explanationResponseProcessor.getTag());
        assertEquals(description, explanationResponseProcessor.getDescription());
        assertEquals(ignoreFailure, explanationResponseProcessor.isIgnoreFailure());
    }
}
