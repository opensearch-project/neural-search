/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;

import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class NormalizationProcessorFactoryTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testNormalizationProcessor_whenNoParams_thenSuccessful() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory();
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = normalizationProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof NormalizationProcessor);
        NormalizationProcessor normalizationProcessor = (NormalizationProcessor) searchPhaseResultsProcessor;
        assertEquals("normalization-processor", normalizationProcessor.getType());
    }

    @SneakyThrows
    public void testNormalizationProcessor_whenWithParams_thenSuccessful() {
        NormalizationProcessorFactory normalizationProcessorFactory = new NormalizationProcessorFactory();
        final Map<String, Processor.Factory<SearchPhaseResultsProcessor>> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        config.put("normalization", Map.of("technique", "MIN_MAX"));
        config.put("combination", Map.of("technique", "ARITHMETIC_MEAN"));
        Processor.PipelineContext pipelineContext = mock(Processor.PipelineContext.class);
        SearchPhaseResultsProcessor searchPhaseResultsProcessor = normalizationProcessorFactory.create(
            processorFactories,
            tag,
            description,
            ignoreFailure,
            config,
            pipelineContext
        );
        assertNotNull(searchPhaseResultsProcessor);
        assertTrue(searchPhaseResultsProcessor instanceof NormalizationProcessor);
        NormalizationProcessor normalizationProcessor = (NormalizationProcessor) searchPhaseResultsProcessor;
        assertEquals("normalization-processor", normalizationProcessor.getType());
    }
}
