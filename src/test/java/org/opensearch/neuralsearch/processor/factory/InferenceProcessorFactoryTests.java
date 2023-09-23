/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.factory;

import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.processor.InferenceProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.InferenceProcessor.MODEL_ID_FIELD;

import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;

import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.InferenceProcessor;
import org.opensearch.test.OpenSearchTestCase;

public class InferenceProcessorFactoryTests extends OpenSearchTestCase {

    private static final String NORMALIZATION_METHOD = "min_max";
    private static final String COMBINATION_METHOD = "arithmetic_mean";

    @SneakyThrows
    public void testNormalizationProcessor_whenNoParams_thenSuccessful() {
        InferenceProcessorFactory inferenceProcessorFactory = new InferenceProcessorFactory(
            mock(MLCommonsClientAccessor.class),
            mock(Environment.class)
        );

        final Map<String, org.opensearch.ingest.Processor.Factory> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, "1234567678");
        config.put(
            FIELD_MAP_FIELD,
            Map.of("passage_text", Map.of("model_input", "TextInput1", "model_output", "TextEmbdedding1", "embedding", "passage_embedding"))
        );
        InferenceProcessor inferenceProcessor = inferenceProcessorFactory.create(processorFactories, tag, description, config);
        assertNotNull(inferenceProcessor);
        assertEquals("inference-processor", inferenceProcessor.getType());
    }
}
