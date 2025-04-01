/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.EMBEDDING_FIELD;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.IMAGE_FIELD_NAME;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.SKIP_EXISTING;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.TEXT_FIELD_NAME;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;
import org.opensearch.transport.client.OpenSearchClient;

public class TextImageEmbeddingProcessorFactoryTests extends OpenSearchTestCase {

    @SneakyThrows
    public void testTextImageEmbeddingProcessor_whenAllParamsPassed_thenSuccessful() {
        TextImageEmbeddingProcessorFactory textImageEmbeddingProcessorFactory = new TextImageEmbeddingProcessorFactory(
            mock(OpenSearchClient.class),
            mock(MLCommonsClientAccessor.class),
            mock(Environment.class),
            mock(ClusterService.class)
        );

        final Map<String, org.opensearch.ingest.Processor.Factory> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> config = new HashMap<>();
        config.put(MODEL_ID_FIELD, "1234567678");
        config.put(EMBEDDING_FIELD, "embedding_field");
        config.put(FIELD_MAP_FIELD, Map.of(TEXT_FIELD_NAME, "my_text_field", IMAGE_FIELD_NAME, "my_image_field"));
        config.put(SKIP_EXISTING, true);
        TextImageEmbeddingProcessor inferenceProcessor = (TextImageEmbeddingProcessor) textImageEmbeddingProcessorFactory.create(
            processorFactories,
            tag,
            description,
            config
        );
        assertNotNull(inferenceProcessor);
        assertEquals("text_image_embedding", inferenceProcessor.getType());
    }

    @SneakyThrows
    public void testTextImageEmbeddingProcessor_whenOnlyOneParamSet_thenSuccessful() {
        TextImageEmbeddingProcessorFactory textImageEmbeddingProcessorFactory = new TextImageEmbeddingProcessorFactory(
            mock(OpenSearchClient.class),
            mock(MLCommonsClientAccessor.class),
            mock(Environment.class),
            mock(ClusterService.class)
        );

        final Map<String, org.opensearch.ingest.Processor.Factory> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> configOnlyTextField = new HashMap<>();
        configOnlyTextField.put(MODEL_ID_FIELD, "1234567678");
        configOnlyTextField.put(EMBEDDING_FIELD, "embedding_field");
        configOnlyTextField.put(FIELD_MAP_FIELD, Map.of(TEXT_FIELD_NAME, "my_text_field"));
        TextImageEmbeddingProcessor processor = (TextImageEmbeddingProcessor) textImageEmbeddingProcessorFactory.create(
            processorFactories,
            tag,
            description,
            configOnlyTextField
        );
        assertNotNull(processor);
        assertEquals("text_image_embedding", processor.getType());

        Map<String, Object> configOnlyImageField = new HashMap<>();
        configOnlyImageField.put(MODEL_ID_FIELD, "1234567678");
        configOnlyImageField.put(EMBEDDING_FIELD, "embedding_field");
        configOnlyImageField.put(FIELD_MAP_FIELD, Map.of(TEXT_FIELD_NAME, "my_text_field"));
        processor = (TextImageEmbeddingProcessor) textImageEmbeddingProcessorFactory.create(
            processorFactories,
            tag,
            description,
            configOnlyImageField
        );
        assertNotNull(processor);
        assertEquals("text_image_embedding", processor.getType());
    }

    @SneakyThrows
    public void testTextImageEmbeddingProcessor_whenMixOfParamsOrEmptyParams_thenFail() {
        TextImageEmbeddingProcessorFactory textImageEmbeddingProcessorFactory = new TextImageEmbeddingProcessorFactory(
            mock(OpenSearchClient.class),
            mock(MLCommonsClientAccessor.class),
            mock(Environment.class),
            mock(ClusterService.class)
        );

        final Map<String, org.opensearch.ingest.Processor.Factory> processorFactories = new HashMap<>();
        String tag = "tag";
        String description = "description";
        boolean ignoreFailure = false;
        Map<String, Object> configMixOfFields = new HashMap<>();
        configMixOfFields.put(MODEL_ID_FIELD, "1234567678");
        configMixOfFields.put(EMBEDDING_FIELD, "embedding_field");
        configMixOfFields.put(FIELD_MAP_FIELD, Map.of(TEXT_FIELD_NAME, "my_text_field", "random_field_name", "random_field"));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> textImageEmbeddingProcessorFactory.create(processorFactories, tag, description, configMixOfFields)
        );
        org.hamcrest.MatcherAssert.assertThat(
            exception.getMessage(),
            allOf(
                containsString(
                    "Unable to create the TextImageEmbedding processor with provided field name(s). Following names are supported ["
                ),
                containsString("image"),
                containsString("text")
            )
        );
        Map<String, Object> configNoFields = new HashMap<>();
        configNoFields.put(MODEL_ID_FIELD, "1234567678");
        configNoFields.put(EMBEDDING_FIELD, "embedding_field");
        configNoFields.put(FIELD_MAP_FIELD, Map.of());
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> textImageEmbeddingProcessorFactory.create(processorFactories, tag, description, configNoFields)
        );
        assertEquals(exception.getMessage(), "Unable to create the TextImageEmbedding processor as field_map has invalid key or value");
    }
}
