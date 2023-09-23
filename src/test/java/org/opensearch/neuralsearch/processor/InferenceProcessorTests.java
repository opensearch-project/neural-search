/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.InferenceProcessorFactory;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class InferenceProcessorTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment env;

    @InjectMocks
    private InferenceProcessorFactory inferenceProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(env.settings()).thenReturn(settings);
    }

    @SneakyThrows
    private InferenceProcessor createInstance(List<List<Float>> vector) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                "key1",
                Map.of("model_input", "ModelInput1", "model_output", "ModelOutput1", "embedding", "key1Mapped"),
                "key2",
                Map.of("model_input", "ModelInput2", "model_output", "ModelOutput2", "embedding", "key2Mapped")
            )
        );
        return inferenceProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapEmpty_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        try {
            inferenceProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        } catch (OpenSearchParseException e) {
            assertEquals("[field_map] required property is missing", e.getMessage());
        }
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        InferenceProcessor processor = createInstance(createMockVectorWithLength(2));

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceMultimodal(anyString(), anyMap(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    private List<List<Float>> createMockVectorResult() {
        List<List<Float>> modelTensorList = new ArrayList<>();
        List<Float> number1 = ImmutableList.of(1.234f, 2.354f);
        List<Float> number2 = ImmutableList.of(3.234f, 4.354f);
        List<Float> number3 = ImmutableList.of(5.234f, 6.354f);
        List<Float> number4 = ImmutableList.of(7.234f, 8.354f);
        List<Float> number5 = ImmutableList.of(9.234f, 10.354f);
        List<Float> number6 = ImmutableList.of(11.234f, 12.354f);
        List<Float> number7 = ImmutableList.of(13.234f, 14.354f);
        modelTensorList.add(number1);
        modelTensorList.add(number2);
        modelTensorList.add(number3);
        modelTensorList.add(number4);
        modelTensorList.add(number5);
        modelTensorList.add(number6);
        modelTensorList.add(number7);
        return modelTensorList;
    }

    private List<List<Float>> createMockVectorWithLength(int size) {
        float suffix = .234f;
        List<List<Float>> result = new ArrayList<>();
        for (int i = 0; i < size * 2;) {
            List<Float> number = new ArrayList<>();
            number.add(i++ + suffix);
            number.add(i++ + suffix);
            result.add(number);
        }
        return result;
    }
}
