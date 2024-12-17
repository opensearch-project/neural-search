/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.isA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.AbstractBatchingProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class TextEmbeddingProcessorTests extends InferenceProcessorTestCase {

    protected static final String PARENT_FIELD = "parent";
    protected static final String CHILD_FIELD_LEVEL_1 = "child_level1";
    protected static final String CHILD_FIELD_LEVEL_2 = "child_level2";
    protected static final String CHILD_LEVEL_2_TEXT_FIELD_VALUE = "text_field_value";
    protected static final String CHILD_LEVEL_2_KNN_FIELD = "test3_knn";
    protected static final String CHILD_1_TEXT_FIELD = "child_1_text_field";
    protected static final String TEXT_VALUE_1 = "text_value";
    protected static final String TEXT_FIELD_2 = "abc";
    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment environment;

    private ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

    @InjectMocks
    private TextEmbeddingProcessorFactory textEmbeddingProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(clusterService.state().metadata().index(anyString()).getSettings()).thenReturn(settings);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel2MapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of("key1", ImmutableMap.of("test1", "test1_knn"), "key2", ImmutableMap.of("test3", CHILD_LEVEL_2_KNN_FIELD))
        );
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel1MapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1_knn", "key2", "key2_knn"));
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel1MapConfig(int batchSize) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1_knn", "key2", "key2_knn"));
        config.put(AbstractBatchingProcessor.BATCH_SIZE_FIELD, batchSize);
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapError_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put(null, "key1Mapped");
        fieldMap.put("key2", "key2Mapped");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        try {
            textEmbeddingProcessorFactory.create(registry, TextEmbeddingProcessor.TYPE, DESCRIPTION, config);
        } catch (IllegalArgumentException e) {
            assertEquals("Unable to create the processor as field_map has invalid key or value", e.getMessage());
        }
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapEmpty_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        try {
            textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        } catch (OpenSearchParseException e) {
            assertEquals("[field_map] required property is missing", e.getMessage());
        }
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    @SneakyThrows
    public void testExecute_whenInferenceThrowInterruptedException_throwRuntimeException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            accessor,
            environment,
            clusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        TextEmbeddingProcessor processor = (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        doThrow(new RuntimeException()).when(accessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(RuntimeException.class));
    }

    @SneakyThrows
    public void testExecute_whenInferenceTextListEmpty_SuccessWithoutEmbedding() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            accessor,
            environment,
            clusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        TextEmbeddingProcessor processor = (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        doThrow(new RuntimeException()).when(accessor)
            .inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_withListTypeInput_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", list1);
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_SimpleTypeWithEmptyStringValue_throwIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "    ");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasEmptyStringValue_throwIllegalArgumentException() {
        List<String> list1 = ImmutableList.of("", "test2", "test3");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", list1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasNonStringValue_throwIllegalArgumentException() {
        List<Integer> list2 = ImmutableList.of(1, 2, 3);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasNull_throwIllegalArgumentException() {
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add(null);
        list.add("world");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key2", list);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_withMapTypeInput_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test3", "test4");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());

    }

    @SneakyThrows
    public void testNestedFieldInMapping_withMapTypeInput_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        Map<String, String> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        sourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_FIELD_LEVEL_2)),
                CHILD_LEVEL_2_KNN_FIELD
            )
        );
        TextEmbeddingProcessor processor = (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        assertNotNull(ingestDocument);
        assertNotNull(ingestDocument.getSourceAndMetadata().get(PARENT_FIELD));
        Map<String, Object> childLevel1AfterProcessor = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map<String, Object> childLevel2AfterProcessor = (Map<String, Object>) childLevel1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(CHILD_LEVEL_2_TEXT_FIELD_VALUE, childLevel2AfterProcessor.get(CHILD_FIELD_LEVEL_2));
        assertNotNull(childLevel2AfterProcessor.get(CHILD_LEVEL_2_KNN_FIELD));
        List<Float> vectors = (List<Float>) childLevel2AfterProcessor.get(CHILD_LEVEL_2_KNN_FIELD);
        assertEquals(100, vectors.size());
        for (Float vector : vectors) {
            assertTrue(vector >= 0.0f && vector <= 1.0f);
        }
    }

    @SneakyThrows
    public void testNestedFieldInMappingForSourceAndDestination_withIngestDocumentHasTheDestinationStructure_theSuccessful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        /*
        modeling following document:
          parent:
           child_level_1:
               child_level_1_text_field: "text"
               child_level_2:
                   child_level_2_text_field: "abc"
         */
        Map<String, String> childLevel2NestedField = new HashMap<>();
        childLevel2NestedField.put(CHILD_LEVEL_2_TEXT_FIELD_VALUE, TEXT_FIELD_2);
        Map<String, Object> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, childLevel2NestedField);
        childLevel2.put(CHILD_1_TEXT_FIELD, TEXT_VALUE_1);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        sourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_1_TEXT_FIELD)),
                CHILD_FIELD_LEVEL_2 + "." + CHILD_LEVEL_2_KNN_FIELD
            )
        );
        TextEmbeddingProcessor processor = (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        assertNotNull(ingestDocument);
        assertNotNull(ingestDocument.getSourceAndMetadata().get(PARENT_FIELD));
        Map<String, Object> parent1AfterProcessor = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map<String, Object> childLevel1Actual = (Map<String, Object>) parent1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(2, childLevel1Actual.size());
        assertEquals(TEXT_VALUE_1, childLevel1Actual.get(CHILD_1_TEXT_FIELD));
        Map<String, Object> child2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
        assertEquals(2, child2Actual.size());
        assertEquals(TEXT_FIELD_2, child2Actual.get(CHILD_LEVEL_2_TEXT_FIELD_VALUE));
        List<Float> vectors = (List<Float>) child2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
        assertEquals(100, vectors.size());
        for (Float vector : vectors) {
            assertTrue(vector >= 0.0f && vector <= 1.0f);
        }
    }

    @SneakyThrows
    public void testNestedFieldInMappingForSourceAndDestination_withIngestDocumentWithoutDestinationStructure_theSuccessful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        /*
        modeling following document:
          parent:
           child_level_1:
               child_level_1_text_field: "text"
        */
        Map<String, Object> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_1_TEXT_FIELD, TEXT_VALUE_1);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        sourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_1_TEXT_FIELD)),
                CHILD_FIELD_LEVEL_2 + "." + CHILD_LEVEL_2_KNN_FIELD
            )
        );
        TextEmbeddingProcessor processor = (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        assertNotNull(ingestDocument);
        assertNotNull(ingestDocument.getSourceAndMetadata().get(PARENT_FIELD));
        Map<String, Object> parent1AfterProcessor = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map<String, Object> childLevel1Actual = (Map<String, Object>) parent1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(2, childLevel1Actual.size());
        assertEquals(TEXT_VALUE_1, childLevel1Actual.get(CHILD_1_TEXT_FIELD));
        Map<String, Object> child2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
        assertEquals(1, child2Actual.size());
        List<Float> vectors = (List<Float>) child2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
        assertEquals(100, vectors.size());
        for (Float vector : vectors) {
            assertTrue(vector >= 0.0f && vector <= 1.0f);
        }
    }

    @SneakyThrows
    public void testNestedFieldInMappingMixedSyntax_withMapTypeInput_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        Map<String, String> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        sourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1)),
                Map.of(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_KNN_FIELD)
            )
        );
        TextEmbeddingProcessor processor = (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        assertNotNull(ingestDocument);
        assertNotNull(ingestDocument.getSourceAndMetadata().get(PARENT_FIELD));
        Map<String, Object> childLevel1AfterProcessor = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map<String, Object> childLevel2AfterProcessor = (Map<String, Object>) childLevel1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(CHILD_LEVEL_2_TEXT_FIELD_VALUE, childLevel2AfterProcessor.get(CHILD_FIELD_LEVEL_2));
        assertNotNull(childLevel2AfterProcessor.get(CHILD_LEVEL_2_KNN_FIELD));
        List<Float> vectors = (List<Float>) childLevel2AfterProcessor.get(CHILD_LEVEL_2_KNN_FIELD);
        assertEquals(100, vectors.size());
        for (Float vector : vectors) {
            assertTrue(vector >= 0.0f && vector <= 1.0f);
        }
    }

    public void testExecute_mapHasNonStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, Double> map2 = ImmutableMap.of("test3", 209.3D);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapHasEmptyStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, String> map2 = ImmutableMap.of("test3", "   ");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapDepthReachLimit_throwIllegalArgumentException() {
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> 1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "hello world");
        sourceAndMetadata.put("key2", ret);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_MLClientAccessorThrowFail_handlerFailure() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentences(
                argThat(request -> request.getMlAlgoParams() != null && request.getInputTexts() != null),
                isA(ActionListener.class)
            );

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    private Map<String, Object> createMaxDepthLimitExceedMap(Supplier<Integer> maxDepthSupplier) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > 21) {
            return null;
        }
        Map<String, Object> innerMap = new HashMap<>();
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> maxDepth + 1);
        if (ret == null) return innerMap;
        innerMap.put("hello", ret);
        return innerMap;
    }

    public void testExecute_hybridTypeInput_successful() throws Exception {
        List<String> list1 = ImmutableList.of("test1", "test2");
        Map<String, List<String>> map1 = ImmutableMap.of("test3", list1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", map1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key2");
    }

    public void testExecute_simpleTypeInputWithNonStringValue_handleIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", 100);
        sourceAndMetadata.put("key2", 100.232D);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testGetType_successful() {
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        assert processor.getType().equals(TextEmbeddingProcessor.TYPE);
    }

    public void testProcessResponse_successful() throws Exception {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildMapWithTargetKeys(ingestDocument);

        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
    }

    @SneakyThrows
    public void testBuildVectorOutput_withPlainStringValue_successful() {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildMapWithTargetKeys(ingestDocument);

        // To assert the order is not changed between config map and generated map.
        List<Object> configValueList = new LinkedList<>(config.values());
        List<String> knnKeyList = new LinkedList<>(knnMap.keySet());
        assertEquals(configValueList.size(), knnKeyList.size());
        assertEquals(knnKeyList.get(0), configValueList.get(0).toString());
        int lastIndex = knnKeyList.size() - 1;
        assertEquals(knnKeyList.get(lastIndex), configValueList.get(lastIndex).toString());

        List<List<Float>> modelTensorList = createMockVectorResult();
        Map<String, Object> result = processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        assertTrue(result.containsKey("oriKey1_knn"));
        assertTrue(result.containsKey("oriKey2_knn"));
        assertTrue(result.containsKey("oriKey3_knn"));
        assertTrue(result.containsKey("oriKey4_knn"));
        assertTrue(result.containsKey("oriKey5_knn"));
        assertTrue(result.containsKey("oriKey6_knn"));
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withNestedMap_successful() {
        Map<String, Object> config = createNestedMapConfiguration();
        IngestDocument ingestDocument = createNestedMapIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = processor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 100, 0.0f, 1.0f);
        processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        /**
         * "favorites": {
         *      "favorite": {
         *          "movie": "matrix",
         *          "actor": "Charlie Chaplin",
         *          "games" : {
         *              "adventure": {
         *                  "action": "overwatch",
         *                  "rpg": "elden ring"
         *              }
         *          }
         *      }
         * }
         */
        Map<String, Object> favoritesMap = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("favorites");
        assertNotNull(favoritesMap);
        Map<String, Object> favorites = (Map<String, Object>) favoritesMap.get("favorite");
        assertNotNull(favorites);

        Map<String, Object> favoriteGames = (Map<String, Object>) favorites.get("games");
        assertNotNull(favoriteGames);
        Map<String, Object> adventure = (Map<String, Object>) favoriteGames.get("adventure");
        List<Float> adventureKnnVector = (List<Float>) adventure.get("with_action_knn");
        assertNotNull(adventureKnnVector);
        assertEquals(100, adventureKnnVector.size());
        for (float vector : adventureKnnVector) {
            assertTrue(vector >= 0.0f && vector <= 1.0f);
        }

        List<Float> favoriteKnnVector = (List<Float>) favorites.get("favorite_movie_knn");
        assertNotNull(favoriteKnnVector);
        assertEquals(100, favoriteKnnVector.size());
        for (float vector : favoriteKnnVector) {
            assertTrue(vector >= 0.0f && vector <= 1.0f);
        }
    }

    public void testBuildVectorOutput_withNestedList_successful() {
        Map<String, Object> config = createNestedListConfiguration();
        IngestDocument ingestDocument = createNestedListIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 2, 0.0f, 1.0f);
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("nestedField");
        assertTrue(nestedObj.get(0).containsKey("vectorField"));
        assertTrue(nestedObj.get(1).containsKey("vectorField"));
        assertNotNull(nestedObj.get(0).get("vectorField"));
        assertNotNull(nestedObj.get(1).get("vectorField"));
    }

    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withNestedListHasNotForEmbeddingField_successful() {
        Map<String, Object> config = createNestedListConfiguration();
        IngestDocument ingestDocument = createNestedListWithNotEmbeddingFieldIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 2, 0.0f, 1.0f);
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("nestedField");
        assertFalse(nestedObj.get(0).containsKey("vectorField"));
        assertTrue(nestedObj.get(0).containsKey("textFieldNotForEmbedding"));
        assertTrue(nestedObj.get(1).containsKey("vectorField"));
        assertNotNull(nestedObj.get(1).get("vectorField"));
    }

    public void testBuildVectorOutput_withNestedList_Level2_successful() {
        Map<String, Object> config = createNestedList2LevelConfiguration();
        IngestDocument ingestDocument = create2LevelNestedListIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 2, 0.0f, 1.0f);
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        Map<String, Object> nestedLevel1 = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("nestedField");
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) nestedLevel1.get("nestedField");
        assertTrue(nestedObj.get(0).containsKey("vectorField"));
        assertTrue(nestedObj.get(1).containsKey("vectorField"));
        assertNotNull(nestedObj.get(0).get("vectorField"));
        assertNotNull(nestedObj.get(1).get("vectorField"));
    }

    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withNestedListHasNotForEmbeddingField_Level2_successful() {
        Map<String, Object> config = createNestedList2LevelConfiguration();
        IngestDocument ingestDocument = create2LevelNestedListWithNotEmbeddingFieldIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 2, 0.0f, 1.0f);
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        Map<String, Object> nestedLevel1 = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("nestedField");
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) nestedLevel1.get("nestedField");
        assertFalse(nestedObj.get(0).containsKey("vectorField"));
        assertTrue(nestedObj.get(0).containsKey("textFieldNotForEmbedding"));
        assertTrue(nestedObj.get(1).containsKey("vectorField"));
        assertNotNull(nestedObj.get(1).get("vectorField"));
    }

    public void test_updateDocument_appendVectorFieldsToDocument_successful() {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = processor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList);

        List<List<Float>> modelTensorList1 = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList1);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
        assertEquals(2, ((List<?>) ingestDocument.getSourceAndMetadata().get("oriKey6_knn")).size());
    }

    public void test_doublyNestedList_withMapType_successful() {
        Map<String, Object> config = createNestedListConfiguration();

        Map<String, Object> toEmbeddings = new HashMap<>();
        toEmbeddings.put("textField", "text to embedding");
        List<Map<String, Object>> l1List = new ArrayList<>();
        l1List.add(toEmbeddings);
        List<List<Map<String, Object>>> l2List = new ArrayList<>();
        l2List.add(l1List);
        Map<String, Object> document = new HashMap<>();
        document.put("nestedField", l2List);
        document.put(IndexFieldMapper.NAME, "my_index");

        IngestDocument ingestDocument = new IngestDocument(document, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        ArgumentCaptor<IllegalArgumentException> argumentCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(handler).accept(isNull(), argumentCaptor.capture());
        assertEquals("list type field [nestedField] is nested list type, cannot process it", argumentCaptor.getValue().getMessage());
    }

    public void test_batchExecute_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount);

        List<List<Float>> modelTensorList = createMockVectorWithLength(10);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertNull(resultCallback.getValue().get(i).getException());
        }
    }

    public void test_batchExecute_exception() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertNotNull(resultCallback.getValue().get(i).getException());
        }
    }

    public void testParsingNestedField_whenNestedFieldsConfigured_thenSuccessful() {
        Map<String, Object> config = createNestedMapConfiguration();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        /**
         * Assert that mapping
         * "favorites": {
         *      "favorite.movie": "favorite_movie_knn",
         *      "favorite.games": {
         *         "adventure.action": "with_action_knn"
         *     }
         * }
         * has been transformed to structure:
         * "favorites": {
         *      "favorite": {
         *          "movie": "favorite_movie_knn",
         *          "games": {
         *              "adventure": {
         *                  "action": "with_action_knn"
         *              }
         *          }
         *      }
         * }
         */
        assertMapWithNestedFields(
            processor.processNestedKey(
                config.entrySet().stream().filter(entry -> entry.getKey().equals("favorites")).findAny().orElseThrow()
            ),
            List.of("favorites"),
            Optional.empty()
        );

        Map<String, Object> favorites = (Map) config.get("favorites");

        assertMapWithNestedFields(
            processor.processNestedKey(
                favorites.entrySet().stream().filter(entry -> entry.getKey().equals("favorite.games")).findAny().orElseThrow()
            ),
            List.of("favorite", "games"),
            Optional.of("favorite_movie_knn")
        );

        assertMapWithNestedFields(
            processor.processNestedKey(
                favorites.entrySet().stream().filter(entry -> entry.getKey().equals("favorite.movie")).findAny().orElseThrow()
            ),
            List.of("favorite", "movie"),
            Optional.empty()
        );

        Map<String, Object> adventureActionMap = (Map<String, Object>) favorites.get("favorite.games");
        assertMapWithNestedFields(
            processor.processNestedKey(
                adventureActionMap.entrySet().stream().filter(entry -> entry.getKey().equals("adventure.action")).findAny().orElseThrow()
            ),
            List.of("adventure", "action"),
            Optional.of("with_action_knn")
        );
    }

    public void testBuildingOfNestedMap_whenHasNestedMapping_thenSuccessful() {
        /**
         * assert based on following structure:
         * "nestedField": {
         *     "nestedField": {
         *             "textField": "vectorField"
         *     }
         * }
         */
        Map<String, Object> config = createNestedList2LevelConfiguration();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> resultAsTree = new LinkedHashMap<>();
        processor.buildNestedMap("nestedField", config.get("nestedField"), config, resultAsTree);
        assertNotNull(resultAsTree);
        Map<String, Object> actualMapLevel1 = (Map<String, Object>) resultAsTree.get("nestedField");
        assertEquals(1, actualMapLevel1.size());
        assertEquals(Map.of("vectorField", "vectorField"), actualMapLevel1.get("nestedField"));
    }

    private void assertMapWithNestedFields(Pair<String, Object> actual, List<String> expectedKeys, Optional<Object> expectedFinalValue) {
        assertNotNull(actual);
        assertEquals(expectedKeys.get(0), actual.getKey());
        Map<String, Object> actualValue = (Map<String, Object>) actual.getValue();
        for (int i = 1; i < expectedKeys.size(); i++) {
            assertTrue(actualValue.containsKey(expectedKeys.get(i)));
            if (actualValue.get(expectedKeys.get(i)) instanceof Map) {
                actualValue = (Map<String, Object>) actualValue.get(expectedKeys.get(i));
            } else if (expectedFinalValue.isPresent()) {
                assertEquals(expectedFinalValue.get(), actualValue.get(expectedKeys.get(i)));
            } else {
                break;
            }
        }
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedMapConfiguration(Map<String, Object> fieldMap) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    private Map<String, Object> createPlainStringConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("oriKey1", "oriKey1_knn");
        config.put("oriKey2", "oriKey2_knn");
        config.put("oriKey3", "oriKey3_knn");
        config.put("oriKey4", "oriKey4_knn");
        config.put("oriKey5", "oriKey5_knn");
        config.put("oriKey6", "oriKey6_knn");
        return config;
    }

    /**
     * Create following mapping
     * "favorites": {
     *      "favorite.movie": "favorite_movie_knn",
     *      "favorite.games": {
     *         "adventure.action": "with_action_knn"
     *     }
     * }
     */
    private Map<String, Object> createNestedMapConfiguration() {
        Map<String, Object> adventureGames = new HashMap<>();
        adventureGames.put("adventure.action", "with_action_knn");
        Map<String, Object> favorite = new HashMap<>();
        favorite.put("favorite.movie", "favorite_movie_knn");
        favorite.put("favorite.games", adventureGames);
        Map<String, Object> result = new HashMap<>();
        result.put("favorites", favorite);
        return result;
    }

    private IngestDocument createPlainIngestDocument() {
        Map<String, Object> result = new HashMap<>();
        result.put("oriKey1", "oriValue1");
        result.put("oriKey2", "oriValue2");
        result.put("oriKey3", "oriValue3");
        result.put("oriKey4", "oriValue4");
        result.put("oriKey5", "oriValue5");
        result.put("oriKey6", ImmutableList.of("oriValue6", "oriValue7"));
        return new IngestDocument(result, new HashMap<>());
    }

    /**
     * Create following document
     * "favorites": {
     *      "favorite": {
     *          "movie": "matrix",
     *          "actor": "Charlie Chaplin",
     *          "games" : {
     *              "adventure": {
     *                  "action": "overwatch",
     *                  "rpg": "elden ring"
     *              }
     *          }
     *      }
     * }
     */
    private IngestDocument createNestedMapIngestDocument() {
        Map<String, Object> adventureGames = new HashMap<>();
        adventureGames.put("action", "overwatch");
        adventureGames.put("rpg", "elden ring");
        Map<String, Object> favGames = new HashMap<>();
        favGames.put("adventure", adventureGames);
        Map<String, Object> favorites = new HashMap<>();
        favorites.put("movie", "matrix");
        favorites.put("games", favGames);
        favorites.put("actor", "Charlie Chaplin");
        Map<String, Object> favorite = new HashMap<>();
        favorite.put("favorite", favorites);
        Map<String, Object> result = new HashMap<>();
        result.put("favorites", favorite);
        return new IngestDocument(result, new HashMap<>());
    }

    private Map<String, Object> createNestedListConfiguration() {
        Map<String, Object> nestedConfig = new HashMap<>();
        nestedConfig.put("textField", "vectorField");
        Map<String, Object> result = new HashMap<>();
        result.put("nestedField", nestedConfig);
        return result;
    }

    private Map<String, Object> createNestedList2LevelConfiguration() {
        Map<String, Object> nestedConfig = new HashMap<>();
        nestedConfig.put("textField", "vectorField");
        Map<String, Object> nestConfigLevel1 = new HashMap<>();
        nestConfigLevel1.put("nestedField", nestedConfig);
        Map<String, Object> result = new HashMap<>();
        result.put("nestedField", nestConfigLevel1);
        return result;
    }

    private IngestDocument createNestedListIngestDocument() {
        HashMap<String, Object> nestedObj1 = new HashMap<>();
        nestedObj1.put("textField", "This is a text field");
        HashMap<String, Object> nestedObj2 = new HashMap<>();
        nestedObj2.put("textField", "This is another text field");
        HashMap<String, Object> nestedList = new HashMap<>();
        nestedList.put("nestedField", Arrays.asList(nestedObj1, nestedObj2));
        return new IngestDocument(nestedList, new HashMap<>());
    }

    private IngestDocument createNestedListWithNotEmbeddingFieldIngestDocument() {
        HashMap<String, Object> nestedObj1 = new HashMap<>();
        nestedObj1.put("textFieldNotForEmbedding", "This is a text field");
        HashMap<String, Object> nestedObj2 = new HashMap<>();
        nestedObj2.put("textField", "This is another text field");
        HashMap<String, Object> nestedList = new HashMap<>();
        nestedList.put("nestedField", Arrays.asList(nestedObj1, nestedObj2));
        return new IngestDocument(nestedList, new HashMap<>());
    }

    private IngestDocument create2LevelNestedListIngestDocument() {
        HashMap<String, Object> nestedObj1 = new HashMap<>();
        nestedObj1.put("textField", "This is a text field");
        HashMap<String, Object> nestedObj2 = new HashMap<>();
        nestedObj2.put("textField", "This is another text field");
        HashMap<String, Object> nestedList = new HashMap<>();
        nestedList.put("nestedField", Arrays.asList(nestedObj1, nestedObj2));
        HashMap<String, Object> nestedList1 = new HashMap<>();
        nestedList1.put("nestedField", nestedList);
        return new IngestDocument(nestedList1, new HashMap<>());
    }

    private IngestDocument create2LevelNestedListWithNotEmbeddingFieldIngestDocument() {
        HashMap<String, Object> nestedObj1 = new HashMap<>();
        nestedObj1.put("textFieldNotForEmbedding", "This is a text field");
        HashMap<String, Object> nestedObj2 = new HashMap<>();
        nestedObj2.put("textField", "This is another text field");
        HashMap<String, Object> nestedList = new HashMap<>();
        nestedList.put("nestedField", Arrays.asList(nestedObj1, nestedObj2));
        HashMap<String, Object> nestedList1 = new HashMap<>();
        nestedList1.put("nestedField", nestedList);
        return new IngestDocument(nestedList1, new HashMap<>());
    }
}
