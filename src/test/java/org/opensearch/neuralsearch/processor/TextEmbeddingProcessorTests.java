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
import static org.mockito.Mockito.times;
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
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
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
import org.opensearch.transport.client.OpenSearchClient;

public class TextEmbeddingProcessorTests extends InferenceProcessorTestCase {

    protected static final String PARENT_FIELD = "parent";
    protected static final String CHILD_FIELD_LEVEL_1 = "child_level1";
    protected static final String CHILD_FIELD_LEVEL_2 = "child_level2";
    protected static final String CHILD_LEVEL_2_TEXT_FIELD_VALUE = "text_field_value";
    protected static final String CHILD_LEVEL_2_KNN_FIELD = "test3_knn";
    protected static final String CHILD_1_TEXT_FIELD = "child_1_text_field";
    protected static final String CHILD_2_TEXT_FIELD = "child_2_text_field";
    protected static final String CHILD_3_TEXT_FIELD = "child_3_text_field";
    protected static final String TEXT_VALUE_1 = "text_value";
    protected static final String TEXT_VALUE_2 = "text_value2";
    protected static final String TEXT_VALUE_3 = "text_value3";
    protected static final String TEXT_FIELD_2 = "abc";

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment environment;

    private ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

    @InjectMocks
    private TextEmbeddingProcessorFactory textEmbeddingProcessorFactory;

    @Captor
    private ArgumentCaptor<TextInferenceRequest> inferenceRequestCaptor;

    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(clusterService.state().metadata().index(anyString()).getSettings()).thenReturn(settings);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel2MapConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of("key1", ImmutableMap.of("test1", "test1_knn"), "key2", ImmutableMap.of("test3", CHILD_LEVEL_2_KNN_FIELD))
        );
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel1MapConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1_knn", "key2", "key2_knn"));
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel1MapConfig(int batchSize, boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1_knn", "key2", "key2_knn"));
        config.put(AbstractBatchingProcessor.BATCH_SIZE_FIELD, batchSize);
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedLevelConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_FIELD_LEVEL_2)),
                CHILD_LEVEL_2_KNN_FIELD
            )
        );
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedMappingsConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_FIELD_LEVEL_2)),
                CHILD_LEVEL_2_KNN_FIELD
            )
        );
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedSourceAndDestinationConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_1_TEXT_FIELD)),
                CHILD_FIELD_LEVEL_2 + "." + CHILD_LEVEL_2_KNN_FIELD
            )
        );
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedMapConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting);
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1)),
                Map.of(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_KNN_FIELD)
            )
        );
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedSourceAndDestinationMapConfig(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = buildObjMap(
            Pair.of(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId"),
            Pair.of(
                TextEmbeddingProcessor.FIELD_MAP_FIELD,
                buildObjMap(
                    Pair.of(
                        PARENT_FIELD,
                        Map.of(
                            CHILD_FIELD_LEVEL_1,
                            Map.of(CHILD_1_TEXT_FIELD, String.join(".", CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_KNN_FIELD))
                        )
                    )
                )
            ),
            Pair.of(TextEmbeddingProcessor.SKIP_EXISTING, skipExisting)
        );
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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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
        OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            openSearchClient,
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
        verify(handler).accept(isNull(), any(RuntimeException.class));
    }

    @SneakyThrows
    public void testExecute_whenInferenceTextListEmpty_SuccessWithoutEmbedding() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            openSearchClient,
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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_SimpleTypeWithEmptyStringValue_throwIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "    ");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);

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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);

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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);
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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);
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
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(false);

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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

        TextEmbeddingProcessor processor = createInstanceWithNestedLevelConfig(false);

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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

        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig(false);

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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

        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig(false);

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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
    @SuppressWarnings("unchecked")
    public void testNestedFieldInMappingForListWithNestedObj_withIngestDocumentWithoutDestinationStructure_theSuccessful() {
        /*
        modeling following document:
          parent: [
           {
               child_level_1:
                   child_1_text_field: "text_value",
           },
           {
               child_level_1:
                   child_1_text_field: "text_value",
                   child_2_text_field: "text_value2",
                   child_3_text_field: "text_value3",
           }

          ]
        */
        Map<String, Object> child1Level2 = buildObjMap(Pair.of(CHILD_1_TEXT_FIELD, TEXT_VALUE_1));
        Map<String, Object> child1Level1 = buildObjMap(Pair.of(CHILD_FIELD_LEVEL_1, child1Level2));
        Map<String, Object> child2Level2 = buildObjMap(
            Pair.of(CHILD_1_TEXT_FIELD, TEXT_VALUE_1),
            Pair.of(CHILD_2_TEXT_FIELD, TEXT_VALUE_2),
            Pair.of(CHILD_3_TEXT_FIELD, TEXT_VALUE_3)
        );
        Map<String, Object> child2Level1 = buildObjMap(Pair.of(CHILD_FIELD_LEVEL_1, child2Level2));
        Map<String, Object> sourceAndMetadata = buildObjMap(
            Pair.of(PARENT_FIELD, Arrays.asList(child1Level1, child2Level1)),
            Pair.of(IndexFieldMapper.NAME, "my_index")
        );
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationMapConfig(false);

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        assertNotNull(ingestDocument);
        assertNotNull(ingestDocument.getSourceAndMetadata().get(PARENT_FIELD));

        List<Map<String, Object>> parentAfterProcessor = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata()
            .get(PARENT_FIELD);

        for (Map<String, Object> childActual : parentAfterProcessor) {
            Map<String, Object> childLevel1Actual = (Map<String, Object>) childActual.get(CHILD_FIELD_LEVEL_1);
            assertEquals(TEXT_VALUE_1, childLevel1Actual.get(CHILD_1_TEXT_FIELD));
            assertNotNull(childLevel1Actual.get(CHILD_FIELD_LEVEL_2));
            Map<String, Object> childLevel2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
            List<Float> vectors = (List<Float>) childLevel2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
            assertEquals(100, vectors.size());
            for (Float vector : vectors) {
                assertTrue(vector >= 0.0f && vector <= 1.0f);
            }
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

        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfig(false);

        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(1, 100, 0.0f, 1.0f);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(false);
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
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(false);
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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);
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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);

        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

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
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(false);
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
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testGetType_successful() {
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(false);
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

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withFlattenedNestedMap_successful() {
        Map<String, Object> config = createNestedMapConfiguration();
        IngestDocument ingestDocument = createFlattenedNestedMapIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        processor.preprocessIngestDocument(ingestDocument);
        Map<String, Object> knnMap = processor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 100, 0.0f, 1.0f);
        processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        /**
         * "favorites.favorite": {
         *      "movie": "matrix",
         *      "actor": "Charlie Chaplin",
         *      "games" : {
         *          "adventure": {
         *              "action": "overwatch",
         *              "rpg": "elden ring"
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

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withFlattenedNestedMapAndList_successful() {
        Map<String, Object> config = createNestedMapConfiguration();
        IngestDocument ingestDocument = createFlattenedNestedMapAndListIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        processor.preprocessIngestDocument(ingestDocument);
        Map<String, Object> knnMap = processor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(3, 100, 0.0f, 1.0f);
        processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        /**
         * "favorites.favorite": {
         *      "movie": "matrix",
         *      "actor": "Charlie Chaplin",
         *      "games" : [
         *          {
         *              "adventure": {
         *                  "action": "overwatch",
         *                  "rpg": "elden ring"
         *              }
         *          },
         *          {
         *              "adventure.action": "wukong"
         *          }
         *      ]
         * }
         */
        Map<String, Object> favoritesMap = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("favorites");
        assertNotNull(favoritesMap);
        Map<String, Object> favorite = (Map<String, Object>) favoritesMap.get("favorite");
        assertNotNull(favorite);

        List<Map<String, Object>> favoriteGames = (List<Map<String, Object>>) favorite.get("games");
        assertNotNull(favoriteGames);
        for (Map<String, Object> favoriteGame : favoriteGames) {
            Map<String, Object> adventure = (Map<String, Object>) favoriteGame.get("adventure");
            List<Float> adventureKnnVector = (List<Float>) adventure.get("with_action_knn");
            assertNotNull(adventureKnnVector);
            assertEquals(100, adventureKnnVector.size());
            for (float vector : adventureKnnVector) {
                assertTrue(vector >= 0.0f && vector <= 1.0f);
            }
        }

        List<Float> favoriteKnnVector = (List<Float>) favorite.get("favorite_movie_knn");
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
    public void testBuildVectorOutput_withNestedListLevel2_withNestedFields_successful() {
        Map<String, Object> config = createNestedList2LevelConfiguration();
        IngestDocument ingestDocument = create2LevelNestedListWithNestedFieldsIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 2, 0.0f, 1.0f);
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("nestedField");

        for (Map<String, Object> singleNestedObjMap : nestedObj) {
            Map<String, Object> singleNestedObj = (Map<String, Object>) singleNestedObjMap.get("nestedField");
            assertTrue(singleNestedObj.containsKey("vectorField"));
            assertNotNull(singleNestedObj.get("vectorField"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withNestedListLevel2_withPartialNullNestedFields_successful() {
        Map<String, Object> config = createNestedList2LevelConfiguration();
        IngestDocument ingestDocument = create2LevelNestedListWithNestedFieldsIngestDocument();
        /**
         * Ingest doc with below fields
         * "nestedField": {
         *     "nestedField": [
         *          {
         *              "nestedField": {
         *                  "textField": null,
         *              }
         *          },
         *          {
         *              "nestedField": {
         *                  "textField": "This is another text field",
         *               }
         *          }
         *     ]
         * }
         */
        List<Map<String, Object>> nestedList = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("nestedField");
        Map<String, Object> objWithNullText = buildObjMap(Pair.of("textField", null));
        Map<String, Object> nestedObjWithNullText = buildObjMap(Pair.of("nestedField", objWithNullText));
        nestedList.set(0, nestedObjWithNullText);
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeys(ingestDocument);
        List<List<Float>> modelTensorList = createRandomOneDimensionalMockVector(2, 2, 0.0f, 1.0f);
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("nestedField");

        IntStream.range(0, nestedObj.size()).forEachOrdered(index -> {
            Map<String, Object> singleNestedObjMap = nestedObj.get(index);
            if (index == 0) {
                Map<String, Object> singleNestedObj = (Map<String, Object>) singleNestedObjMap.get("nestedField");
                assertFalse(singleNestedObj.containsKey("vectorField"));
                assertTrue(singleNestedObj.containsKey("textField"));
            } else {
                Map<String, Object> singleNestedObj = (Map<String, Object>) singleNestedObjMap.get("nestedField");
                assertTrue(singleNestedObj.containsKey("vectorField"));
                assertNotNull(singleNestedObj.get("vectorField"));
            }
        });
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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount, false);

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
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount, false);
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

    public void test_batchExecute_withSkipExisting_exception() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        List<IngestDocumentWrapper> updateDocumentWrappers = createIngestDocumentWrappers(docCount);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount, true);
        Consumer resultHandler = mock(Consumer.class);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(List.of("value1", "value1", "value1", "value1", "value1"))
            .build();
        mockVectorCreation(ingestRequest, ingestRequest);
        mockFailedUpdateMultipleDocuments(ingestDocumentWrappers);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        processor.batchExecute(updateDocumentWrappers, resultHandler);
        for (IngestDocumentWrapper updateDocumentWrapper : updateDocumentWrappers) {
            assertNotNull(updateDocumentWrapper.getException());
        }
    }

    @SneakyThrows
    public void testExecute_when_initial_ingest_with_skip_existing_flag_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("_id", "1");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        List<String> inferenceList = Arrays.asList("value1", "value2");
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(true);

        GetResponse response = mockEmptyGetResponse();
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(openSearchClient).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(isA(TextInferenceRequest.class), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(1)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
    }

    @SneakyThrows
    public void testExecute_with_no_update_with_skip_existing_flag_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", "value1");
        ingestSourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        List<String> inferenceList = Arrays.asList("value1", "value2");
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata); // no change
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, null);
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler); // insert document
        processor.execute(updateDocument, handler); // update document

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List key1insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key1_knn");
        List key1updateVectors = (List) updateDocument.getSourceAndMetadata().get("key1_knn");
        List key2insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key2_knn");
        List key2updateVectors = (List) updateDocument.getSourceAndMetadata().get("key2_knn");
        verifyEqualEmbedding(key1insertVectors, key1updateVectors);
        verifyEqualEmbedding(key2insertVectors, key2updateVectors);
    }

    public void testExecute_with_updated_field_skip_existing_flag_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", "value1");
        ingestSourceAndMetadata.put("key2", "value2");
        List<String> inferenceList = Arrays.asList("value1", "value2");
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        updateSourceAndMetadata.put("key2", "newValue"); // updated
        List<String> filteredInferenceList = List.of("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler); // insert
        processor.execute(updateDocument, handler); // update

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());

        List key1insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key1_knn");
        List key1updateVectors = (List) updateDocument.getSourceAndMetadata().get("key1_knn");
        List key2insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key2_knn");
        List key2updateVectors = (List) updateDocument.getSourceAndMetadata().get("key2_knn");
        verifyEqualEmbedding(key1insertVectors, key1updateVectors);
        assertEquals(key2insertVectors.size(), key2updateVectors.size());
    }

    public void testExecute_withListTypeInput_no_update_skip_existing_flag_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", list1);
        ingestSourceAndMetadata.put("key2", list2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, null);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List key1insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key1_knn");
        List key1updateVectors = (List) updateDocument.getSourceAndMetadata().get("key1_knn");
        List key2insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key2_knn");
        List key2updateVectors = (List) updateDocument.getSourceAndMetadata().get("key2_knn");
        verifyEqualEmbeddingInMap(key1insertVectors, key1updateVectors);
        verifyEqualEmbeddingInMap(key2insertVectors, key2updateVectors);
    }

    public void testExecute_withListTypeInput_with_update_skip_existing_flag_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", list1);
        ingestSourceAndMetadata.put("key2", list2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        updateSourceAndMetadata.put("key1", ImmutableList.of("test1", "newValue1", "newValue2"));
        updateSourceAndMetadata.put("key2", ImmutableList.of("newValue3", "test5", "test6"));
        List<String> filteredInferenceList = Arrays.asList("test1", "newValue1", "newValue2", "newValue3", "test5", "test6");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        List key1insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key1_knn");
        List key1updateVectors = (List) updateDocument.getSourceAndMetadata().get("key1_knn");
        List key2insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key2_knn");
        List key2updateVectors = (List) updateDocument.getSourceAndMetadata().get("key2_knn");
        assertEquals(key1insertVectors.size(), key1updateVectors.size());
        assertEquals(key2insertVectors.size(), key2updateVectors.size());
    }

    public void testExecute_withNestedListTypeInput_no_update_skip_existing_flag_successful() {
        Map<String, List<String>> map1 = new HashMap<>();
        map1.put("test1", ImmutableList.of("test1", "test2", "test3"));
        Map<String, List<String>> map2 = new HashMap<>();
        map2.put("test3", ImmutableList.of("test4", "test5", "test6"));
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", map1);
        ingestSourceAndMetadata.put("key2", map2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, null);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List key1IngestVectors = ((List) ((Map) ingestDocument.getSourceAndMetadata().get("key1")).get("test1_knn"));
        List key1UpdateVectors = ((List) ((Map) updateDocument.getSourceAndMetadata().get("key1")).get("test1_knn"));
        List key2IngestVectors = ((List) ((Map) ingestDocument.getSourceAndMetadata().get("key2")).get("test3_knn"));
        List key2UpdateVectors = ((List) ((Map) updateDocument.getSourceAndMetadata().get("key2")).get("test3_knn"));
        assertEquals(key1IngestVectors.size(), key1UpdateVectors.size());
        assertEquals(key2IngestVectors.size(), key2UpdateVectors.size());
        verifyEqualEmbeddingInMap(key1IngestVectors, key1UpdateVectors);
        verifyEqualEmbeddingInMap(key2IngestVectors, key2UpdateVectors);
    }

    public void testExecute_withNestedListTypeInput_with_update_skip_existing_flag_successful() {
        Map<String, List<String>> map1 = new HashMap<>();
        map1.put("test1", ImmutableList.of("test1", "test2", "test3"));
        Map<String, List<String>> map2 = new HashMap<>();
        map2.put("test3", ImmutableList.of("test4", "test5", "test6"));
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", map1);
        ingestSourceAndMetadata.put("key2", map2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        ((Map) updateSourceAndMetadata.get("key1")).put("test1", ImmutableList.of("test1", "newValue1", "newValue2"));
        ((Map) updateSourceAndMetadata.get("key2")).put("test3", ImmutableList.of("newValue3", "test5", "test6"));

        List<String> filteredInferenceList = Arrays.asList("test1", "newValue1", "newValue2", "newValue3", "test5", "test6");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());

        List key1IngestVectors = ((List) ((Map) ingestDocument.getSourceAndMetadata().get("key1")).get("test1_knn"));
        List key1UpdateVectors = ((List) ((Map) updateDocument.getSourceAndMetadata().get("key1")).get("test1_knn"));
        List key2IngestVectors = ((List) ((Map) ingestDocument.getSourceAndMetadata().get("key2")).get("test3_knn"));
        List key2UpdateVectors = ((List) ((Map) updateDocument.getSourceAndMetadata().get("key2")).get("test3_knn"));
        assertEquals(key1IngestVectors.size(), key1UpdateVectors.size());
        assertEquals(key2IngestVectors.size(), key2UpdateVectors.size());
    }

    public void testExecute_withMapTypeInput_no_update_skip_existing_flag_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test3", "test4");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", map1);
        ingestSourceAndMetadata.put("key2", map2);
        List<String> inferenceList = Arrays.asList("test2", "test4");
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(true);
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List test1KnnInsertVectors = (List) ((Map) ingestDocument.getSourceAndMetadata().get("key1")).get("test1_knn");
        List test1KnnUpdateVectors = (List) ((Map) updateDocument.getSourceAndMetadata().get("key1")).get("test1_knn");
        List test3KnnInsertVectors = (List) ((Map) ingestDocument.getSourceAndMetadata().get("key2")).get("test3_knn");
        List test3KnnUpdateVectors = (List) ((Map) updateDocument.getSourceAndMetadata().get("key2")).get("test3_knn");
        verifyEqualEmbedding(test1KnnInsertVectors, test1KnnUpdateVectors);
        verifyEqualEmbedding(test3KnnInsertVectors, test3KnnUpdateVectors);
    }

    public void testExecute_withMapTypeInput_with_update_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test3", "test4");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", map1);
        ingestSourceAndMetadata.put("key2", map2);
        List<String> inferenceList = Arrays.asList("test2", "test4");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig(true);
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        ((Map) updateSourceAndMetadata.get("key1")).put("test1", "newValue1");
        List<String> filteredInferenceList = Arrays.asList("newValue1");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        List test1KnnInsertVectors = (List) ((Map) ingestDocument.getSourceAndMetadata().get("key2")).get("test3_knn");
        List test1KnnUpdateVectors = (List) ((Map) updateDocument.getSourceAndMetadata().get("key2")).get("test3_knn");
        verifyEqualEmbedding(test1KnnInsertVectors, test1KnnUpdateVectors);
    }

    @SneakyThrows
    public void testNestedFieldInMapping_withMapTypeInput_no_update_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        Map<String, String> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        ingestSourceAndMetadata.put(PARENT_FIELD, childLevel1);
        List<String> inferenceList = Arrays.asList(CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithNestedLevelConfig(true);

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List test3KnnInsertVectors = (List) (((Map) ((Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(
            CHILD_FIELD_LEVEL_1
        ))).get(CHILD_LEVEL_2_KNN_FIELD);
        List test3KnnUpdateVectors = (List) (((Map) ((Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(
            CHILD_FIELD_LEVEL_1
        ))).get(CHILD_LEVEL_2_KNN_FIELD);
        verifyEqualEmbedding(test3KnnInsertVectors, test3KnnUpdateVectors);
    }

    @SneakyThrows
    public void testNestedFieldInMapping_withMapTypeInput_with_update_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        Map<String, String> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        ingestSourceAndMetadata.put(PARENT_FIELD, childLevel1);
        List<String> inferenceList = Arrays.asList(CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        ((Map) ((Map) updateSourceAndMetadata.get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1)).put(CHILD_FIELD_LEVEL_2, "newValue");
        List<String> filteredInferenceList = Arrays.asList("newValue");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        TextEmbeddingProcessor processor = createInstanceWithNestedMappingsConfig(true);

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        List test3KnnInsertVectors = (List) (((Map) ((Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(
            CHILD_FIELD_LEVEL_1
        ))).get(CHILD_LEVEL_2_KNN_FIELD);
        List test3KnnUpdateVectors = (List) (((Map) ((Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(
            CHILD_FIELD_LEVEL_1
        ))).get(CHILD_LEVEL_2_KNN_FIELD);
        assertEquals(test3KnnInsertVectors.size(), test3KnnUpdateVectors.size());
    }

    @SneakyThrows
    public void testNestedFieldInMappingForSourceAndDestination_withIngestDocumentHasTheDestinationStructure_no_update_thenSuccessful() {
        /*
        modeling following document:
          parent:
           child_level_1:
               child_level_1_text_field: "text"
               child_level_2:
                   child_level_2_text_field: "abc"
         */
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        Map<String, String> childLevel2NestedField = new HashMap<>();
        childLevel2NestedField.put(CHILD_LEVEL_2_TEXT_FIELD_VALUE, TEXT_FIELD_2);
        Map<String, Object> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, childLevel2NestedField);
        childLevel2.put(CHILD_1_TEXT_FIELD, TEXT_VALUE_1);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        ingestSourceAndMetadata.put(PARENT_FIELD, childLevel1);
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig(true);

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        Map nestedIngestMap = (Map) (((Map) ((Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1))).get(
            CHILD_FIELD_LEVEL_2
        );
        Map nestedUpdateMap = (Map) (((Map) ((Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1))).get(
            CHILD_FIELD_LEVEL_2
        );
        List test3KnnIngestVectors = (List) nestedIngestMap.get(CHILD_LEVEL_2_KNN_FIELD);
        List test3KnnUpdateVectors = (List) nestedUpdateMap.get(CHILD_LEVEL_2_KNN_FIELD);

        verifyEqualEmbedding(test3KnnIngestVectors, test3KnnUpdateVectors);
    }

    @SneakyThrows
    public void testNestedFieldInMappingForSourceAndDestination_withIngestDocumentHasTheDestinationStructure_with_update_thenSuccessful() {
        /*
        modeling following document:
          parent:
           child_level_1:
               child_level_1_text_field: "text"
               child_level_2:
                   child_level_2_text_field: "abc"
         */
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        Map<String, String> childLevel2NestedField = new HashMap<>();
        childLevel2NestedField.put(CHILD_LEVEL_2_TEXT_FIELD_VALUE, TEXT_FIELD_2);
        Map<String, Object> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, childLevel2NestedField);
        childLevel2.put(CHILD_1_TEXT_FIELD, TEXT_VALUE_1);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        ingestSourceAndMetadata.put(PARENT_FIELD, childLevel1);
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        ((Map) ((Map) updateSourceAndMetadata.get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1)).put(CHILD_1_TEXT_FIELD, "newValue");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        List<String> filteredInferenceList = Arrays.asList("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();
        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig(true);

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        Map nestedIngestMap = (Map) (((Map) ((Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1))).get(
            CHILD_FIELD_LEVEL_2
        );
        Map nestedUpdateMap = (Map) (((Map) ((Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1))).get(
            CHILD_FIELD_LEVEL_2
        );
        List test3KnnIngestVectors = (List) nestedIngestMap.get(CHILD_LEVEL_2_KNN_FIELD);
        List test3KnnUpdateVectors = (List) nestedUpdateMap.get(CHILD_LEVEL_2_KNN_FIELD);

        assertEquals(test3KnnIngestVectors.size(), test3KnnUpdateVectors.size());
    }

    @SneakyThrows
    public void testNestedFieldInMappingForSourceAndDestination_withIngestDocumentWithoutDestinationStructure_no_update_thenSuccessful() {
        /*
        modeling following document:
          parent:
           child_level_1:
               child_level_1_text_field: "text"
        */
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        Map<String, Object> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_1_TEXT_FIELD, TEXT_VALUE_1);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        ingestSourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1);
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        Map parent1AfterProcessor = (Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map childLevel1Actual = (Map) parent1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        Map child2Actual = (Map) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
        Map updateParent1AfterProcessor = (Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map updateChildLevel1Actual = (Map) updateParent1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        Map updateChild2Actual = (Map) updateChildLevel1Actual.get(CHILD_FIELD_LEVEL_2);
        List ingestVectors = (List) child2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
        List updateVectors = (List) updateChild2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
        verifyEqualEmbedding(ingestVectors, updateVectors);

    }

    @SneakyThrows
    public void testNestedFieldInMappingForSourceAndDestination_withIngestDocumentWithoutDestinationStructure_with_update_thenSuccessful() {
        /*
        modeling following document:
          parent:
           child_level_1:
               child_level_1_text_field: "text"
        */
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        Map<String, Object> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_1_TEXT_FIELD, TEXT_VALUE_1);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        ingestSourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        ((Map) ((Map) updateSourceAndMetadata.get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1)).put(CHILD_1_TEXT_FIELD, "newValue");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        List<String> filteredInferenceList = Arrays.asList("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();
        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        Map parent1AfterProcessor = (Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map childLevel1Actual = (Map) parent1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        Map child2Actual = (Map) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
        Map updateParent1AfterProcessor = (Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map updateChildLevel1Actual = (Map) updateParent1AfterProcessor.get(CHILD_FIELD_LEVEL_1);
        Map updateChild2Actual = (Map) updateChildLevel1Actual.get(CHILD_FIELD_LEVEL_2);
        List ingestVectors = (List) child2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
        List updateVectors = (List) updateChild2Actual.get(CHILD_LEVEL_2_KNN_FIELD);
        assertEquals(ingestVectors.size(), updateVectors.size());

    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testNestedFieldInMappingForListWithNestedObj_withIngestDocumentWithoutDestinationStructure_no_update_theSuccessful() {
        /*
        modeling following document:
          parent: [
           {
               child_level_1:
                   child_1_text_field: "text_value",
           },
           {
               child_level_1:
                   child_1_text_field: "text_value",
                   child_2_text_field: "text_value2",
                   child_3_text_field: "text_value3",
           }

          ]
        */
        Map<String, Object> child1Level2 = buildObjMap(Pair.of(CHILD_1_TEXT_FIELD, TEXT_VALUE_1));
        Map<String, Object> child1Level1 = buildObjMap(Pair.of(CHILD_FIELD_LEVEL_1, child1Level2));
        Map<String, Object> child2Level2 = buildObjMap(
            Pair.of(CHILD_1_TEXT_FIELD, TEXT_VALUE_1),
            Pair.of(CHILD_2_TEXT_FIELD, TEXT_VALUE_2),
            Pair.of(CHILD_3_TEXT_FIELD, TEXT_VALUE_3)
        );
        Map<String, Object> child2Level1 = buildObjMap(Pair.of(CHILD_FIELD_LEVEL_1, child2Level2));
        Map<String, Object> sourceAndMetadata = buildObjMap(
            Pair.of(PARENT_FIELD, Arrays.asList(child1Level1, child2Level1)),
            Pair.of(IndexFieldMapper.NAME, "my_index"),
            Pair.of("_id", "1")
        );
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Object> updateSourceAndMetadata = deepCopy(sourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationMapConfig(true);
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1, TEXT_VALUE_1);
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        List<Map<String, Object>> parentAfterIngestProcessor = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata()
            .get(PARENT_FIELD);

        List<Map<String, Object>> parentAfterUpdateProcessor = (List<Map<String, Object>>) updateDocument.getSourceAndMetadata()
            .get(PARENT_FIELD);
        List<List<Number>> insertVectors = new ArrayList<>();
        List<List<Number>> updateVectors = new ArrayList<>();
        for (Map<String, Object> childActual : parentAfterIngestProcessor) {
            Map<String, Object> childLevel1Actual = (Map<String, Object>) childActual.get(CHILD_FIELD_LEVEL_1);
            assertEquals(TEXT_VALUE_1, childLevel1Actual.get(CHILD_1_TEXT_FIELD));
            assertNotNull(childLevel1Actual.get(CHILD_FIELD_LEVEL_2));
            Map<String, Object> childLevel2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
            insertVectors.add((List<Number>) childLevel2Actual.get(CHILD_LEVEL_2_KNN_FIELD));
        }

        for (Map<String, Object> childActual : parentAfterUpdateProcessor) {
            Map<String, Object> childLevel1Actual = (Map<String, Object>) childActual.get(CHILD_FIELD_LEVEL_1);
            assertEquals(TEXT_VALUE_1, childLevel1Actual.get(CHILD_1_TEXT_FIELD));
            assertNotNull(childLevel1Actual.get(CHILD_FIELD_LEVEL_2));
            Map<String, Object> childLevel2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
            updateVectors.add((List<Number>) childLevel2Actual.get(CHILD_LEVEL_2_KNN_FIELD));
        }
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        verifyEqualEmbeddingInNestedList(insertVectors, updateVectors);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testNestedFieldInMappingForListWithNestedObj_withIngestDocumentWithoutDestinationStructure_with_update_theSuccessful() {
        /*
        modeling following document:
          parent: [
           {
               child_level_1:
                   child_1_text_field: "text_value",
           },
           {
               child_level_1:
                   child_1_text_field: "text_value",
                   child_2_text_field: "text_value2",
                   child_3_text_field: "text_value3",
           }

          ]
        */
        Map<String, Object> child1Level2 = buildObjMap(Pair.of(CHILD_1_TEXT_FIELD, TEXT_VALUE_1));
        Map<String, Object> child1Level1 = buildObjMap(Pair.of(CHILD_FIELD_LEVEL_1, child1Level2));
        Map<String, Object> child2Level2 = buildObjMap(
            Pair.of(CHILD_1_TEXT_FIELD, TEXT_VALUE_1),
            Pair.of(CHILD_2_TEXT_FIELD, TEXT_VALUE_2),
            Pair.of(CHILD_3_TEXT_FIELD, TEXT_VALUE_3)
        );
        Map<String, Object> child2Level1 = buildObjMap(Pair.of(CHILD_FIELD_LEVEL_1, child2Level2));
        Map<String, Object> sourceAndMetadata = buildObjMap(
            Pair.of(PARENT_FIELD, Arrays.asList(child1Level1, child2Level1)),
            Pair.of(IndexFieldMapper.NAME, "my_index"),
            Pair.of("_id", "1")
        );
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Object> updateSourceAndMetadata = deepCopy(sourceAndMetadata);
        ((Map) ((Map) ((List) updateSourceAndMetadata.get(PARENT_FIELD)).get(0)).get(CHILD_FIELD_LEVEL_1)).put(
            CHILD_1_TEXT_FIELD,
            "newValue"
        );
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationMapConfig(true);
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1, TEXT_VALUE_1);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelID").inputTexts(inferenceList).build();
        List<String> filteredInferenceList = Arrays.asList("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(filteredInferenceList)
            .build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});

        List<Map<String, Object>> parentAfterIngestProcessor = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata()
            .get(PARENT_FIELD);

        List<Map<String, Object>> parentAfterUpdateProcessor = (List<Map<String, Object>>) updateDocument.getSourceAndMetadata()
            .get(PARENT_FIELD);
        List<List<Number>> insertVectors = new ArrayList<>();
        List<List<Number>> updateVectors = new ArrayList<>();
        for (Map<String, Object> childActual : parentAfterIngestProcessor) {
            Map<String, Object> childLevel1Actual = (Map<String, Object>) childActual.get(CHILD_FIELD_LEVEL_1);
            assertNotNull(childLevel1Actual.get(CHILD_FIELD_LEVEL_2));
            Map<String, Object> childLevel2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
            insertVectors.add((List<Number>) childLevel2Actual.get(CHILD_LEVEL_2_KNN_FIELD));
        }

        for (Map<String, Object> childActual : parentAfterUpdateProcessor) {
            Map<String, Object> childLevel1Actual = (Map<String, Object>) childActual.get(CHILD_FIELD_LEVEL_1);
            assertNotNull(childLevel1Actual.get(CHILD_FIELD_LEVEL_2));
            Map<String, Object> childLevel2Actual = (Map<String, Object>) childLevel1Actual.get(CHILD_FIELD_LEVEL_2);
            updateVectors.add((List<Number>) childLevel2Actual.get(CHILD_LEVEL_2_KNN_FIELD));
        }
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        assertEquals(insertVectors.get(0).size(), updateVectors.get(0).size());
        verifyEqualEmbedding(insertVectors.get(1), updateVectors.get(1));
    }

    @SneakyThrows
    public void testNestedFieldInMappingMixedSyntax_withMapTypeInput_no_update_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("_id", "1");
        Map<String, String> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        sourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Object> updateSourceAndMetadata = deepCopy(sourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfig(true);
        List<String> inferenceList = Arrays.asList(CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelID").inputTexts(inferenceList).build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});
        Map childLevel1AfterIngestProcessor = (Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map childLevel2AfterIngestProcessor = (Map) childLevel1AfterIngestProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(CHILD_LEVEL_2_TEXT_FIELD_VALUE, childLevel2AfterIngestProcessor.get(CHILD_FIELD_LEVEL_2));
        assertNotNull(childLevel2AfterIngestProcessor.get(CHILD_LEVEL_2_KNN_FIELD));
        List ingestVectors = (List) childLevel2AfterIngestProcessor.get(CHILD_LEVEL_2_KNN_FIELD);
        Map childLevel1AfterUpdatetProcessor = (Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map childLevel2AfterUpdateProcessor = (Map) childLevel1AfterUpdatetProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(CHILD_LEVEL_2_TEXT_FIELD_VALUE, childLevel2AfterIngestProcessor.get(CHILD_FIELD_LEVEL_2));
        assertNotNull(childLevel2AfterUpdateProcessor.get(CHILD_LEVEL_2_KNN_FIELD));
        List updateVectors = (List) childLevel2AfterUpdateProcessor.get(CHILD_LEVEL_2_KNN_FIELD);
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        verifyEqualEmbedding(ingestVectors, updateVectors);
    }

    @SneakyThrows
    public void testNestedFieldInMappingMixedSyntax_withMapTypeInput_with_update_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("_id", "1");
        Map<String, String> childLevel2 = new HashMap<>();
        childLevel2.put(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        Map<String, Object> childLevel1 = new HashMap<>();
        childLevel1.put(CHILD_FIELD_LEVEL_1, childLevel2);
        sourceAndMetadata.put(PARENT_FIELD, childLevel1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Object> updateSourceAndMetadata = deepCopy(sourceAndMetadata);
        ((Map) ((Map) updateSourceAndMetadata.get(PARENT_FIELD)).get(CHILD_FIELD_LEVEL_1)).put(CHILD_FIELD_LEVEL_2, "newValue");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfig(true);
        List<String> inferenceList = Arrays.asList(CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelID").inputTexts(inferenceList).build();
        List<String> filteredInferenceList = Arrays.asList("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(filteredInferenceList)
            .build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        processor.execute(ingestDocument, (BiConsumer) (doc, ex) -> {});
        processor.execute(updateDocument, (BiConsumer) (doc, ex) -> {});
        Map childLevel1AfterIngestProcessor = (Map) ingestDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map childLevel2AfterIngestProcessor = (Map) childLevel1AfterIngestProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(CHILD_LEVEL_2_TEXT_FIELD_VALUE, childLevel2AfterIngestProcessor.get(CHILD_FIELD_LEVEL_2));
        assertNotNull(childLevel2AfterIngestProcessor.get(CHILD_LEVEL_2_KNN_FIELD));
        List ingestVectors = (List) childLevel2AfterIngestProcessor.get(CHILD_LEVEL_2_KNN_FIELD);
        Map childLevel1AfterUpdateProcessor = (Map) updateDocument.getSourceAndMetadata().get(PARENT_FIELD);
        Map childLevel2AfterUpdateProcessor = (Map) childLevel1AfterUpdateProcessor.get(CHILD_FIELD_LEVEL_1);
        assertEquals(CHILD_LEVEL_2_TEXT_FIELD_VALUE, childLevel2AfterIngestProcessor.get(CHILD_FIELD_LEVEL_2));
        assertNotNull(childLevel2AfterUpdateProcessor.get(CHILD_LEVEL_2_KNN_FIELD));
        List updateVectors = (List) childLevel2AfterUpdateProcessor.get(CHILD_LEVEL_2_KNN_FIELD);
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        assertEquals(ingestVectors.size(), updateVectors.size());
    }

    public void test_batchExecute_no_update_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        List<IngestDocumentWrapper> updateDocumentWrappers = createIngestDocumentWrappers(docCount);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount, true);
        Consumer resultHandler = mock(Consumer.class);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(List.of("value1", "value1", "value1", "value1", "value1"))
            .build();
        mockVectorCreation(ingestRequest, null);
        mockUpdateMultipleDocuments(ingestDocumentWrappers);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        processor.batchExecute(updateDocumentWrappers, resultHandler);
        for (int i = 0; i < docCount; ++i) {
            assertEquals(
                ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get("key1"),
                updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get("key1")
            );
            verifyEqualEmbedding(
                (List<Number>) ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get("key1_knn"),
                (List<Number>) updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get("key1_knn")
            );
        }
        verify(openSearchClient, times(2)).execute(isA(MultiGetAction.class), isA(MultiGetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
    }

    public void test_batchExecute_with_update_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        List<IngestDocumentWrapper> updateDocumentWrappers = createIngestDocumentWrappers(docCount, "newValue");
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig(docCount, true);
        Consumer resultHandler = mock(Consumer.class);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(List.of("value1", "value1", "value1", "value1", "value1"))
            .build();
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(List.of("newValue", "newValue", "newValue", "newValue", "newValue"))
            .build();
        mockVectorCreation(ingestRequest, updateRequest);
        mockUpdateMultipleDocuments(ingestDocumentWrappers);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        processor.batchExecute(updateDocumentWrappers, resultHandler);
        for (int i = 0; i < docCount; ++i) {
            List<Number> ingestVectors = (List<Number>) ingestDocumentWrappers.get(i)
                .getIngestDocument()
                .getSourceAndMetadata()
                .get("key1_knn");
            List<Number> updateVectors = (List<Number>) updateDocumentWrappers.get(i)
                .getIngestDocument()
                .getSourceAndMetadata()
                .get("key1_knn");
            assertEquals(ingestVectors.size(), updateVectors.size());
        }
        verify(openSearchClient, times(2)).execute(isA(MultiGetAction.class), isA(MultiGetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
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
        Map<String, Object> config = buildObjMap(
            Pair.of(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId"),
            Pair.of(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap)
        );
        return (TextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    private Map<String, Object> createPlainStringConfiguration() {
        return buildObjMap(
            Pair.of("oriKey1", "oriKey1_knn"),
            Pair.of("oriKey2", "oriKey2_knn"),
            Pair.of("oriKey3", "oriKey3_knn"),
            Pair.of("oriKey4", "oriKey4_knn"),
            Pair.of("oriKey5", "oriKey5_knn"),
            Pair.of("oriKey6", "oriKey6_knn")
        );
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
        Map<String, Object> adventureGames = buildObjMap(Pair.of("adventure.action", "with_action_knn"));
        Map<String, Object> favorite = buildObjMap(
            Pair.of("favorite.movie", "favorite_movie_knn"),
            Pair.of("favorite.games", adventureGames)
        );
        Map<String, Object> result = buildObjMap(Pair.of("favorites", favorite));
        return result;
    }

    private IngestDocument createPlainIngestDocument() {
        Map<String, Object> result = buildObjMap(
            Pair.of("oriKey1", "oriValue1"),
            Pair.of("oriKey2", "oriValue2"),
            Pair.of("oriKey3", "oriValue3"),
            Pair.of("oriKey4", "oriValue4"),
            Pair.of("oriKey5", "oriValue5"),
            Pair.of("oriKey6", ImmutableList.of("oriValue6", "oriValue7"))
        );
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
        Map<String, Object> adventureGames = buildObjMap(Pair.of("action", "overwatch"), Pair.of("rpg", "elden ring"));
        Map<String, Object> favGames = buildObjMap(Pair.of("adventure", adventureGames));
        Map<String, Object> favorites = buildObjMap(
            Pair.of("movie", "matrix"),
            Pair.of("games", favGames),
            Pair.of("actor", "Charlie Chaplin")
        );
        Map<String, Object> favorite = buildObjMap(Pair.of("favorite", favorites));
        Map<String, Object> result = buildObjMap(Pair.of("favorites", favorite));
        return new IngestDocument(result, new HashMap<>());
    }

    /**
     * Create following document with flattened nested map
     * "favorites.favorite": {
     *      "movie": "matrix",
     *      "actor": "Charlie Chaplin",
     *      "games" : {
     *          "adventure": {
     *              "action": "overwatch",
     *              "rpg": "elden ring"
     *          }
     *      }
     * }
     */
    private IngestDocument createFlattenedNestedMapIngestDocument() {
        Map<String, Object> adventureGames = buildObjMap(Pair.of("action", "overwatch"), Pair.of("rpg", "elden ring"));
        Map<String, Object> favGames = buildObjMap(Pair.of("adventure", adventureGames));
        Map<String, Object> favorites = buildObjMap(
            Pair.of("movie", "matrix"),
            Pair.of("games", favGames),
            Pair.of("actor", "Charlie Chaplin")
        );
        Map<String, Object> result = buildObjMap(Pair.of("favorites.favorite", favorites));
        return new IngestDocument(result, new HashMap<>());
    }

    /**
     * Create following document with flattened nested map and list
     * "favorites.favorite": {
     *      "movie": "matrix",
     *      "actor": "Charlie Chaplin",
     *      "games" : [
     *          {
     *              "adventure": {
     *                  "action": "overwatch",
     *                  "rpg": "elden ring"
     *              }
     *          },
     *          {
     *              "adventure.action": "wukong"
     *          }
     *      ]
     * }
     */
    private IngestDocument createFlattenedNestedMapAndListIngestDocument() {
        Map<String, Object> adventureGames = buildObjMap(Pair.of("action", "overwatch"), Pair.of("rpg", "elden ring"));
        Map<String, Object> game1 = buildObjMap(Pair.of("adventure", adventureGames));
        Map<String, Object> game2 = buildObjMap(Pair.of("adventure.action", "wukong"));
        Map<String, Object> favorites = buildObjMap(
            Pair.of("movie", "matrix"),
            Pair.of("games", Arrays.asList(game1, game2)),
            Pair.of("actor", "Charlie Chaplin")
        );
        Map<String, Object> result = buildObjMap(Pair.of("favorites.favorite", favorites));
        return new IngestDocument(result, new HashMap<>());
    }

    private Map<String, Object> createNestedListConfiguration() {
        Map<String, Object> nestedConfig = buildObjMap(Pair.of("textField", "vectorField"));
        return buildObjMap(Pair.of("nestedField", nestedConfig));
    }

    private Map<String, Object> createNestedList2LevelConfiguration() {
        Map<String, Object> nestedConfig = buildObjMap(Pair.of("textField", "vectorField"));
        Map<String, Object> nestConfigLevel1 = buildObjMap(Pair.of("nestedField", nestedConfig));
        return buildObjMap(Pair.of("nestedField", nestConfigLevel1));
    }

    private IngestDocument createNestedListIngestDocument() {
        Map<String, Object> nestedObj1 = buildObjMap(Pair.of("textField", "This is a text field"));
        Map<String, Object> nestedObj2 = buildObjMap(Pair.of("textField", "This is another text field"));
        Map<String, Object> nestedList = buildObjMap(Pair.of("nestedField", Arrays.asList(nestedObj1, nestedObj2)));
        return new IngestDocument(nestedList, new HashMap<>());
    }

    private IngestDocument createNestedListWithNotEmbeddingFieldIngestDocument() {
        Map<String, Object> nestedObj1 = buildObjMap(Pair.of("textFieldNotForEmbedding", "This is a text field"));
        Map<String, Object> nestedObj2 = buildObjMap(Pair.of("textField", "This is another text field"));
        Map<String, Object> nestedList = buildObjMap(Pair.of("nestedField", Arrays.asList(nestedObj1, nestedObj2)));
        return new IngestDocument(nestedList, new HashMap<>());
    }

    private IngestDocument create2LevelNestedListIngestDocument() {
        Map<String, Object> nestedObj1 = buildObjMap(Pair.of("textField", "This is a text field"));
        Map<String, Object> nestedObj2 = buildObjMap(Pair.of("textField", "This is another text field"));
        Map<String, Object> nestedList = buildObjMap(Pair.of("nestedField", Arrays.asList(nestedObj1, nestedObj2)));
        Map<String, Object> nestedList1 = buildObjMap(Pair.of("nestedField", nestedList));
        return new IngestDocument(nestedList1, new HashMap<>());
    }

    private IngestDocument create2LevelNestedListWithNestedFieldsIngestDocument() {
        Map<String, Object> nestedObj1Level2 = buildObjMap(Pair.of("textField", "This is a text field"));
        Map<String, Object> nestedObj1Level1 = buildObjMap(Pair.of("nestedField", nestedObj1Level2));

        Map<String, Object> nestedObj2Level2 = buildObjMap(Pair.of("textField", "This is another text field"));
        Map<String, Object> nestedObj2Level1 = buildObjMap(Pair.of("nestedField", nestedObj2Level2));

        Map<String, Object> nestedList = buildObjMap(Pair.of("nestedField", Arrays.asList(nestedObj1Level1, nestedObj2Level1)));
        return new IngestDocument(nestedList, new HashMap<>());
    }

    private Map<String, Object> buildObjMap(Pair<String, Object>... pairs) {
        Map<String, Object> objMap = new HashMap<>();
        for (Pair<String, Object> pair : pairs) {
            objMap.put(pair.getKey(), pair.getValue());
        }
        return objMap;
    }

    private IngestDocument create2LevelNestedListWithNotEmbeddingFieldIngestDocument() {
        Map<String, Object> nestedObj1 = buildObjMap(Pair.of("textFieldNotForEmbedding", "This is a text field"));
        Map<String, Object> nestedObj2 = buildObjMap(Pair.of("textField", "This is another text field"));
        Map<String, Object> nestedList = buildObjMap(Pair.of("nestedField", Arrays.asList(nestedObj1, nestedObj2)));
        Map<String, Object> nestedList1 = buildObjMap(Pair.of("nestedField", nestedList));
        return new IngestDocument(nestedList1, new HashMap<>());
    }

    private void mockUpdateDocument(IngestDocument ingestDocument) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(mockEmptyGetResponse()); // returns empty result for ingest action
            return null;
        }).doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(convertToGetResponse(ingestDocument)); // returns previously ingested document for update action
            return null;
        }).when(openSearchClient).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
    }

    private void mockUpdateMultipleDocuments(List<IngestDocumentWrapper> ingestDocuments) {
        doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(2);
            listener.onResponse(mockEmptyMultiGetItemResponse()); // returns empty result for ingest action
            return null;
        }).doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(2);
            listener.onResponse(convertToMultiGetItemResponse(ingestDocuments)); // returns previously ingested document for update action
            return null;
        }).when(openSearchClient).execute(isA(MultiGetAction.class), isA(MultiGetRequest.class), isA(ActionListener.class));
    }

    private void mockFailedUpdateMultipleDocuments(List<IngestDocumentWrapper> ingestDocuments) {
        doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(2);
            listener.onResponse(mockEmptyMultiGetItemResponse()); // returns empty result for ingest action
            return null;
        }).doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException()); // throw exception on update
            return null;
        }).when(openSearchClient).execute(isA(MultiGetAction.class), isA(MultiGetRequest.class), isA(ActionListener.class));
    }

    private void mockVectorCreation(TextInferenceRequest ingestRequest, TextInferenceRequest updateRequest) {
        doAnswer(invocation -> {
            int numVectors = ingestRequest.getInputTexts().size();
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(createRandomOneDimensionalMockVector(numVectors, 2, 0.0f, 1.0f));
            return null;
        }).doAnswer(invocation -> {
            int numVectors = updateRequest.getInputTexts().size();
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(createRandomOneDimensionalMockVector(numVectors, 2, 0.0f, 1.0f));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(isA(TextInferenceRequest.class), isA(ActionListener.class));
    }

    private void verifyEqualEmbedding(List<Number> insertVectors, List<Number> updateVectors) {
        assertEquals(insertVectors.size(), updateVectors.size());
        for (int i = 0; i < insertVectors.size(); i++) {
            assertEquals(insertVectors.get(i).floatValue(), updateVectors.get(i).floatValue(), 0.0000001f);
        }
    }

    private void verifyEqualEmbeddingInMap(List<Map> insertVectors, List<Map> updateVectors) {
        assertEquals(insertVectors.size(), updateVectors.size());

        for (int i = 0; i < insertVectors.size(); i++) {
            Map<String, List> insertMap = insertVectors.get(i);
            Map<String, List> updateMap = updateVectors.get(i);
            for (Map.Entry<String, List> entry : insertMap.entrySet()) {
                List<Number> insertValue = entry.getValue();
                List<Number> updateValue = updateMap.get(entry.getKey());
                for (int j = 0; j < insertValue.size(); j++) {
                    assertEquals(insertValue.get(j).floatValue(), updateValue.get(j).floatValue(), 0.0000001f);
                }
            }
        }
    }

    private void verifyEqualEmbeddingInNestedList(List<List<Number>> insertVectors, List<List<Number>> updateVectors) {
        assertEquals(insertVectors.size(), updateVectors.size());
        for (int i = 0; i < insertVectors.size(); i++) {
            List<Number> insertVector = insertVectors.get(i);
            List<Number> updateVector = updateVectors.get(i);
            for (int j = 0; j < insertVectors.size(); j++) {
                assertEquals(insertVector.get(j).floatValue(), updateVector.get(j).floatValue(), 0.0000001f);
            }
        }
    }
}
