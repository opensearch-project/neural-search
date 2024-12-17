/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.IMAGE_FIELD_NAME;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.TEXT_FIELD_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.TextImageEmbeddingProcessorFactory;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class TextImageEmbeddingProcessorTests extends OpenSearchTestCase {

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;
    @Mock
    private Environment env;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ClusterState clusterState;
    @Mock
    private Metadata metadata;
    @Mock
    private IndexMetadata indexMetadata;

    @InjectMocks
    private TextImageEmbeddingProcessorFactory textImageEmbeddingProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(env.settings()).thenReturn(settings);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(settings);
    }

    @SneakyThrows
    private TextImageEmbeddingProcessor createInstance() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextImageEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextImageEmbeddingProcessor.EMBEDDING_FIELD, "my_embedding_field");
        config.put(
            TextImageEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(TEXT_FIELD_NAME, "my_text_field", IMAGE_FIELD_NAME, "image_field")
        );
        return (TextImageEmbeddingProcessor) textImageEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapEmpty_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextImageEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        try {
            textImageEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        } catch (OpenSearchParseException e) {
            assertEquals("[embedding] required property is missing", e.getMessage());
        }
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenTypeMappingIsNullOrInvalid_throwIllegalArgumentException() {
        boolean ignoreFailure = false;
        String modelId = "mockModelId";
        String embeddingField = "my_embedding_field";

        // create with null type mapping
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TextImageEmbeddingProcessor(
                PROCESSOR_TAG,
                DESCRIPTION,
                modelId,
                embeddingField,
                null,
                mlCommonsClientAccessor,
                env,
                clusterService
            )
        );
        assertEquals("Unable to create the TextImageEmbedding processor as field_map has invalid key or value", exception.getMessage());

        // type mapping has empty key
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TextImageEmbeddingProcessor(
                PROCESSOR_TAG,
                DESCRIPTION,
                modelId,
                embeddingField,
                Map.of("", "my_field"),
                mlCommonsClientAccessor,
                env,
                clusterService
            )
        );
        assertEquals("Unable to create the TextImageEmbedding processor as field_map has invalid key or value", exception.getMessage());

        // type mapping has empty value
        // use vanila java syntax because it allows null values
        Map<String, String> typeMapping = new HashMap<>();
        typeMapping.put("my_field", null);

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TextImageEmbeddingProcessor(
                PROCESSOR_TAG,
                DESCRIPTION,
                modelId,
                embeddingField,
                typeMapping,
                mlCommonsClientAccessor,
                env,
                clusterService
            )
        );
        assertEquals("Unable to create the TextImageEmbedding processor as field_map has invalid key or value", exception.getMessage());
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenEmptyModelId_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextImageEmbeddingProcessor.MODEL_ID_FIELD, "");
        config.put(TextImageEmbeddingProcessor.EMBEDDING_FIELD, "my_embedding_field");
        config.put(
            TextImageEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(TEXT_FIELD_NAME, "my_text_field", IMAGE_FIELD_NAME, "image_field")
        );
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> textImageEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config)
        );
        assertEquals("model_id is null or empty, can not process it", exception.getMessage());
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("my_text_field", "value2");
        sourceAndMetadata.put("key3", "value3");
        sourceAndMetadata.put("image_field", "base64_of_image_1234567890");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    @SneakyThrows
    public void testExecute_whenInferenceThrowInterruptedException_throwRuntimeException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("my_text_field", "value1");
        sourceAndMetadata.put("another_text_field", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextImageEmbeddingProcessorFactory textImageEmbeddingProcessorFactory = new TextImageEmbeddingProcessorFactory(
            accessor,
            env,
            clusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextImageEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextImageEmbeddingProcessor.EMBEDDING_FIELD, "my_embedding_field");
        config.put(
            TextImageEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(TEXT_FIELD_NAME, "my_text_field", IMAGE_FIELD_NAME, "image_field")
        );
        TextImageEmbeddingProcessor processor = (TextImageEmbeddingProcessor) textImageEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        doThrow(new RuntimeException()).when(accessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(RuntimeException.class));
    }

    public void testExecute_withListTypeInput_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("my_text_field", "value1");
        sourceAndMetadata.put("another_text_field", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_mapDepthReachLimit_throwIllegalArgumentException() {
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> 1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "hello world");
        sourceAndMetadata.put("my_text_field", ret);
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_MLClientAccessorThrowFail_handlerFailure() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("my_text_field", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();

        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapHasNonStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, Double> map2 = ImmutableMap.of("test3", 209.3D);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("my_text_field", map2);
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapHasEmptyStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, String> map2 = ImmutableMap.of("test3", "   ");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("my_text_field", map2);
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_hybridTypeInput_successful() throws Exception {
        List<String> list1 = ImmutableList.of("test1", "test2");
        Map<String, List<String>> map1 = ImmutableMap.of("test3", list1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", map1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key2");
    }

    public void testExecute_whenInferencesAreEmpty_thenSuccessful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("my_field", "value1");
        sourceAndMetadata.put("another_text_field", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextImageEmbeddingProcessor processor = createInstance();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesMap(argThat(request -> request.getInputObjects() != null), isA(ActionListener.class));

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
}
