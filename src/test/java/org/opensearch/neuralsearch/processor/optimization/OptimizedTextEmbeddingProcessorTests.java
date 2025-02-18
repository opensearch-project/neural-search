/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.optimization;

import static org.mockito.ArgumentMatchers.anyString;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.InferenceProcessorTestCase;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;
import org.opensearch.transport.client.OpenSearchClient;

public class OptimizedTextEmbeddingProcessorTests extends InferenceProcessorTestCase {

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
    private OptimizedTextEmbeddingProcessor createInstanceWithLevel2MapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of("key1", ImmutableMap.of("test1", "test1_knn"), "key2", ImmutableMap.of("test3", CHILD_LEVEL_2_KNN_FIELD))
        );
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private OptimizedTextEmbeddingProcessor createInstanceWithLevel1MapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1_knn", "key2", "key2_knn"));
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private OptimizedTextEmbeddingProcessor createInstanceWithNestedLevelConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        config.put(
            OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_FIELD_LEVEL_2)),
                CHILD_LEVEL_2_KNN_FIELD
            )
        );
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private OptimizedTextEmbeddingProcessor createInstanceWithNestedMappingsConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        config.put(
            OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_FIELD_LEVEL_2)),
                CHILD_LEVEL_2_KNN_FIELD
            )
        );
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private OptimizedTextEmbeddingProcessor createInstanceWithNestedMapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        config.put(
            OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1)),
                Map.of(CHILD_FIELD_LEVEL_2, CHILD_LEVEL_2_KNN_FIELD)
            )
        );
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private OptimizedTextEmbeddingProcessor createInstanceWithNestedSourceAndDestinationConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        config.put(
            OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(
                String.join(".", Arrays.asList(PARENT_FIELD, CHILD_FIELD_LEVEL_1, CHILD_1_TEXT_FIELD)),
                CHILD_FIELD_LEVEL_2 + "." + CHILD_LEVEL_2_KNN_FIELD
            )
        );
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private OptimizedTextEmbeddingProcessor createInstanceWithNestedSourceAndDestinationMapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = buildObjMap(
            Pair.of(OptimizedTextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId"),
            Pair.of(
                OptimizedTextEmbeddingProcessor.FIELD_MAP_FIELD,
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
            Pair.of(TextEmbeddingProcessor.IGNORE_EXISTING, true)
        );
        return (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    public void testExecute_when_initial_ingest_successful() throws IOException {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("_id", "1");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        List<String> inferenceList = Arrays.asList("value1", "value2");
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        OptimizedInferenceProcessor processor = createInstanceWithLevel1MapConfig();

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
    public void testExecute_whenGetDocumentThrowsException_throwRuntimeException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        OpenSearchClient opensearchClient = mock(OpenSearchClient.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            opensearchClient,
            mlCommonsClientAccessor,
            environment,
            clusterService
        );
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.IGNORE_EXISTING, true);
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        OptimizedTextEmbeddingProcessor processor = (OptimizedTextEmbeddingProcessor) textEmbeddingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        doThrow(new RuntimeException()).when(opensearchClient)
            .execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(RuntimeException.class));
    }

    public void testExecute_with_no_update_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", "value1");
        ingestSourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        List<String> inferenceList = Arrays.asList("value1", "value2");
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata); // no change
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        OptimizedInferenceProcessor processor = createInstanceWithLevel1MapConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 2, 0.0f, 1.0f);
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler); // insert document
        processor.execute(updateDocument, handler); // update document

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List key1insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key1_knn");
        List key1updateVectors = (List) updateDocument.getSourceAndMetadata().get("key1_knn");
        List key2insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key2_knn");
        List key2updateVectors = (List) updateDocument.getSourceAndMetadata().get("key2_knn");
        verifyEqualEmbedding(key1insertVectors, key1updateVectors);
        verifyEqualEmbedding(key2insertVectors, key2updateVectors);
    }

    public void testExecute_with_updated_field_successful() {
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

        OptimizedInferenceProcessor processor = createInstanceWithLevel1MapConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 2, 0.0f, 1.0f);

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

    public void testExecute_withListTypeInput_no_update_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put("key1", list1);
        ingestSourceAndMetadata.put("key2", list2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        OptimizedInferenceProcessor processor = createInstanceWithLevel1MapConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(6, 2, 0.0f, 1.0f);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentences(inferenceRequestCaptor.capture(), isA(ActionListener.class));
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List key1insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key1_knn");
        List key1updateVectors = (List) updateDocument.getSourceAndMetadata().get("key1_knn");
        List key2insertVectors = (List) ingestDocument.getSourceAndMetadata().get("key2_knn");
        List key2updateVectors = (List) updateDocument.getSourceAndMetadata().get("key2_knn");
        verifyEqualEmbeddingInMap(key1insertVectors, key1updateVectors);
        verifyEqualEmbeddingInMap(key2insertVectors, key2updateVectors);
    }

    public void testExecute_withListTypeInput_with_update_successful() {
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
        List<String> filteredInferenceList = Arrays.asList("newValue1", "newValue2", "newValue3");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        OptimizedInferenceProcessor processor = createInstanceWithLevel1MapConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(6, 2, 0.0f, 1.0f);

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
        verifyEqualEmbeddingInMap(key1insertVectors, key1updateVectors, Arrays.asList(0));
        verifyEqualEmbeddingInMap(key2insertVectors, key2updateVectors, Arrays.asList(1, 2));
    }

    public void testExecute_withNestedListTypeInput_no_update_successful() {
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

        OptimizedInferenceProcessor processor = createInstanceWithLevel2MapConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(6, 2, 0.0f, 1.0f);

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

    public void testExecute_withNestedListTypeInput_with_update_successful() {
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

        List<String> filteredInferenceList = Arrays.asList("newValue1", "newValue2", "newValue3");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        OptimizedInferenceProcessor processor = createInstanceWithLevel2MapConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(6, 2, 0.0f, 1.0f);

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
        verifyEqualEmbeddingInMap(key1IngestVectors, key1UpdateVectors, Arrays.asList(0));
        verifyEqualEmbeddingInMap(key2IngestVectors, key2UpdateVectors, Arrays.asList(1, 2));
    }

    public void testExecute_withMapTypeInput_no_update_successful() {
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
        OptimizedTextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 6, 0.0f, 0.1f);

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
        OptimizedTextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        ((Map) updateSourceAndMetadata.get("key1")).put("test1", "newValue1");
        List<String> filteredInferenceList = Arrays.asList("newValue1");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 6, 0.0f, 0.1f);

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
        OptimizedInferenceProcessor processor = createInstanceWithNestedLevelConfig();

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 4, 0.0f, 1.0f);

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

        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedMappingsConfig();

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 4, 0.0f, 1.0f);

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

        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig();

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 100, 0.0f, 1.0f);

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
        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig();

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 100, 0.0f, 1.0f);

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
        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 100, 0.0f, 1.0f);

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
        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationConfig();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 100, 0.0f, 1.0f);

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

        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationMapConfig();
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1, TEXT_VALUE_1);
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 100, 0.0f, 1.0f);

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

        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedSourceAndDestinationMapConfig();
        List<String> inferenceList = Arrays.asList(TEXT_VALUE_1, TEXT_VALUE_1);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelID").inputTexts(inferenceList).build();
        List<String> filteredInferenceList = Arrays.asList("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(filteredInferenceList)
            .build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 100, 0.0f, 1.0f);

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

        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedMapConfig();
        List<String> inferenceList = Arrays.asList(CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelID").inputTexts(inferenceList).build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(1, 100, 0.0f, 1.0f);

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

        OptimizedTextEmbeddingProcessor processor = createInstanceWithNestedMapConfig();
        List<String> inferenceList = Arrays.asList(CHILD_LEVEL_2_TEXT_FIELD_VALUE);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelID").inputTexts(inferenceList).build();
        List<String> filteredInferenceList = Arrays.asList("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelID")
            .inputTexts(filteredInferenceList)
            .build();
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(2, 100, 0.0f, 1.0f);

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

    private Map<String, Object> buildObjMap(Pair<String, Object>... pairs) {
        Map<String, Object> objMap = new HashMap<>();
        for (Pair<String, Object> pair : pairs) {
            objMap.put(pair.getKey(), pair.getValue());
        }
        return objMap;
    }

    private GetResponse mockEmptyGetResponse() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("_index", "my_index")
            .field("_id", "1")
            .field("found", false)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        return GetResponse.fromXContent(contentParser);
    }

    private void verifyEqualEmbedding(List<Number> insertVectors, List<Number> updateVectors) {
        assertEquals(insertVectors.size(), updateVectors.size());
        for (int i = 0; i < insertVectors.size(); i++) {
            assertEquals(insertVectors.get(i).floatValue(), updateVectors.get(i).floatValue(), 0.0000001f);
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

    private void verifyEqualEmbeddingInMap(List<Map> insertVectors, List<Map> updateVectors, List<Integer> indices) {
        assertEquals(insertVectors.size(), updateVectors.size());

        for (int i = 0; i < indices.size(); i++) {
            Map<String, List> insertMap = insertVectors.get(indices.get(i));
            Map<String, List> updateMap = updateVectors.get(indices.get(i));
            for (Map.Entry<String, List> entry : insertMap.entrySet()) {
                List<Number> insertValue = entry.getValue();
                List<Number> updateValue = updateMap.get(entry.getKey());
                for (int j = 0; j < insertValue.size(); j++) {
                    assertEquals(insertValue.get(j).floatValue(), updateValue.get(j).floatValue(), 0.0000001f);
                }
            }

        }
    }

    private void mockVectorCreation(int numVectors, int vectorDimension, float min, float max) {
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(1);
            listener.onResponse(createRandomOneDimensionalMockVector(numVectors, vectorDimension, min, max));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(isA(TextInferenceRequest.class), isA(ActionListener.class));
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

    private GetResponse convertToGetResponse(IngestDocument ingestDocument) throws IOException {
        String index = ingestDocument.getSourceAndMetadata().get("_index").toString();
        String id = ingestDocument.getSourceAndMetadata().get("_id").toString();
        Map<String, Object> source = ingestDocument.getSourceAndMetadata();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(source);
        BytesReference bytes = BytesReference.bytes(builder);
        GetResult result = new GetResult(index, id, 0, 1, 1, true, bytes, null, null);
        return new GetResponse(result);
    }
}
