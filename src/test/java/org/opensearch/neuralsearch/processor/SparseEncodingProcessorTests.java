/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
import org.opensearch.ml.common.input.parameter.MLAlgoParams;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters;
import org.opensearch.ml.common.input.parameter.textembedding.SparseEmbeddingFormat;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.SparseEncodingProcessorFactory;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldMapper;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.transport.client.OpenSearchClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SparseEncodingProcessorTests extends InferenceProcessorTestCase {

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment environment;

    @Captor
    private ArgumentCaptor<TextInferenceRequest> inferenceRequestCaptor;

    @Mock
    private ClusterService mockClusterService;

    @InjectMocks
    private SparseEncodingProcessorFactory sparseEncodingProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private static final String VALUE1 = "value1";
    private static final String VALUE2 = "value2";
    private static final String KEY1_MAPPED = "key1Mapped";
    private static final String KEY2_MAPPED = "key2Mapped";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        TestsPrepareUtils.prepareSparseFieldUtilsClusterServiceMock(mockClusterService, Collections.emptyList(), settings);
        TestUtils.initializeEventStatsManager();
    }

    @SneakyThrows
    private SparseEncodingProcessor createInstance(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.FIELD_MAP_FIELD, ImmutableMap.of(KEY1, KEY1_MAPPED, KEY2, KEY2_MAPPED));
        config.put(SparseEncodingProcessor.SKIP_EXISTING, skipExisting);
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private SparseEncodingProcessor createInstance(int batchSize, boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.FIELD_MAP_FIELD, ImmutableMap.of(KEY1, KEY1_MAPPED, KEY2, KEY2_MAPPED));
        config.put(AbstractBatchingProcessor.BATCH_SIZE_FIELD, batchSize);
        config.put(SparseEncodingProcessor.SKIP_EXISTING, skipExisting);
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private SparseEncodingProcessor createNestedTypeInstance(boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.SKIP_EXISTING, skipExisting);
        config.put(
            SparseEncodingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of(KEY1, Map.of("test1", "test1_knn"), KEY2, Map.of("test4", "test4_knn"))
        );
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private SparseEncodingProcessor createInstance(PruneType pruneType, float pruneRatio, boolean skipExisting) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.FIELD_MAP_FIELD, ImmutableMap.of(KEY1, KEY1_MAPPED, KEY2, KEY2_MAPPED));
        config.put(SparseEncodingProcessor.SKIP_EXISTING, skipExisting);
        config.put("prune_type", pruneType.getValue());
        config.put("prune_ratio", pruneRatio);
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(false);

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    @SneakyThrows
    public void testExecute_whenInferenceTextListEmpty_SuccessWithoutAnyMap() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        SparseEncodingProcessorFactory sparseEncodingProcessorFactory = new SparseEncodingProcessorFactory(
            openSearchClient,
            accessor,
            environment,
            mockClusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of(KEY1, KEY1_MAPPED, KEY2, KEY2_MAPPED));
        SparseEncodingProcessor processor = (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        doThrow(new RuntimeException()).when(accessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_withListTypeInput_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, list1);
        sourceAndMetadata.put(KEY2, list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(false);

        List<Map<String, ?>> dataAsMapList = createMockMapResult(6);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_MLClientAccessorThrowFail_handlerFailure() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(false);

        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    @SneakyThrows
    public void testExecute_withMapTypeInput_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test4", "test5");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, map1);
        sourceAndMetadata.put(KEY2, map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createNestedTypeInstance(false);

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());

    }

    public void test_batchExecute_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(10);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

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
        SparseEncodingProcessor processor = createInstance(docCount, false);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

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

    @SuppressWarnings("unchecked")
    public void testExecute_withPruneConfig_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        SparseEncodingProcessor processor = createInstance(PruneType.MAX_RATIO, 0.5f, false);

        List<Map<String, ?>> dataAsMapList = Collections.singletonList(
            Map.of("response", Arrays.asList(ImmutableMap.of("hello", 1.0f, "world", 0.1f), ImmutableMap.of("test", 0.8f, "low", 0.4f)))
        );

        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);

        ArgumentCaptor<IngestDocument> docCaptor = ArgumentCaptor.forClass(IngestDocument.class);
        verify(handler).accept(docCaptor.capture(), isNull());

        IngestDocument processedDoc = docCaptor.getValue();
        Map<String, Float> first = (Map<String, Float>) processedDoc.getFieldValue(KEY1_MAPPED, Map.class);
        Map<String, Float> second = (Map<String, Float>) processedDoc.getFieldValue(KEY2_MAPPED, Map.class);

        assertNotNull(first);
        assertNotNull(second);

        assertTrue(first.containsKey("hello"));
        assertFalse(first.containsKey("world"));
        assertEquals(1.0f, first.get("hello"), 0.001f);

        assertTrue(second.containsKey("test"));
        assertTrue(second.containsKey("low"));
        assertEquals(0.8f, second.get("test"), 0.001f);
        assertEquals(0.4f, second.get("low"), 0.001f);
    }

    public void test_batchExecute_withPrune_successful() {
        SparseEncodingProcessor processor = createInstance(PruneType.MAX_RATIO, 0.5f, false);

        List<Map<String, ?>> mockMLResponse = Collections.singletonList(
            Map.of(
                "response",
                Arrays.asList(
                    ImmutableMap.of("token1", 1.0f, "token2", 0.3f, "token3", 0.8f),
                    ImmutableMap.of("token4", 0.9f, "token5", 0.2f, "token6", 0.7f)
                )
            )
        );

        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(mockMLResponse);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));

        Consumer<List<?>> resultHandler = mock(Consumer.class);
        Consumer<Exception> exceptionHandler = mock(Consumer.class);

        List<String> inferenceList = Arrays.asList("test1", "test2");
        processor.doBatchExecute(inferenceList, resultHandler, exceptionHandler);

        ArgumentCaptor<List<Map<String, Float>>> resultCaptor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCaptor.capture());
        verify(exceptionHandler, never()).accept(any());

        List<Map<String, Float>> processedResults = resultCaptor.getValue();

        assertEquals(2, processedResults.size());

        Map<String, Float> firstResult = processedResults.get(0);
        assertEquals(2, firstResult.size());
        assertTrue(firstResult.containsKey("token1"));
        assertTrue(firstResult.containsKey("token3"));
        assertFalse(firstResult.containsKey("token2"));

        Map<String, Float> secondResult = processedResults.get(1);
        assertEquals(2, secondResult.size());
        assertTrue(secondResult.containsKey("token4"));
        assertTrue(secondResult.containsKey("token6"));
        assertFalse(secondResult.containsKey("token5"));
    }

    public void testIngest_skip_existing_flag_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("_id", "1");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(true);
        mockUpdateDocument(ingestDocument);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_skip_existing_flag_null_id_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(true);
        mockUpdateDocument(ingestDocument);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), isNull(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_with_no_update_with_skip_existing_flag_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put(KEY1, VALUE1);
        ingestSourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(true);
        List<String> inferenceList = Arrays.asList(VALUE1, VALUE2);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata); // no change
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, null);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler); // update document
        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        Map key1insert = (Map) ingestDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        Map key1update = (Map) updateDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        Map key2insert = (Map) ingestDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        Map key2update = (Map) updateDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        assertEquals(((Number) key1insert.get("hello")).floatValue(), ((Number) key1update.get("hello")).floatValue(), 0.001f);
        assertEquals(((Number) key1insert.get("world")).floatValue(), ((Number) key1update.get("world")).floatValue(), 0.001f);
        assertEquals(((Number) key2insert.get("hello")).floatValue(), ((Number) key2update.get("hello")).floatValue(), 0.001f);
        assertEquals(((Number) key2insert.get("world")).floatValue(), ((Number) key2update.get("world")).floatValue(), 0.001f);
    }

    public void testExecute_with_update_with_skip_existing_flag_successful() {
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put(KEY1, VALUE1);
        ingestSourceAndMetadata.put(KEY2, VALUE2);
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(true);
        List<String> inferenceList = Arrays.asList(VALUE1, VALUE2);
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata); // updated
        updateSourceAndMetadata.put(KEY2, "newValue"); // updated
        List<String> filteredInferenceList = List.of("newValue");
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler); // update document
        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        Map key1insert = (Map) ingestDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        Map key1update = (Map) updateDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        Map key2insert = (Map) ingestDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        Map key2update = (Map) updateDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        assertEquals(((Number) key1insert.get("hello")).floatValue(), ((Number) key1update.get("hello")).floatValue(), 0.001f);
        assertEquals(((Number) key1insert.get("world")).floatValue(), ((Number) key1update.get("world")).floatValue(), 0.001f);
        assertTrue(key2insert.containsKey("hello"));
        assertTrue(key2insert.containsKey("world"));
        assertTrue(key2update.containsKey("hello"));
        assertTrue(key2update.containsKey("world"));
    }

    public void testExecute_withListTypeInput_no_update_skip_existing_flag_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put(KEY1, list1);
        ingestSourceAndMetadata.put(KEY2, list2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        SparseEncodingProcessor processor = createInstance(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, null);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        List key1insert = (List) ingestDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        List key1update = (List) updateDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        List key2insert = (List) ingestDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        List key2update = (List) updateDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        verifyEqualEmbeddingInMap(key1insert, key1update);
        verifyEqualEmbeddingInMap(key2insert, key2update);
    }

    public void testExecute_withListTypeInput_with_update_skip_existing_flag_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put(KEY1, list1);
        ingestSourceAndMetadata.put(KEY2, list2);
        List<String> inferenceList = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6");
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();

        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        updateSourceAndMetadata.put(KEY1, ImmutableList.of("test1", "newValue1", "newValue2"));
        updateSourceAndMetadata.put(KEY2, ImmutableList.of("newValue3", "test5", "test6"));
        List<String> filteredInferenceList = Arrays.asList("test1", "newValue1", "newValue2", "newValue3", "test5", "test6");
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        TextInferenceRequest updateRequest = TextInferenceRequest.builder()
            .modelId("mockModelId")
            .inputTexts(filteredInferenceList)
            .build();

        SparseEncodingProcessor processor = createInstance(true);
        mockUpdateDocument(ingestDocument);
        mockVectorCreation(ingestRequest, updateRequest);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        List key1insert = (List) ingestDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        List key1update = (List) updateDocument.getSourceAndMetadata().get(KEY1_MAPPED);
        List key2insert = (List) ingestDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        List key2update = (List) updateDocument.getSourceAndMetadata().get(KEY2_MAPPED);
        assertEquals(key1insert.size(), key1update.size());
        assertEquals(key2insert.size(), key2update.size());
    }

    public void testExecute_withMapTypeInput_no_update_skip_existing_flag_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test4", "test5");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put(KEY1, map1);
        ingestSourceAndMetadata.put(KEY2, map2);
        List<String> inferenceList = Arrays.asList("test2", "test5");
        TextInferenceRequest request = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createNestedTypeInstance(true);
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());

        mockUpdateDocument(ingestDocument);
        mockVectorCreation(request, null);

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        processor.execute(updateDocument, handler);

        verify(handler, times(2)).accept(any(IngestDocument.class), isNull());
        verify(openSearchClient, times(2)).execute(isA(GetAction.class), isA(GetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        assertEquals(request.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
        Map key1insert = (Map) ((Map) ingestDocument.getSourceAndMetadata().get(KEY1)).get("test1_knn");
        Map key1update = (Map) ((Map) updateDocument.getSourceAndMetadata().get(KEY1)).get("test1_knn");
        Map key2insert = (Map) ((Map) ingestDocument.getSourceAndMetadata().get(KEY2)).get("test4_knn");
        Map key2update = (Map) ((Map) updateDocument.getSourceAndMetadata().get(KEY2)).get("test4_knn");
        assertEquals(((Number) key1insert.get("hello")).floatValue(), ((Number) key1update.get("hello")).floatValue(), 0.001f);
        assertEquals(((Number) key1insert.get("world")).floatValue(), ((Number) key1update.get("world")).floatValue(), 0.001f);
        assertEquals(((Number) key2insert.get("hello")).floatValue(), ((Number) key2update.get("hello")).floatValue(), 0.001f);
        assertEquals(((Number) key2insert.get("world")).floatValue(), ((Number) key2update.get("world")).floatValue(), 0.001f);
    }

    public void testExecute_withMapTypeInput_with_update_skip_existing_flag_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test4", "test5");
        Map<String, Object> ingestSourceAndMetadata = new HashMap<>();
        ingestSourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        ingestSourceAndMetadata.put("_id", "1");
        ingestSourceAndMetadata.put(KEY1, map1);
        ingestSourceAndMetadata.put(KEY2, map2);
        List<String> inferenceList = Arrays.asList("test2", "test5");
        TextInferenceRequest ingestRequest = TextInferenceRequest.builder().modelId("mockModelId").inputTexts(inferenceList).build();
        IngestDocument ingestDocument = new IngestDocument(ingestSourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createNestedTypeInstance(true);
        Map<String, Object> updateSourceAndMetadata = deepCopy(ingestSourceAndMetadata);
        IngestDocument updateDocument = new IngestDocument(updateSourceAndMetadata, new HashMap<>());
        ((Map) updateSourceAndMetadata.get(KEY1)).put("test1", "newValue1");
        List<String> filteredInferenceList = Arrays.asList("newValue1");
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
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
        Map key1insert = (Map) ((Map) ingestDocument.getSourceAndMetadata().get(KEY1)).get("test1_knn");
        Map key1update = (Map) ((Map) updateDocument.getSourceAndMetadata().get(KEY1)).get("test1_knn");
        Map key2insert = (Map) ((Map) ingestDocument.getSourceAndMetadata().get(KEY2)).get("test4_knn");
        Map key2update = (Map) ((Map) updateDocument.getSourceAndMetadata().get(KEY2)).get("test4_knn");
        assertEquals(key1insert.size(), key1update.size());
        assertEquals(key2insert.size(), key2update.size());
    }

    public void test_batchExecute_no_update_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        List<IngestDocumentWrapper> updateDocumentWrappers = createIngestDocumentWrappers(docCount);
        SparseEncodingProcessor processor = createInstance(docCount, true);
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
                ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1),
                updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1)
            );
            assertEquals(
                ((Number) ((Map) ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).get("hello"))
                    .floatValue(),
                ((Number) ((Map) updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).get("hello"))
                    .floatValue(),
                0.001f
            );
            assertEquals(
                ((Number) ((Map) ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).get("world"))
                    .floatValue(),
                ((Number) ((Map) updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).get("world"))
                    .floatValue(),
                0.001f
            );
        }
        verify(openSearchClient, times(2)).execute(isA(MultiGetAction.class), isA(MultiGetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        assertEquals(ingestRequest.getInputTexts(), inferenceRequestCaptor.getValue().getInputTexts());
    }

    public void test_batchExecute_with_update_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        List<IngestDocumentWrapper> updateDocumentWrappers = createIngestDocumentWrappers(docCount, "newValue");
        SparseEncodingProcessor processor = createInstance(docCount, true);
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
            assertEquals(
                ((Map) ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).size(),
                ((Map) updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).size()
            );
            assertEquals(
                ((Map) ingestDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).size(),
                ((Map) updateDocumentWrappers.get(i).getIngestDocument().getSourceAndMetadata().get(KEY1_MAPPED)).size()
            );
        }
        verify(openSearchClient, times(2)).execute(isA(MultiGetAction.class), isA(MultiGetRequest.class), isA(ActionListener.class));
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(
            inferenceRequestCaptor.capture(),
            isNull(),
            isA(ActionListener.class)
        );
        List<TextInferenceRequest> requests = inferenceRequestCaptor.getAllValues();
        assertEquals(ingestRequest.getInputTexts(), requests.get(0).getInputTexts());
        assertEquals(updateRequest.getInputTexts(), requests.get(1).getInputTexts());
    }

    public void test_subBatchExecute_emptyIngestDocumentWrapper_successful() {
        List<IngestDocumentWrapper> ingestDocumentWrappers = Collections.emptyList();
        SparseEncodingProcessor processor = createInstance(false);
        Consumer resultHandler = mock(Consumer.class);
        processor.subBatchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
    }

    public void test_subBatchExecute_emptyInferenceList_successful() {
        List<IngestDocumentWrapper> ingestDocumentWrappers = new ArrayList<>();
        ingestDocumentWrappers.add(new IngestDocumentWrapper(0, 0, null, null));
        SparseEncodingProcessor processor = createInstance(false);
        Consumer resultHandler = mock(Consumer.class);
        processor.subBatchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
    }

    public void test_execute_emptyInferenceList_successful() {
        List<IngestDocumentWrapper> ingestDocumentWrappers = new ArrayList<>();
        ingestDocumentWrappers.add(new IngestDocumentWrapper(0, 0, null, null));
        SparseEncodingProcessor processor = createInstance(false);
        Consumer resultHandler = mock(Consumer.class);
        processor.subBatchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
    }

    public void testExecute_with_all_seismic_field() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        String seisType = SparseVectorFieldMapper.CONTENT_TYPE;
        mockSeismic(KEY1_MAPPED, seisType, KEY2_MAPPED, seisType);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(false);

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
        ArgumentCaptor<MLAlgoParams> captor = ArgumentCaptor.forClass(MLAlgoParams.class);
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), captor.capture(), isA(ActionListener.class));
        verifyMLAlgoParam(captor);
    }

    public void testExecute_with_mixed_seismic() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        String seisType = SparseVectorFieldMapper.CONTENT_TYPE;
        mockSeismic(KEY1_MAPPED, seisType);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(false);

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler, times(1)).accept(any(IngestDocument.class), isNull());
        ArgumentCaptor<MLAlgoParams> captor = ArgumentCaptor.forClass(MLAlgoParams.class);
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), captor.capture(), isA(ActionListener.class));
        List<MLAlgoParams> params = captor.getAllValues();
        assertTrue(params.get(0) instanceof AsymmetricTextEmbeddingParameters);
        AsymmetricTextEmbeddingParameters asymmetricTextEmbeddingParameters = (AsymmetricTextEmbeddingParameters) params.get(0);
        assertEquals(SparseEmbeddingFormat.TOKEN_ID, asymmetricTextEmbeddingParameters.getSparseEmbeddingFormat());
        assertNull(params.get(1));
    }

    public void testExecute_with_mixed_seismic_exception() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put(KEY1, VALUE1);
        sourceAndMetadata.put(KEY2, VALUE2);
        String seisType = SparseVectorFieldMapper.CONTENT_TYPE;
        mockSeismic(KEY1_MAPPED, seisType);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance(false);

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doThrow(new RuntimeException()).doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        })
            .when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler, times(1)).accept(isNull(), any(Exception.class));
    }

    public void test_batchExecute_all_seismic() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(10);
        String seisType = SparseVectorFieldMapper.CONTENT_TYPE;
        mockSeismic(KEY1_MAPPED, seisType, KEY2_MAPPED, seisType);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler, times(1)).accept(resultCallback.capture());
        ArgumentCaptor<MLAlgoParams> captor = ArgumentCaptor.forClass(MLAlgoParams.class);
        verify(mlCommonsClientAccessor, times(1)).inferenceSentencesWithMapResult(any(), captor.capture(), any());
        verifyMLAlgoParam(captor);
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertNull(resultCallback.getValue().get(i).getException());
        }
    }

    public void test_batchExecute_mixed_seismic() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount, KEY1, VALUE1, KEY2, VALUE2);
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(10);
        String seisType = SparseVectorFieldMapper.CONTENT_TYPE;
        mockSeismic(KEY1_MAPPED, seisType);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler, times(1)).accept(resultCallback.capture());
        ArgumentCaptor<MLAlgoParams> captor = ArgumentCaptor.forClass(MLAlgoParams.class);
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), captor.capture(), any());
        List<MLAlgoParams> params = captor.getAllValues();
        assertTrue(params.get(0) instanceof AsymmetricTextEmbeddingParameters);
        AsymmetricTextEmbeddingParameters asymmetricTextEmbeddingParameters = (AsymmetricTextEmbeddingParameters) params.get(0);
        assertEquals(SparseEmbeddingFormat.TOKEN_ID, asymmetricTextEmbeddingParameters.getSparseEmbeddingFormat());
        assertNull(params.get(1));
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertNull(resultCallback.getValue().get(i).getException());
        }
    }

    public void test_batchExecute_mixed_seismic_exception() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount, KEY1, VALUE1, KEY2, VALUE2);
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(10);
        String seisType = SparseVectorFieldMapper.CONTENT_TYPE;
        mockSeismic(KEY1_MAPPED, seisType);
        doThrow(new RuntimeException()).doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        })
            .when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler, times(1)).accept(resultCallback.capture());
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertTrue(resultCallback.getValue().get(i).getException() != null);
        }
    }

    public void test_doSubBatchExecute_emptyList() {
        SparseEncodingProcessor processor = createInstance(5, false);
        List<List<IngestDocumentWrapper>> consumer = new ArrayList<>();
        processor.doSubBatchExecute(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), consumer::add);
        assertEquals(1, consumer.size());
        assertTrue(consumer.getFirst().isEmpty());
    }

    public void test_doSubBatchExecute_noSeismicFields() {
        int docCount = 5;
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount, KEY1, VALUE1, KEY2, VALUE2);
        List<InferenceProcessor.DataForInference> dataForInferences = processor.getDataForInference(ingestDocumentWrappers);
        List<String> inferenceList = processor.constructInferenceTexts(dataForInferences);
        List<List<IngestDocumentWrapper>> consumer = new ArrayList<>();
        processor = spy(processor);
        processor.doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, consumer::add);
        verify(processor).doBatchExecute(any(), any(), any());
    }

    public void test_doSubBatchExecute_noSeismicFieldsAfterSplit() {
        int docCount = 5;
        mockSeismic("key3");
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount, KEY1, VALUE1, KEY2, VALUE2);
        List<InferenceProcessor.DataForInference> dataForInferences = processor.getDataForInference(ingestDocumentWrappers);
        List<String> inferenceList = processor.constructInferenceTexts(dataForInferences);
        List<List<IngestDocumentWrapper>> consumer = new ArrayList<>();
        processor = spy(processor);
        processor.doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, consumer::add);
        verify(processor).doBatchExecute(any(), any(), any());
        verify(mlCommonsClientAccessor).inferenceSentencesWithMapResult(any(), any(), any());
    }

    public void test_doSubBatchExecute_withMixed() {
        int docCount = 5;
        mockSeismic(KEY1_MAPPED);
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount, KEY1, VALUE1, KEY2, VALUE2);
        List<InferenceProcessor.DataForInference> dataForInferences = processor.getDataForInference(ingestDocumentWrappers);
        List<String> inferenceList = processor.constructInferenceTexts(dataForInferences);
        List<List<IngestDocumentWrapper>> consumer = new ArrayList<>();
        processor = spy(processor);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(5);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        processor.doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, consumer::add);
        verify(processor, never()).doBatchExecute(any(), any(), any());
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), any(), any());
        for (IngestDocumentWrapper wrapper : consumer.getFirst()) {
            assertNull(wrapper.getException());
        }
    }

    public void test_doSubBatchExecute_withMixed_oneThrowException() {
        int docCount = 5;
        mockSeismic(KEY1_MAPPED);
        SparseEncodingProcessor processor = createInstance(docCount, false);
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount, KEY1, VALUE1, KEY2, VALUE2);
        List<InferenceProcessor.DataForInference> dataForInferences = processor.getDataForInference(ingestDocumentWrappers);
        List<String> inferenceList = processor.constructInferenceTexts(dataForInferences);
        List<List<IngestDocumentWrapper>> consumer = new ArrayList<>();
        processor = spy(processor);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(5);
        doAnswer(invocation -> {
            MLAlgoParams mlAlgoParams = invocation.getArgument(1);
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            if (mlAlgoParams instanceof AsymmetricTextEmbeddingParameters asymmetricTextEmbeddingParameters) {
                if (asymmetricTextEmbeddingParameters.getSparseEmbeddingFormat() == SparseEmbeddingFormat.TOKEN_ID) {
                    listener.onFailure(new RuntimeException());
                    return null;
                }
            }
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(argThat(request -> request.getInputTexts() != null), any(), isA(ActionListener.class));

        processor.doSubBatchExecute(ingestDocumentWrappers, inferenceList, dataForInferences, consumer::add);
        verify(processor, never()).doBatchExecute(any(), any(), any());
        verify(mlCommonsClientAccessor, times(2)).inferenceSentencesWithMapResult(any(), any(), any());
        for (IngestDocumentWrapper wrapper : consumer.getFirst()) {
            assertNotNull(wrapper.getException());
        }
    }

    private List<Map<String, ?>> createMockMapResult(int number) {
        List<Map<String, Float>> mockSparseEncodingResult = new ArrayList<>();
        IntStream.range(0, number)
            .forEachOrdered(x -> mockSparseEncodingResult.add(ImmutableMap.of("hello", randomFloat(), "world", randomFloat())));

        List<Map<String, ?>> mockMapResult = Collections.singletonList(Map.of("response", mockSparseEncodingResult));
        return mockMapResult;
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

    private void mockVectorCreation(TextInferenceRequest ingestRequest, TextInferenceRequest updateRequest) {
        doAnswer(invocation -> {
            int numVectors = ingestRequest.getInputTexts().size();
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(createMockMapResult(numVectors));
            return null;
        }).doAnswer(invocation -> {
            int numVectors = updateRequest.getInputTexts().size();
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(createMockMapResult(numVectors));
            return null;
        })
            .when(mlCommonsClientAccessor)
            .inferenceSentencesWithMapResult(isA(TextInferenceRequest.class), isNull(), isA(ActionListener.class));
    }

    private void verifyEqualEmbeddingInMap(List<Map> insertVectors, List<Map> updateVectors) {
        assertEquals(insertVectors.size(), updateVectors.size());

        for (int i = 0; i < insertVectors.size(); i++) {
            Map<String, Map<String, Number>> insertMap = insertVectors.get(i);
            Map<String, Map<String, Number>> updateMap = updateVectors.get(i);
            for (Map.Entry<String, Map<String, Number>> entry : insertMap.entrySet()) {
                Map<String, Number> insertValue = entry.getValue();
                Map<String, Number> updateValue = updateMap.get(entry.getKey());
                for (String key : insertValue.keySet()) {
                    assertEquals(insertValue.get(key).floatValue(), updateValue.get(key).floatValue(), 0.001f);
                }
            }
        }
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

    private void verifyMLAlgoParam(ArgumentCaptor<MLAlgoParams> captor) {
        MLAlgoParams params = captor.getValue();
        assertTrue(params instanceof AsymmetricTextEmbeddingParameters);
        AsymmetricTextEmbeddingParameters asymmetricTextEmbeddingParameters = (AsymmetricTextEmbeddingParameters) params;
        assertEquals(SparseEmbeddingFormat.TOKEN_ID, asymmetricTextEmbeddingParameters.getSparseEmbeddingFormat());
    }

    private void mockSeismic(String... fieldValues) {
        TestsPrepareUtils.prepareSparseFieldUtilsClusterServiceMock(mockClusterService, List.of(fieldValues), null);
    }
}
