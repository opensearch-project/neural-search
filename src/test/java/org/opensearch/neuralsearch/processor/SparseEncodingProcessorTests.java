/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.any;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
import org.opensearch.neuralsearch.processor.factory.SparseEncodingProcessorFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.util.prune.PruneType;

public class SparseEncodingProcessorTests extends InferenceProcessorTestCase {
    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment environment;

    private ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

    @InjectMocks
    private SparseEncodingProcessorFactory sparseEncodingProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(clusterService.state().metadata().index(anyString()).getSettings()).thenReturn(settings);
    }

    @SneakyThrows
    private SparseEncodingProcessor createInstance() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private SparseEncodingProcessor createInstance(int batchSize) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        config.put(AbstractBatchingProcessor.BATCH_SIZE_FIELD, batchSize);
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private SparseEncodingProcessor createInstance(PruneType pruneType, float pruneRatio) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(SparseEncodingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        config.put("prune_type", pruneType.getValue());
        config.put("prune_ratio", pruneRatio);
        return (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance();

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

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
            accessor,
            environment,
            clusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        SparseEncodingProcessor processor = (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );
        doThrow(new RuntimeException()).when(accessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));
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
        SparseEncodingProcessor processor = createInstance();

        List<Map<String, ?>> dataAsMapList = createMockMapResult(6);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_MLClientAccessorThrowFail_handlerFailure() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        SparseEncodingProcessor processor = createInstance();

        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

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
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(SparseEncodingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            SparseEncodingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of("key1", Map.of("test1", "test1_knn"), "key2", Map.of("test4", "test4_knn"))
        );
        SparseEncodingProcessor processor = (SparseEncodingProcessor) sparseEncodingProcessorFactory.create(
            registry,
            PROCESSOR_TAG,
            DESCRIPTION,
            config
        );

        List<Map<String, ?>> dataAsMapList = createMockMapResult(2);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());

    }

    public void test_batchExecute_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        SparseEncodingProcessor processor = createInstance(docCount);
        List<Map<String, ?>> dataAsMapList = createMockMapResult(10);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

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
        SparseEncodingProcessor processor = createInstance(docCount);
        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

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
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());

        SparseEncodingProcessor processor = createInstance(PruneType.MAX_RATIO, 0.5f);

        List<Map<String, ?>> dataAsMapList = Collections.singletonList(
            Map.of("response", Arrays.asList(ImmutableMap.of("hello", 1.0f, "world", 0.1f), ImmutableMap.of("test", 0.8f, "low", 0.4f)))
        );

        doAnswer(invocation -> {
            ActionListener<List<Map<String, ?>>> listener = invocation.getArgument(2);
            listener.onResponse(dataAsMapList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);

        ArgumentCaptor<IngestDocument> docCaptor = ArgumentCaptor.forClass(IngestDocument.class);
        verify(handler).accept(docCaptor.capture(), isNull());

        IngestDocument processedDoc = docCaptor.getValue();
        Map<String, Float> first = (Map<String, Float>) processedDoc.getFieldValue("key1Mapped", Map.class);
        Map<String, Float> second = (Map<String, Float>) processedDoc.getFieldValue("key2Mapped", Map.class);

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
        SparseEncodingProcessor processor = createInstance(PruneType.MAX_RATIO, 0.5f);

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
        }).when(mlCommonsClientAccessor).inferenceSentencesWithMapResult(anyString(), anyList(), isA(ActionListener.class));

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

    private List<Map<String, ?>> createMockMapResult(int number) {
        List<Map<String, Float>> mockSparseEncodingResult = new ArrayList<>();
        IntStream.range(0, number).forEachOrdered(x -> mockSparseEncodingResult.add(ImmutableMap.of("hello", 1.0f, "world", 0.1f)));

        List<Map<String, ?>> mockMapResult = Collections.singletonList(Map.of("response", mockSparseEncodingResult));
        return mockMapResult;
    }
}
