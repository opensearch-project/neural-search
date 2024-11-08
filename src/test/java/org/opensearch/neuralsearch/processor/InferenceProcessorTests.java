/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.Getter;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor.InferenceRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceProcessorTests extends InferenceProcessorTestCase {
    private MLCommonsClientAccessor clientAccessor;
    private Environment environment;

    private ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

    private static final String TAG = "tag";
    private static final String TYPE = "type";
    private static final String DESCRIPTION = "description";
    private static final String MAP_KEY = "map_key";
    private static final String MODEL_ID = "model_id";
    private static final int BATCH_SIZE = 10;
    private static final Map<String, Object> FIELD_MAP = Map.of("key1", "embedding_key1", "key2", "embedding_key2");

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        clientAccessor = mock(MLCommonsClientAccessor.class);
        environment = mock(Environment.class);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(environment.settings()).thenReturn(settings);
    }

    public void test_batchExecute_emptyInput() {
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), BATCH_SIZE, null);
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(Collections.emptyList(), resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertTrue(captor.getValue().isEmpty());
        verify(clientAccessor, never()).inferenceSentencesMap(argThat(request -> request.getInputTexts() != null), any());
    }

    public void test_batchExecuteWithEmpty_allFailedValidation() {
        final int docCount = 2;
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), BATCH_SIZE, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", Arrays.asList("", "value1"));
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", Arrays.asList("", "value1"));
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(wrapperList, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertEquals(docCount, captor.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertNotNull(captor.getValue().get(i).getException());
            assertEquals(
                "list type field [key1] has empty string, cannot process it",
                captor.getValue().get(i).getException().getMessage()
            );
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        verify(clientAccessor, never()).inferenceSentencesMap(argThat(request -> request.getInputTexts() != null), any());
    }

    public void test_batchExecuteWithNull_allFailedValidation() {
        final int docCount = 2;
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), BATCH_SIZE, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", Arrays.asList(null, "value1"));
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", Arrays.asList(null, "value1"));
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(wrapperList, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertEquals(docCount, captor.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertNotNull(captor.getValue().get(i).getException());
            assertEquals("list type field [key1] has null, cannot process it", captor.getValue().get(i).getException().getMessage());
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        verify(clientAccessor, never()).inferenceSentencesMap(argThat(request -> request.getInputTexts() != null), any());
    }

    public void test_batchExecute_partialFailedValidation() {
        final int docCount = 2;
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), BATCH_SIZE, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", Arrays.asList("", "value1"));
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", Arrays.asList("value3", "value4"));
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(wrapperList, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertEquals(docCount, captor.getValue().size());
        assertNotNull(captor.getValue().get(0).getException());
        assertNull(captor.getValue().get(1).getException());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        ArgumentCaptor<InferenceRequest> inferenceRequestArgumentCaptor = ArgumentCaptor.forClass(InferenceRequest.class);
        verify(clientAccessor).inferenceSentences(inferenceRequestArgumentCaptor.capture(), any());
        assertEquals(2, inferenceRequestArgumentCaptor.getValue().getInputTexts().size());
    }

    public void test_batchExecute_happyCase() {
        final int docCount = 2;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, BATCH_SIZE, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", Arrays.asList("value1", "value2"));
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", Arrays.asList("value3", "value4"));
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(wrapperList, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertEquals(docCount, captor.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertNull(captor.getValue().get(i).getException());
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        ArgumentCaptor<InferenceRequest> inferenceRequestArgumentCaptor = ArgumentCaptor.forClass(InferenceRequest.class);
        verify(clientAccessor).inferenceSentences(inferenceRequestArgumentCaptor.capture(), any());
        assertEquals(4, inferenceRequestArgumentCaptor.getValue().getInputTexts().size());
    }

    public void test_batchExecute_sort() {
        final int docCount = 2;
        List<List<Float>> inferenceResults = createMockVectorWithLength(100);
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, BATCH_SIZE, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", Arrays.asList("aaaaa", "bbb"));
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", Arrays.asList("cc", "ddd"));
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(wrapperList, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertEquals(docCount, captor.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertNull(captor.getValue().get(i).getException());
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        ArgumentCaptor<InferenceRequest> inferenceRequestArgumentCaptor = ArgumentCaptor.forClass(InferenceRequest.class);
        verify(clientAccessor).inferenceSentences(inferenceRequestArgumentCaptor.capture(), any());
        assertEquals(4, inferenceRequestArgumentCaptor.getValue().getInputTexts().size());
        assertEquals(Arrays.asList("cc", "bbb", "ddd", "aaaaa"), inferenceRequestArgumentCaptor.getValue().getInputTexts());

        List<?> doc1Embeddings = (List) (captor.getValue().get(0).getIngestDocument().getFieldValue("embedding_key1", List.class));
        List<?> doc2Embeddings = (List) (captor.getValue().get(1).getIngestDocument().getFieldValue("embedding_key1", List.class));
        assertEquals(2, doc1Embeddings.size());
        assertEquals(2, doc2Embeddings.size());
        // inferenceResults are results for sorted-by-length array ("cc", "bbb", "ddd", "aaaaa")
        assertEquals(inferenceResults.get(3), ((Map) doc1Embeddings.get(0)).get("map_key"));
        assertEquals(inferenceResults.get(1), ((Map) doc1Embeddings.get(1)).get("map_key"));
        assertEquals(inferenceResults.get(0), ((Map) doc2Embeddings.get(0)).get("map_key"));
        assertEquals(inferenceResults.get(2), ((Map) doc2Embeddings.get(1)).get("map_key"));
    }

    public void test_doBatchExecute_exception() {
        final int docCount = 2;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, BATCH_SIZE, new RuntimeException());
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", Arrays.asList("value1", "value2"));
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", Arrays.asList("value3", "value4"));
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(wrapperList, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertEquals(docCount, captor.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertNotNull(captor.getValue().get(i).getException());
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        verify(clientAccessor).inferenceSentences(argThat(request -> request.getInputTexts() != null), any());
    }

    public void test_batchExecute_subBatches() {
        final int docCount = 5;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, 2, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        for (int i = 0; i < docCount; ++i) {
            wrapperList.get(i).getIngestDocument().setFieldValue("key1", Collections.singletonList("value" + i));
        }
        List<IngestDocumentWrapper> allResults = new ArrayList<>();
        processor.batchExecute(wrapperList, allResults::addAll);
        for (int i = 0; i < docCount; ++i) {
            assertEquals(allResults.get(i).getIngestDocument(), wrapperList.get(i).getIngestDocument());
            assertEquals(allResults.get(i).getSlot(), wrapperList.get(i).getSlot());
            assertEquals(allResults.get(i).getException(), wrapperList.get(i).getException());
        }
        assertEquals(3, processor.getAllInferenceInputs().size());
        assertEquals(List.of("value0", "value1"), processor.getAllInferenceInputs().get(0));
        assertEquals(List.of("value2", "value3"), processor.getAllInferenceInputs().get(1));
        assertEquals(List.of("value4"), processor.getAllInferenceInputs().get(2));
    }

    private class TestInferenceProcessor extends InferenceProcessor {
        List<?> vectors;
        Exception exception;

        @Getter
        List<List<String>> allInferenceInputs = new ArrayList<>();

        public TestInferenceProcessor(List<?> vectors, int batchSize, Exception exception) {
            super(TAG, DESCRIPTION, batchSize, TYPE, MAP_KEY, MODEL_ID, FIELD_MAP, clientAccessor, environment, clusterService);
            this.vectors = vectors;
            this.exception = exception;
        }

        @Override
        public void doExecute(
            IngestDocument ingestDocument,
            Map<String, Object> ProcessMap,
            List<String> inferenceList,
            BiConsumer<IngestDocument, Exception> handler
        ) {}

        @Override
        void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
            // use to verify if doBatchExecute is called from InferenceProcessor
            clientAccessor.inferenceSentences(
                InferenceRequest.builder().modelId(MODEL_ID).inputTexts(inferenceList).build(),
                ActionListener.wrap(results -> {}, ex -> {})
            );
            allInferenceInputs.add(inferenceList);
            if (this.exception != null) {
                onException.accept(this.exception);
            } else {
                handler.accept(this.vectors);
            }
        }
    }
}
