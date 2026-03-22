/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.Getter;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.neuralsearch.constants.TestCommonConstants;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.constants.TestCommonConstants.TEXT_INFERENCE_REQUEST;

public class InferenceProcessorTests extends InferenceProcessorTestCase {
    private MLCommonsClientAccessor clientAccessor;
    private Environment environment;

    @Mock
    private ClusterService mockClusterService;
    @Mock
    private ClusterState clusterState;
    @Mock
    private Metadata metadata;
    @Mock
    private IndexMetadata indexMetadata;

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
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.index(anyString())).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(null);
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
        verify(clientAccessor, never()).inferenceSentences(any(), any());
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
        verify(clientAccessor, never()).inferenceSentences(any(), any());
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
        ArgumentCaptor<TextInferenceRequest> inferenceRequest = ArgumentCaptor.forClass(TextInferenceRequest.class);
        verify(clientAccessor).inferenceSentences(inferenceRequest.capture(), any());
        assertEquals(2, inferenceRequest.getValue().getInputTexts().size());
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
        ArgumentCaptor<TextInferenceRequest> inferenceRequestArgumentCaptor = ArgumentCaptor.forClass(TextInferenceRequest.class);
        verify(clientAccessor).inferenceSentences(inferenceRequestArgumentCaptor.capture(), any());
        assertEquals(2, inferenceRequestArgumentCaptor.getValue().getInputTexts().size());
        assertEquals(TestCommonConstants.SENTENCES_LIST, inferenceRequestArgumentCaptor.getValue().getInputTexts());

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
        verify(clientAccessor).inferenceSentences(any(), any());
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

    public void test_splitInferenceListByBytes_singleBatch() {
        List<String> inferenceList = List.of("hello", "world");
        // 10 bytes total for ASCII, set limit high enough for one batch
        List<List<String>> result = InferenceProcessor.splitInferenceListByBytes(inferenceList, 1000);
        assertEquals(1, result.size());
        assertEquals(List.of("hello", "world"), result.get(0));
    }

    public void test_splitInferenceListByBytes_multipleBatches() {
        // "aaa" = 3 bytes, "bbb" = 3 bytes, "ccc" = 3 bytes, "ddd" = 3 bytes
        List<String> inferenceList = List.of("aaa", "bbb", "ccc", "ddd");
        // Set limit to 6 bytes: should produce batches of [aaa, bbb], [ccc, ddd]
        List<List<String>> result = InferenceProcessor.splitInferenceListByBytes(inferenceList, 6);
        assertEquals(2, result.size());
        assertEquals(List.of("aaa", "bbb"), result.get(0));
        assertEquals(List.of("ccc", "ddd"), result.get(1));
    }

    public void test_splitInferenceListByBytes_singleItemExceedsLimit() {
        // A single text that exceeds the byte limit should still be in its own batch (floor of 1)
        List<String> inferenceList = List.of("this_is_a_long_text", "short");
        List<List<String>> result = InferenceProcessor.splitInferenceListByBytes(inferenceList, 5);
        assertEquals(2, result.size());
        assertEquals(List.of("this_is_a_long_text"), result.get(0));
        assertEquals(List.of("short"), result.get(1));
    }

    public void test_splitInferenceListByBytes_exactBoundary() {
        // "abc" = 3 bytes each. Limit of 3 means each item gets its own batch
        List<String> inferenceList = List.of("abc", "def", "ghi");
        List<List<String>> result = InferenceProcessor.splitInferenceListByBytes(inferenceList, 3);
        assertEquals(3, result.size());
        assertEquals(List.of("abc"), result.get(0));
        assertEquals(List.of("def"), result.get(1));
        assertEquals(List.of("ghi"), result.get(2));
    }

    public void test_splitInferenceListByBytes_emptyList() {
        List<String> inferenceList = Collections.emptyList();
        List<List<String>> result = InferenceProcessor.splitInferenceListByBytes(inferenceList, 100);
        assertTrue(result.isEmpty());
    }

    public void test_splitInferenceListByBytes_utf8MultiByte() {
        // Unicode characters: "日本" = 6 bytes in UTF-8 (3 bytes per CJK character)
        List<String> inferenceList = List.of("日本", "語");
        // "日本" = 6 bytes, "語" = 3 bytes. Limit of 6 means first item fills a batch, second goes to next
        List<List<String>> result = InferenceProcessor.splitInferenceListByBytes(inferenceList, 6);
        assertEquals(2, result.size());
        assertEquals(List.of("日本"), result.get(0));
        assertEquals(List.of("語"), result.get(1));
    }

    public void test_batchExecute_byteSizeSubBatches() {
        final int docCount = 3;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        // batchSize=100 (high, won't limit), batchSizeBytes=10 (will split)
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, 100, 10, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        // Each "valueX" is 6 bytes. With batchSizeBytes=10, we can fit 1 per batch (6 > 10/2 but 6+6=12 > 10)
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", "value0");
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", "value1");
        wrapperList.get(2).getIngestDocument().setFieldValue("key1", "value2");
        List<IngestDocumentWrapper> allResults = new ArrayList<>();
        processor.batchExecute(wrapperList, allResults::addAll);
        // "value0"=6 bytes, "value1"=6 bytes, "value2"=6 bytes
        // With limit 10: batch1=[value0] (6 bytes), then value1 would make 12 > 10, so flush
        // After sort by length (all same length), batches should be:
        // [value0, value1] would be 12 > 10, so: [value0](6), [value1](6), [value2](6)
        // Actually: first item "value0" (6 bytes) fits. Second "value1" (6+6=12 > 10) triggers flush.
        // So: [value0], [value1], [value2] = 3 sub-batches
        assertEquals(3, processor.getAllInferenceInputs().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(wrapperList.get(i).getIngestDocument(), allResults.get(i).getIngestDocument());
        }
    }

    public void test_batchExecute_byteSizeDisabled() {
        final int docCount = 2;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        // batchSizeBytes=-1 means disabled, should behave like normal batch
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, 100, -1, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", "value0");
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", "value1");
        List<IngestDocumentWrapper> allResults = new ArrayList<>();
        processor.batchExecute(wrapperList, allResults::addAll);
        // All in one batch since batchSizeBytes is disabled
        assertEquals(1, processor.getAllInferenceInputs().size());
        assertEquals(2, processor.getAllInferenceInputs().get(0).size());
    }

    public void test_batchExecute_byteSizeLargeEnough() {
        final int docCount = 3;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        // batchSizeBytes large enough to fit all texts in one batch
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, 100, 100000, null);
        List<IngestDocumentWrapper> wrapperList = createIngestDocumentWrappers(docCount);
        wrapperList.get(0).getIngestDocument().setFieldValue("key1", "value0");
        wrapperList.get(1).getIngestDocument().setFieldValue("key1", "value1");
        wrapperList.get(2).getIngestDocument().setFieldValue("key1", "value2");
        List<IngestDocumentWrapper> allResults = new ArrayList<>();
        processor.batchExecute(wrapperList, allResults::addAll);
        // All in one batch
        assertEquals(1, processor.getAllInferenceInputs().size());
        assertEquals(3, processor.getAllInferenceInputs().get(0).size());
    }

    private class TestInferenceProcessor extends InferenceProcessor {
        List<?> vectors;
        Exception exception;

        @Getter
        List<List<String>> allInferenceInputs = new ArrayList<>();

        public TestInferenceProcessor(List<?> vectors, int batchSize, Exception exception) {
            super(TAG, DESCRIPTION, batchSize, TYPE, MAP_KEY, MODEL_ID, FIELD_MAP, clientAccessor, environment, mockClusterService);
            this.vectors = vectors;
            this.exception = exception;
        }

        public TestInferenceProcessor(List<?> vectors, int batchSize, int batchSizeBytes, Exception exception) {
            super(
                TAG,
                DESCRIPTION,
                batchSize,
                batchSizeBytes,
                TYPE,
                MAP_KEY,
                MODEL_ID,
                FIELD_MAP,
                clientAccessor,
                environment,
                mockClusterService
            );
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
        protected void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
            // use to verify if doBatchExecute is called from InferenceProcessor
            clientAccessor.inferenceSentences(TEXT_INFERENCE_REQUEST, ActionListener.wrap(results -> {}, ex -> {}));
            allInferenceInputs.add(inferenceList);
            if (this.exception != null) {
                onException.accept(this.exception);
            } else {
                handler.accept(this.vectors);
            }
        }
    }
}
