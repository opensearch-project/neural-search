/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceProcessorTests extends InferenceProcessorTestCase {
    private MLCommonsClientAccessor clientAccessor;
    private Environment environment;

    private static final String TAG = "tag";
    private static final String TYPE = "type";
    private static final String DESCRIPTION = "description";
    private static final String MAP_KEY = "map_key";
    private static final String MODEL_ID = "model_id";
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
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), null);
        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(Collections.emptyList(), resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> captor = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(captor.capture());
        assertTrue(captor.getValue().isEmpty());
        verify(clientAccessor, never()).inferenceSentences(anyString(), anyList(), any());
    }

    public void test_batchExecute_allFailedValidation() {
        final int docCount = 2;
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), null);
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
            assertEquals(wrapperList.get(i).getIngestDocument(), captor.getValue().get(i).getIngestDocument());
        }
        verify(clientAccessor, never()).inferenceSentences(anyString(), anyList(), any());
    }

    public void test_batchExecute_partialFailedValidation() {
        final int docCount = 2;
        TestInferenceProcessor processor = new TestInferenceProcessor(createMockVectorResult(), null);
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
        ArgumentCaptor<List<String>> inferenceTextCaptor = ArgumentCaptor.forClass(List.class);
        verify(clientAccessor).inferenceSentences(anyString(), inferenceTextCaptor.capture(), any());
        assertEquals(2, inferenceTextCaptor.getValue().size());
    }

    public void test_batchExecute_happyCase() {
        final int docCount = 2;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, null);
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
        ArgumentCaptor<List<String>> inferenceTextCaptor = ArgumentCaptor.forClass(List.class);
        verify(clientAccessor).inferenceSentences(anyString(), inferenceTextCaptor.capture(), any());
        assertEquals(4, inferenceTextCaptor.getValue().size());
    }

    public void test_doBatchExecute_exception() {
        final int docCount = 2;
        List<List<Float>> inferenceResults = createMockVectorWithLength(6);
        TestInferenceProcessor processor = new TestInferenceProcessor(inferenceResults, new RuntimeException());
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
        verify(clientAccessor).inferenceSentences(anyString(), anyList(), any());
    }

    private class TestInferenceProcessor extends InferenceProcessor {
        List<?> vectors;
        Exception exception;

        public TestInferenceProcessor(List<?> vectors, Exception exception) {
            super(TAG, DESCRIPTION, TYPE, MAP_KEY, MODEL_ID, FIELD_MAP, clientAccessor, environment);
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
            clientAccessor.inferenceSentences(MODEL_ID, inferenceList, ActionListener.wrap(results -> {}, ex -> {}));
            if (this.exception != null) {
                onException.accept(this.exception);
            } else {
                handler.accept(this.vectors);
            }
        }
    }
}
