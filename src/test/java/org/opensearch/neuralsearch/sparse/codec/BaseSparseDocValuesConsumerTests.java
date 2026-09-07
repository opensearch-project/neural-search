/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyIterator;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

/**
 * Covers the Lucene-facing consumer: everything it forwards to the delegate, and
 * how it routes sparse fields to the Lucene or native writer by engine.
 */
public class BaseSparseDocValuesConsumerTests extends AbstractSparseTestBase {
    @Mock
    private DocValuesConsumer delegate;
    @Mock
    private SparseVectorBinaryConsumer luceneConsumer;
    @Mock
    private SparseVectorBinaryConsumer nativeConsumer;
    @Mock
    private MergeHelper mergeHelper;
    @Mock
    private MergeStateFacade mergeStateFacade;
    @Mock
    private MergeState mergeState;
    @Mock
    private FieldInfo luceneFieldInfo;
    @Mock
    private FieldInfo nativeFieldInfo;
    @Mock
    private FieldInfo nonSparseFieldInfo;

    private DocValuesProducer docValuesProducer;
    private BaseSparseDocValuesConsumer consumer;

    @SneakyThrows
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        stubSparseField(luceneFieldInfo, SparseEngine.LUCENE);
        stubSparseField(nativeFieldInfo, SparseEngine.NATIVE);
        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());
        when(nonSparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        when(mergeHelper.convertToMergeStateFacade(any())).thenReturn(mergeStateFacade);
        stubMergeFieldInfos(luceneFieldInfo, nativeFieldInfo, nonSparseFieldInfo);

        docValuesProducer = mock(DocValuesProducer.class);
        consumer = new BaseSparseDocValuesConsumer(delegate, luceneConsumer, nativeConsumer, mergeHelper);
    }

    private void stubSparseField(FieldInfo fieldInfo, SparseEngine engine) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(SPARSE_FIELD, String.valueOf(true));
        attributes.put(ENGINE_FIELD, engine.getName());
        when(fieldInfo.attributes()).thenReturn(attributes);
        when(fieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(engine.getName());
        when(fieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);
    }

    private void stubMergeFieldInfos(FieldInfo... fieldInfos) {
        FieldInfos infos = mock(FieldInfos.class);
        when(infos.iterator()).thenReturn(List.of(fieldInfos).iterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(infos);
    }

    @SneakyThrows
    public void testAddNumericField() {
        consumer.addNumericField(luceneFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addNumericField(luceneFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testAddSortedField() {
        consumer.addSortedField(luceneFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedField(luceneFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testAddSortedNumericField() {
        consumer.addSortedNumericField(luceneFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedNumericField(luceneFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testAddSortedSetField() {
        consumer.addSortedSetField(luceneFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedSetField(luceneFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testClose() {
        consumer.close();

        verify(delegate, times(1)).close();
    }

    @SneakyThrows
    public void testAddBinaryField_routesLuceneEngineToTheLuceneConsumer() {
        consumer.addBinaryField(luceneFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(luceneFieldInfo, docValuesProducer);
        verify(luceneConsumer, times(1)).addBinaryField(luceneFieldInfo, docValuesProducer);
        verify(nativeConsumer, never()).addBinaryField(any(), any());
    }

    @SneakyThrows
    public void testAddBinaryField_routesNativeEngineToTheNativeConsumer() {
        consumer.addBinaryField(nativeFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(nativeFieldInfo, docValuesProducer);
        verify(nativeConsumer, times(1)).addBinaryField(nativeFieldInfo, docValuesProducer);
        verify(luceneConsumer, never()).addBinaryField(any(), any());
    }

    @SneakyThrows
    public void testAddBinaryField_unsetEngineFallsBackToTheDefault() {
        // A field with no engine attribute resolves to SparseEngine.DEFAULT (Lucene).
        // Non-sparse fields are filtered out by the consumer itself, not here.
        consumer.addBinaryField(nonSparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(nonSparseFieldInfo, docValuesProducer);
        verify(luceneConsumer, times(1)).addBinaryField(nonSparseFieldInfo, docValuesProducer);
        verify(nativeConsumer, never()).addBinaryField(any(), any());
    }

    @SneakyThrows
    public void testMerge_partitionsSparseFieldsByEngine() {
        consumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        verify(luceneConsumer, times(1)).merge(eq(List.of(luceneFieldInfo)), eq(mergeStateFacade));
        verify(nativeConsumer, times(1)).merge(eq(List.of(nativeFieldInfo)), eq(mergeStateFacade));
    }

    @SneakyThrows
    public void testMerge_skipsAConsumerWithNoFieldsOfItsEngine() {
        stubMergeFieldInfos(luceneFieldInfo);

        consumer.merge(mergeState);

        verify(luceneConsumer, times(1)).merge(eq(List.of(luceneFieldInfo)), eq(mergeStateFacade));
        verify(nativeConsumer, never()).merge(any(), any());
    }

    @SneakyThrows
    public void testMerge_skipsNonBinaryFields() {
        when(luceneFieldInfo.getDocValuesType()).thenReturn(DocValuesType.NUMERIC);

        consumer.merge(mergeState);

        verify(luceneConsumer, never()).merge(any(), any());
        verify(nativeConsumer, times(1)).merge(eq(List.of(nativeFieldInfo)), eq(mergeStateFacade));
    }

    @SneakyThrows
    public void testMerge_mergeFieldInfosIsNull() {
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(null);

        consumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        verify(luceneConsumer, never()).merge(any(), any());
        verify(nativeConsumer, never()).merge(any(), any());
    }

    @SneakyThrows
    public void testMerge_noSparseFields() {
        stubMergeFieldInfos();

        consumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        verify(luceneConsumer, never()).merge(any(), any());
        verify(nativeConsumer, never()).merge(any(), any());
    }

    /**
     * A merge failure has to reach Lucene. Swallowing it leaves the sparse side files half written
     * while the merge reports success, so the commit publishes a segment that cannot be read back.
     */
    @SneakyThrows
    public void testMerge_PropagatesException() {
        FieldInfos infos = mock(FieldInfos.class);
        when(infos.iterator()).thenThrow(new RuntimeException("Test exception")).thenReturn(emptyIterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(infos);

        RuntimeException thrown = expectThrows(RuntimeException.class, () -> consumer.merge(mergeState));

        assertEquals("Test exception", thrown.getMessage());
        verify(delegate, times(1)).merge(mergeState);
        verify(luceneConsumer, never()).merge(any(), any());
        verify(nativeConsumer, never()).merge(any(), any());
    }
}
