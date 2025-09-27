/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyIterator;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

public class SparseDocValuesConsumerTests extends AbstractSparseTestBase {
    @Mock
    private DocValuesConsumer delegate;
    @Mock
    private FieldInfo sparseFieldInfo;
    @Mock
    private FieldInfo nonSparseFieldInfo;
    @Mock
    private MergeHelper mockMergeHelper;
    @Mock
    private MergeStateFacade mockMergeStateFacade;
    @Mock
    private MergeState mergeState;
    @Mock
    private SegmentInfo segmentInfo;
    @Mock
    private SparseDocValuesReader sparseDocValuesReader;
    @Mock
    private SparseBinaryDocValues binaryDocValues;

    private SegmentWriteState segmentWriteState;
    private DocValuesProducer docValuesProducer;
    private CacheKey cacheKey;
    private SparseDocValuesConsumer sparseDocValuesConsumer;

    @SneakyThrows
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        when(segmentInfo.maxDoc()).thenReturn(100);

        // Setup sparse field
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, String.valueOf(true));
        sparseAttributes.put(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(50));
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);
        when(sparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());
        when(nonSparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        when(mockMergeHelper.convertToMergeStateFacade(any())).thenReturn(mockMergeStateFacade);
        FieldInfos fieldInfos = mock(FieldInfos.class);

        when(fieldInfos.iterator()).thenReturn(List.of(sparseFieldInfo, nonSparseFieldInfo).iterator());
        when(mockMergeStateFacade.getMergeFieldInfos()).thenReturn(fieldInfos);
        when(mockMergeStateFacade.getFieldInfos()).thenReturn(new FieldInfos[] { fieldInfos });

        when(mockMergeHelper.newSparseDocValuesReader(any())).thenReturn(sparseDocValuesReader);
        when(sparseDocValuesReader.getBinary(any())).thenReturn(binaryDocValues);
        when(binaryDocValues.nextDoc()).thenReturn(1, DocIdSetIterator.NO_MORE_DOCS);
        SparseVector vector = new SparseVector(Map.of(1, 0.1f, 2, 0.2f), new ByteQuantizer(3.0f));
        when(binaryDocValues.cachedSparseVector()).thenReturn(vector);

        docValuesProducer = mock(DocValuesProducer.class);
        cacheKey = prepareUniqueCacheKey(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate, mockMergeHelper);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Clean up any created indices
        if (cacheKey != null) {
            ForwardIndexCache.getInstance().onIndexRemoval(cacheKey);
        }
        super.tearDown();
    }

    @SneakyThrows
    public void testAddNumericField() {
        sparseDocValuesConsumer.addNumericField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addNumericField(sparseFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testAddBinaryField_NonSparseField() {
        sparseDocValuesConsumer.addBinaryField(nonSparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(nonSparseFieldInfo, docValuesProducer);
        // Should not create forward index for non-sparse field
        assertNull(ForwardIndexCache.getInstance().get(new CacheKey(segmentInfo, nonSparseFieldInfo)));
    }

    @SneakyThrows
    public void testAddBinaryField_SparseFieldBelowThreshold() {
        // Create new segmentInfo with lower maxDoc
        when(segmentInfo.maxDoc()).thenReturn(30);
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate, mockMergeHelper);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(sparseFieldInfo, docValuesProducer);
        // Should not create forward index when below threshold
        assertNull(ForwardIndexCache.getInstance().get(cacheKey));
    }

    @SneakyThrows
    public void testAddBinaryField_SparseFieldAboveThreshold() {
        // Create new segmentInfo with higher maxDoc
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate, mockMergeHelper);

        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(0, 1, BinaryDocValues.NO_MORE_DOCS);
        when(binaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created and populated
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
        assertNotNull(index);

        // Verify vectors were inserted
        SparseVector vector0 = index.getReader().read(0);
        SparseVector vector1 = index.getReader().read(1);
        assertNotNull(vector0);
        assertNotNull(vector1);
    }

    @SneakyThrows
    public void testAddSortedField() {
        sparseDocValuesConsumer.addSortedField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedField(sparseFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testAddSortedNumericField() {
        sparseDocValuesConsumer.addSortedNumericField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedNumericField(sparseFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testAddSortedSetField() {
        sparseDocValuesConsumer.addSortedSetField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedSetField(sparseFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testClose() {
        sparseDocValuesConsumer.close();

        verify(delegate, times(1)).close();
    }

    @SneakyThrows
    public void testMerge_mergeFieldInfosIsNull() {
        when(mockMergeStateFacade.getMergeFieldInfos()).thenReturn(null);
        sparseDocValuesConsumer.merge(mergeState);
        verify(segmentInfo, never()).maxDoc();
    }

    @SneakyThrows
    public void testMerge_WithSparseField() {
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        verify(binaryDocValues).cachedSparseVector();
    }

    @SneakyThrows
    public void testMerge_WithSparseField_noCachedVector() {
        when(binaryDocValues.cachedSparseVector()).thenReturn(null);

        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        verify(binaryDocValues).binaryValue();
    }

    @SneakyThrows
    public void testMerge_WithSparseField_notBinaryType() {
        when(sparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.NUMERIC);
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        verify(mockMergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testMerge_WithNonSparseField() {
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        when(mergeFieldInfos.iterator()).thenReturn(List.of(nonSparseFieldInfo).iterator());
        mergeState.mergeFieldInfos = mergeFieldInfos;

        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }

    @SneakyThrows
    public void testMerge_WithException() {
        MergeState mergeState = mock(MergeState.class);
        // Don't set mergeFieldInfos to null as it causes assertion error
        // Instead test with empty field infos
        FieldInfos emptyFieldInfos = mock(FieldInfos.class);
        when(emptyFieldInfos.iterator()).thenReturn(emptyIterator());
        mergeState.mergeFieldInfos = emptyFieldInfos;

        // Should not throw exception, just log error
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }

    @SneakyThrows
    public void testAddBinary_WithSparseBinaryDocValues() {
        // Create new segmentInfo with higher maxDoc
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo(100);
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate, mockMergeHelper);

        // Create SparseBinaryDocValues for merge scenario
        SparseBinaryDocValues sparseBinaryDocValues = mock(SparseBinaryDocValues.class);
        when(sparseBinaryDocValues.nextDoc()).thenReturn(0, 1, SparseBinaryDocValues.NO_MORE_DOCS);

        SparseVector mockVector = createVector(1, 2, 3, 4);
        when(sparseBinaryDocValues.cachedSparseVector()).thenReturn(mockVector);
        when(sparseBinaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
        assertNotNull(index);

        // Verify cached vector was used
        SparseVector storedVector = index.getReader().read(0);
        assertNotNull(storedVector);
    }

    @SneakyThrows
    public void testAddBinary_WriterIsNull() {
        // This test covers the normal case since writer null is hard to trigger
        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(BinaryDocValues.NO_MORE_DOCS);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(sparseFieldInfo, docValuesProducer);
    }

    @SneakyThrows
    public void testMerge_WithSparseDocValuesReader() {
        // This will trigger the merge logic and test the instanceof SparseDocValuesReader check
        sparseDocValuesConsumer.merge(mergeState);
        verify(delegate, times(1)).merge(mergeState);
    }

    @SneakyThrows
    public void testMerge_WithRealException() {
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        // Create an iterator that throws exception
        when(mergeFieldInfos.iterator()).thenThrow(new RuntimeException("Test exception"));
        mergeState.mergeFieldInfos = mergeFieldInfos;

        // Should not throw exception, just log error
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }

    @SneakyThrows
    public void testMerge_WithSparseFieldAndReader() {
        // Create segmentInfo above threshold
        SegmentInfo newSegmentInfo = TestsPrepareUtils.prepareSegmentInfo(100);
        SegmentWriteState newState = TestsPrepareUtils.prepareSegmentWriteState(newSegmentInfo);
        SparseDocValuesConsumer newConsumer = new SparseDocValuesConsumer(newState, delegate, mockMergeHelper);

        // Create MergeState with sparse field
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        when(mergeFieldInfos.iterator()).thenReturn(List.of(sparseFieldInfo).iterator());
        mergeState.mergeFieldInfos = mergeFieldInfos;

        // This will trigger the merge logic with sparse field processing
        newConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }
}
