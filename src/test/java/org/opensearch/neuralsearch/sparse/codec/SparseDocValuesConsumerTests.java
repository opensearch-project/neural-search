/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

/**
 * Covers the Lucene-engine sparse writer. The delegate fan-out and the engine
 * dispatch that used to live here now belong to {@link BaseSparseDocValuesConsumer}.
 */
public class SparseDocValuesConsumerTests extends AbstractSparseTestBase {
    @Mock
    private FieldInfo sparseFieldInfo;
    @Mock
    private FieldInfo nonSparseFieldInfo;
    @Mock
    private MergeHelper mockMergeHelper;
    @Mock
    private MergeStateFacade mockMergeStateFacade;
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

        FieldInfos fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.iterator()).thenReturn(List.of(sparseFieldInfo, nonSparseFieldInfo).iterator());
        when(mockMergeStateFacade.getMergeFieldInfos()).thenReturn(fieldInfos);
        when(mockMergeStateFacade.getFieldInfos()).thenReturn(new FieldInfos[] { fieldInfos });

        when(mockMergeHelper.newSparseDocValuesReader(any())).thenReturn(sparseDocValuesReader);
        when(sparseDocValuesReader.getBinary(any())).thenReturn(binaryDocValues);
        when(sparseDocValuesReader.getMergeStateFacade()).thenReturn(mockMergeStateFacade);
        when(binaryDocValues.nextDoc()).thenReturn(1, DocIdSetIterator.NO_MORE_DOCS);
        SparseVector vector = new SparseVector(Map.of(1, 0.1f, 2, 0.2f), new ByteQuantizer(3.0f));
        when(binaryDocValues.cachedSparseVector()).thenReturn(vector);

        docValuesProducer = mock(DocValuesProducer.class);
        cacheKey = prepareUniqueCacheKey(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, mockMergeHelper);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Clean up any created indices
        if (cacheKey != null) {
            ForwardIndexCache.getInstance().onIndexRemoval(cacheKey);
        }
        ForwardIndexCache.getInstance().onIndexRemoval(new CacheKey(segmentInfo, sparseFieldInfo));
        super.tearDown();
    }

    @SneakyThrows
    public void testAddBinaryField_NonSparseField() {
        sparseDocValuesConsumer.addBinaryField(nonSparseFieldInfo, docValuesProducer);

        // Should not even look at the values of a field that is not a sparse field
        verify(docValuesProducer, never()).getBinary(nonSparseFieldInfo);
        assertNull(ForwardIndexCache.getInstance().get(new CacheKey(segmentInfo, nonSparseFieldInfo)));
    }

    @SneakyThrows
    public void testAddBinaryField_SparseFieldBelowThreshold() {
        // Create new segmentInfo with lower maxDoc
        when(segmentInfo.maxDoc()).thenReturn(30);
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, mockMergeHelper);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Should not create forward index when below threshold
        assertNull(ForwardIndexCache.getInstance().get(cacheKey));
    }

    @SneakyThrows
    public void testAddBinaryField_SparseFieldAboveThreshold() {
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(0, 1, BinaryDocValues.NO_MORE_DOCS);
        when(binaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created and populated
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
        assertNotNull(index);

        // Verify vectors were inserted
        assertNotNull(index.getReader().read(0));
        assertNotNull(index.getReader().read(1));
    }

    @SneakyThrows
    public void testAddBinaryField_deserializesRatherThanReusingTheCachedVector() {
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Outside a merge the cached vector is not consulted, even when the producer
        // hands back a SparseBinaryDocValues that has one.
        SparseBinaryDocValues sparseBinaryDocValues = mock(SparseBinaryDocValues.class);
        when(sparseBinaryDocValues.nextDoc()).thenReturn(0, SparseBinaryDocValues.NO_MORE_DOCS);
        when(sparseBinaryDocValues.cachedSparseVector()).thenReturn(createVector(1, 2, 3, 4));
        when(sparseBinaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
        assertNotNull(index);
        assertNotNull(index.getReader().read(0));
        verify(sparseBinaryDocValues, never()).cachedSparseVector();
        verify(sparseBinaryDocValues).binaryValue();
    }

    @SneakyThrows
    public void testAddBinaryField_NoDocs() {
        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(BinaryDocValues.NO_MORE_DOCS);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(binaryDocValues).nextDoc();
    }

    @SneakyThrows
    public void testMerge_WithSparseField() {
        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        // A merge reuses the vector the reader already decoded
        verify(binaryDocValues).cachedSparseVector();
        verify(binaryDocValues, never()).binaryValue();
    }

    @SneakyThrows
    public void testMerge_WithSparseField_noCachedVector() {
        when(binaryDocValues.cachedSparseVector()).thenReturn(null);
        // Without a real encoded value the decode throws, which the merge now propagates
        when(binaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());

        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        verify(binaryDocValues).binaryValue();
    }

    @SneakyThrows
    public void testMerge_clearsCacheDataForTheMergedField() {
        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        verify(mockMergeHelper, times(1)).clearCacheData(eq(mockMergeStateFacade), eq(sparseFieldInfo), any());
    }

    @SneakyThrows
    public void testMerge_WithSparseField_notBinaryType() {
        when(sparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.NUMERIC);

        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        verify(mockMergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testMerge_WithNonSparseField() {
        sparseDocValuesConsumer.merge(List.of(nonSparseFieldInfo), mockMergeStateFacade);

        verify(mockMergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testMerge_WithEmptyFieldList() {
        sparseDocValuesConsumer.merge(List.of(), mockMergeStateFacade);

        verify(mockMergeHelper, never()).newSparseDocValuesReader(any());
    }

    /** A merge failure must reach Lucene, which is the only thing that can abort the merge. */
    @SneakyThrows
    public void testMerge_PropagatesException() {
        when(mockMergeHelper.newSparseDocValuesReader(any())).thenThrow(new RuntimeException("Test exception"));

        RuntimeException thrown = expectThrows(
            RuntimeException.class,
            () -> sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade)
        );

        assertEquals("Test exception", thrown.getMessage());
    }
}
