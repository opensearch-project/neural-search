/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseBinaryDocValuesTests extends AbstractSparseTestBase {

    private DocIDMerger<BinaryDocValuesSub> docIDMerger;
    private BinaryDocValuesSub binaryDocValuesSub;
    private BinaryDocValues binaryDocValues;
    private SparseBinaryDocValues sparseBinaryDocValues;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        docIDMerger = mock(DocIDMerger.class);
        binaryDocValuesSub = mock(BinaryDocValuesSub.class);
        binaryDocValues = mock(BinaryDocValues.class);
        sparseBinaryDocValues = new SparseBinaryDocValues(docIDMerger);
    }

    public void testInitialDocID() {
        assertEquals(-1, sparseBinaryDocValues.docID());
    }

    public void testNextDoc_WithNullCurrent() throws IOException {
        when(docIDMerger.next()).thenReturn(null);

        int result = sparseBinaryDocValues.nextDoc();

        assertEquals(SparseBinaryDocValues.NO_MORE_DOCS, result);
        assertEquals(SparseBinaryDocValues.NO_MORE_DOCS, sparseBinaryDocValues.docID());
    }

    public void testNextDoc_WithNullDocIDMerger() throws IOException {
        sparseBinaryDocValues = new SparseBinaryDocValues(null);
        int result = sparseBinaryDocValues.nextDoc();

        assertEquals(SparseBinaryDocValues.NO_MORE_DOCS, result);
        assertEquals(SparseBinaryDocValues.NO_MORE_DOCS, sparseBinaryDocValues.docID());
    }

    public void testAdvance() {
        expectThrows(UnsupportedOperationException.class, () -> sparseBinaryDocValues.advance(5));
    }

    public void testAdvanceExact() {
        expectThrows(UnsupportedOperationException.class, () -> sparseBinaryDocValues.advanceExact(5));
    }

    public void testCost() {
        expectThrows(UnsupportedOperationException.class, () -> sparseBinaryDocValues.cost());
    }

    public void testBinaryValue() throws IOException {
        BytesRef bytesRef = new BytesRef("test");
        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        when(binaryDocValuesSub.getValues()).thenReturn(binaryDocValues);
        when(binaryDocValues.binaryValue()).thenReturn(bytesRef);

        sparseBinaryDocValues.nextDoc();
        BytesRef result = sparseBinaryDocValues.binaryValue();

        assertEquals(bytesRef, result);
    }

    public void testCachedSparseVector_WithNullCurrent() throws IOException {
        assertNull(sparseBinaryDocValues.cachedSparseVector());
    }

    public void testCachedSparseVector_WithNullKey() throws IOException {
        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        when(binaryDocValuesSub.getKey()).thenReturn(null);

        sparseBinaryDocValues.nextDoc();
        SparseVector result = sparseBinaryDocValues.cachedSparseVector();

        assertNull(result);
    }

    public void testCachedSparseVector_WithNonNullKey() throws IOException {
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        CacheKey testKey = new CacheKey(segmentInfo, fieldInfo);
        when(binaryDocValuesSub.getKey()).thenReturn(testKey);
        sparseBinaryDocValues.nextDoc();
        SparseVector result = sparseBinaryDocValues.cachedSparseVector();

        assertNull(result); // index retrieved by this key is null, so it should return null
    }

    public void testCachedSparseVector_WithExistingIndex() throws IOException {
        SegmentInfo segmentInfo = mock(SegmentInfo.class);
        FieldInfo fieldInfo = mock(FieldInfo.class);
        CacheKey testKey = new CacheKey(segmentInfo, fieldInfo);

        // Create an actual index with some data
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().getOrCreate(testKey, 10);
        SparseVector testVector = createVector(1, 2, 3, 4);
        index.getWriter().insert(5, testVector);

        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        when(binaryDocValuesSub.getKey()).thenReturn(testKey);
        when(binaryDocValuesSub.getDocId()).thenReturn(5);

        sparseBinaryDocValues.nextDoc();
        SparseVector result = sparseBinaryDocValues.cachedSparseVector();

        assertEquals(testVector, result);

        // Clean up
        ForwardIndexCache.getInstance().removeIndex(testKey);
    }

    public void testCachedSparseVector_WithExistingIndexButNoVector() throws IOException {
        SegmentInfo segmentInfo = mock(SegmentInfo.class);
        FieldInfo fieldInfo = mock(FieldInfo.class);
        CacheKey testKey = new CacheKey(segmentInfo, fieldInfo);

        // Create an index but don't insert any data
        ForwardIndexCache.getInstance().getOrCreate(testKey, 10);

        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        when(binaryDocValuesSub.getKey()).thenReturn(testKey);
        when(binaryDocValuesSub.getDocId()).thenReturn(3);

        sparseBinaryDocValues.nextDoc();
        SparseVector result = sparseBinaryDocValues.cachedSparseVector();

        assertNull(result); // No vector at docId 3

        // Clean up
        ForwardIndexCache.getInstance().removeIndex(testKey);
    }

    public void testSetTotalLiveDocs() {
        SparseBinaryDocValues result = sparseBinaryDocValues.setTotalLiveDocs(100L);

        assertEquals(100L, sparseBinaryDocValues.getTotalLiveDocs());
        assertEquals(sparseBinaryDocValues, result);
    }
}
