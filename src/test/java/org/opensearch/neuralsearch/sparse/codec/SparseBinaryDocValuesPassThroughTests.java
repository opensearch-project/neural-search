/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseBinaryDocValuesPassThroughTests extends AbstractSparseTestBase {

    private BinaryDocValues mockDelegate;
    private SegmentInfo mockSegmentInfo;
    private SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        mockDelegate = mock(BinaryDocValues.class);
        mockSegmentInfo = mock(SegmentInfo.class);
        sparseBinaryDocValuesPassThrough = new SparseBinaryDocValuesPassThrough(mockDelegate, mockSegmentInfo, new ByteQuantizer(3.0f));
    }

    public void testConstructor_InitializesFieldsCorrectly() {
        SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough = new SparseBinaryDocValuesPassThrough(
            mockDelegate,
            mockSegmentInfo,
            new ByteQuantizer(3.0f)
        );

        assertSame(mockSegmentInfo, sparseBinaryDocValuesPassThrough.getSegmentInfo());
    }

    public void testGetSegmentInfo() {
        SegmentInfo result = sparseBinaryDocValuesPassThrough.getSegmentInfo();

        assertEquals(mockSegmentInfo, result);
    }

    public void testBinaryValue() throws IOException {
        BytesRef expectedBytesRef = new BytesRef("test_binary_value");
        when(mockDelegate.binaryValue()).thenReturn(expectedBytesRef);

        BytesRef result = sparseBinaryDocValuesPassThrough.binaryValue();

        assertEquals(expectedBytesRef, result);
        verify(mockDelegate, times(1)).binaryValue();
    }

    public void testAdvanceExact() throws IOException {
        int targetDoc = 1;
        when(mockDelegate.advanceExact(targetDoc)).thenReturn(true);

        boolean result = sparseBinaryDocValuesPassThrough.advanceExact(targetDoc);

        assertTrue(result);
        verify(mockDelegate, times(1)).advanceExact(targetDoc);
    }

    public void testDocID() {
        int expectedDocId = 1;
        when(mockDelegate.docID()).thenReturn(expectedDocId);

        int result = sparseBinaryDocValuesPassThrough.docID();

        assertEquals(expectedDocId, result);
        verify(mockDelegate, times(1)).docID();
    }

    public void testNextDoc() throws IOException {
        int expectedNextDoc = 1;
        when(mockDelegate.nextDoc()).thenReturn(expectedNextDoc);

        int result = sparseBinaryDocValuesPassThrough.nextDoc();

        assertEquals(expectedNextDoc, result);
        verify(mockDelegate, times(1)).nextDoc();
    }

    public void testAdvance() throws IOException {
        int targetDoc = 1;
        int expectedAdvancedDoc = 2;
        when(mockDelegate.advance(targetDoc)).thenReturn(expectedAdvancedDoc);

        int result = sparseBinaryDocValuesPassThrough.advance(targetDoc);

        assertEquals(expectedAdvancedDoc, result);
        verify(mockDelegate, times(1)).advance(targetDoc);
    }

    public void testCost() {
        long expectedCost = 1000L;
        when(mockDelegate.cost()).thenReturn(expectedCost);

        long result = sparseBinaryDocValuesPassThrough.cost();

        assertEquals(expectedCost, result);
        verify(mockDelegate, times(1)).cost();
    }

    public void testRead_WhenAdvanceExactReturnsFalse() throws IOException {
        int docId = 1;
        when(mockDelegate.advanceExact(docId)).thenReturn(false);

        SparseVector result = sparseBinaryDocValuesPassThrough.read(docId);

        assertNull(result);
        verify(mockDelegate, times(1)).advanceExact(docId);
        verify(mockDelegate, never()).binaryValue();
    }

    public void testRead_WhenBinaryValueReturnsNull() throws IOException {
        int docId = 1;
        when(mockDelegate.advanceExact(docId)).thenReturn(true);
        when(mockDelegate.binaryValue()).thenReturn(null);

        SparseVector result = sparseBinaryDocValuesPassThrough.read(docId);

        assertNull(result);
        verify(mockDelegate, times(1)).advanceExact(docId);
        verify(mockDelegate, times(1)).binaryValue();
    }

    public void testRead_WhenBinaryValueReturnsBytesRef() throws IOException {
        int docId = 42;
        BytesRef bytesRef = TestsPrepareUtils.prepareValidSparseVectorBytes();
        when(mockDelegate.advanceExact(docId)).thenReturn(true);
        when(mockDelegate.binaryValue()).thenReturn(bytesRef);

        SparseVector result = sparseBinaryDocValuesPassThrough.read(docId);

        assertNotNull(result);
        verify(mockDelegate, times(1)).advanceExact(docId);
        verify(mockDelegate, times(1)).binaryValue();
    }
}
