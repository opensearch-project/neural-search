/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseDocValuesProducerTests extends AbstractSparseTestBase {

    private DocValuesProducer mockDelegate;
    private SegmentReadState segmentReadState;
    private SparseDocValuesProducer producer;
    private FieldInfo fieldInfo;
    private SegmentInfo segmentInfo;

    @Override
    public void setUp() {
        super.setUp();
        mockDelegate = mock(DocValuesProducer.class);

        // Mock components for SegmentReadState
        Directory mockDir = mock(Directory.class);
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        FieldInfos mockFieldInfos = mock(FieldInfos.class);
        IOContext ioContext = IOContext.DEFAULT;

        // Create a real SegmentReadState with mocked components
        segmentReadState = new SegmentReadState(
            mockDir,         // directory
            segmentInfo,     // segmentInfo
            mockFieldInfos,  // fieldInfos
            ioContext        // context
        );

        producer = new SparseDocValuesProducer(segmentReadState, mockDelegate);
        fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
    }

    public void testGetNumeric() throws IOException {
        // Setup
        NumericDocValues mockNumeric = mock(NumericDocValues.class);
        when(mockDelegate.getNumeric(fieldInfo)).thenReturn(mockNumeric);

        // Execute
        NumericDocValues result = producer.getNumeric(fieldInfo);

        // Verify
        assertEquals(mockNumeric, result);
        verify(mockDelegate).getNumeric(fieldInfo);
    }

    public void testGetBinary() throws IOException {
        // Setup
        BinaryDocValues mockBinary = mock(BinaryDocValues.class);
        when(mockDelegate.getBinary(fieldInfo)).thenReturn(mockBinary);

        // Execute
        BinaryDocValues result = producer.getBinary(fieldInfo);

        // Verify
        assertTrue(result instanceof SparseBinaryDocValuesPassThrough);
        SparseBinaryDocValuesPassThrough passThrough = (SparseBinaryDocValuesPassThrough) result;
        assertEquals(segmentReadState.segmentInfo, passThrough.getSegmentInfo());
        verify(mockDelegate).getBinary(fieldInfo);
    }

    public void testGetSorted() throws IOException {
        // Setup
        SortedDocValues mockSorted = mock(SortedDocValues.class);
        when(mockDelegate.getSorted(fieldInfo)).thenReturn(mockSorted);

        // Execute
        SortedDocValues result = producer.getSorted(fieldInfo);

        // Verify
        assertEquals(mockSorted, result);
        verify(mockDelegate).getSorted(fieldInfo);
    }

    public void testGetSortedNumeric() throws IOException {
        // Setup
        SortedNumericDocValues mockSortedNumeric = mock(SortedNumericDocValues.class);
        when(mockDelegate.getSortedNumeric(fieldInfo)).thenReturn(mockSortedNumeric);

        // Execute
        SortedNumericDocValues result = producer.getSortedNumeric(fieldInfo);

        // Verify
        assertEquals(mockSortedNumeric, result);
        verify(mockDelegate).getSortedNumeric(fieldInfo);
    }

    public void testGetSortedSet() throws IOException {
        // Setup
        SortedSetDocValues mockSortedSet = mock(SortedSetDocValues.class);
        when(mockDelegate.getSortedSet(fieldInfo)).thenReturn(mockSortedSet);

        // Execute
        SortedSetDocValues result = producer.getSortedSet(fieldInfo);

        // Verify
        assertEquals(mockSortedSet, result);
        verify(mockDelegate).getSortedSet(fieldInfo);
    }

    public void testGetSkipper() throws IOException {
        // Setup
        DocValuesSkipper mockSkipper = mock(DocValuesSkipper.class);
        when(mockDelegate.getSkipper(fieldInfo)).thenReturn(mockSkipper);

        // Execute
        DocValuesSkipper result = producer.getSkipper(fieldInfo);

        // Verify
        assertEquals(mockSkipper, result);
        verify(mockDelegate).getSkipper(fieldInfo);
    }

    public void testCheckIntegrity() throws IOException {
        // Execute
        producer.checkIntegrity();

        // Verify
        verify(mockDelegate).checkIntegrity();
    }

    public void testClose() throws IOException {
        // Execute
        producer.close();

        // Verify
        verify(mockDelegate).close();
    }

    public void testGetState() {
        // Execute
        SegmentReadState result = producer.getState();

        // Verify
        assertEquals(segmentReadState, result);
    }
}
