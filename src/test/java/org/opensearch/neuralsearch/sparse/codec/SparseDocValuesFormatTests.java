/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseDocValuesFormatTests extends AbstractSparseTestBase {

    private DocValuesFormat mockDelegate;
    private SparseDocValuesFormat sparseDocValuesFormat;
    private SegmentWriteState mockWriteState;
    private SegmentReadState mockReadState;

    @Override
    public void setUp() {
        super.setUp();
        mockDelegate = mock(DocValuesFormat.class);
        when(mockDelegate.getName()).thenReturn("TestFormat");

        this.sparseDocValuesFormat = new SparseDocValuesFormat(mockDelegate);
        mockWriteState = mock(SegmentWriteState.class);
        mockReadState = mock(SegmentReadState.class);
    }

    public void testConstructor() {
        SparseDocValuesFormat sparseDocValuesFormat = new SparseDocValuesFormat(mockDelegate);
        // Verify that the format name is inherited from delegate
        assertEquals("TestFormat", sparseDocValuesFormat.getName());
    }

    public void testFieldsConsumer() throws IOException {
        // Setup
        DocValuesConsumer mockDelegateConsumer = mock(DocValuesConsumer.class);
        when(mockDelegate.fieldsConsumer(mockWriteState)).thenReturn(mockDelegateConsumer);

        // Execute
        DocValuesConsumer result = sparseDocValuesFormat.fieldsConsumer(mockWriteState);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparseDocValuesConsumer);
        verify(mockDelegate).fieldsConsumer(mockWriteState);
    }

    public void testFieldsProducer() throws IOException {
        // Setup
        DocValuesProducer mockDelegateProducer = mock(DocValuesProducer.class);
        when(mockDelegate.fieldsProducer(mockReadState)).thenReturn(mockDelegateProducer);

        // Execute
        DocValuesProducer result = sparseDocValuesFormat.fieldsProducer(mockReadState);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparseDocValuesProducer);
        verify(mockDelegate).fieldsProducer(mockReadState);
    }

    public void testFieldsConsumerIOException() throws IOException {
        // Setup
        IOException expectedException = new IOException("Test exception");
        when(mockDelegate.fieldsConsumer(mockWriteState)).thenThrow(expectedException);

        // Execute & Verify
        IOException exception = expectThrows(IOException.class, () -> { sparseDocValuesFormat.fieldsConsumer(mockWriteState); });
        assertEquals("Test exception", exception.getMessage());
    }

    public void testFieldsProducerIOException() throws IOException {
        // Setup
        IOException expectedException = new IOException("Test exception");
        when(mockDelegate.fieldsProducer(mockReadState)).thenThrow(expectedException);

        // Execute & Verify
        IOException exception = expectThrows(IOException.class, () -> { sparseDocValuesFormat.fieldsProducer(mockReadState); });
        assertEquals("Test exception", exception.getMessage());
    }
}
