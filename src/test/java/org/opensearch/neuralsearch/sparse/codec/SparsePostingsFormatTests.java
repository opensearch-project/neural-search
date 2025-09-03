/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparsePostingsFormatTests extends AbstractSparseTestBase {

    private PostingsFormat mockDelegate;
    private SparsePostingsFormat sparsePostingsFormat;
    private SegmentWriteState mockWriteState;
    private SegmentReadState mockReadState;

    @Override
    public void setUp() {
        super.setUp();
        mockDelegate = mock(PostingsFormat.class);
        when(mockDelegate.getName()).thenReturn("TestPostingsFormat");

        this.sparsePostingsFormat = new SparsePostingsFormat(mockDelegate);

        // Use TestsPrepareUtils to create real objects since SegmentInfo.name is final
        mockWriteState = TestsPrepareUtils.prepareSegmentWriteState();

        mockReadState = mock(SegmentReadState.class);
    }

    public void testConstructor() {
        SparsePostingsFormat sparsePostingsFormat = new SparsePostingsFormat(mockDelegate);
        // Verify that the format name is inherited from delegate
        assertEquals("TestPostingsFormat", sparsePostingsFormat.getName());
    }

    @SneakyThrows
    public void testFieldsConsumer() {
        // Setup
        FieldsConsumer mockDelegateConsumer = mock(FieldsConsumer.class);
        when(mockDelegate.fieldsConsumer(mockWriteState)).thenReturn(mockDelegateConsumer);

        // Execute
        FieldsConsumer result = this.sparsePostingsFormat.fieldsConsumer(mockWriteState);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparsePostingsConsumer);
        verify(mockDelegate).fieldsConsumer(mockWriteState);
    }

    @SneakyThrows
    public void testFieldsConsumerIOException() {
        // Setup
        IOException expectedException = new IOException("Test exception");
        when(mockDelegate.fieldsConsumer(mockWriteState)).thenThrow(expectedException);

        // Execute & Verify
        IOException exception = expectThrows(IOException.class, () -> { this.sparsePostingsFormat.fieldsConsumer(mockWriteState); });
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testFieldsProducer() {
        // Setup
        FieldsProducer mockDelegateProducer = mock(FieldsProducer.class);
        when(mockDelegate.fieldsProducer(mockReadState)).thenReturn(mockDelegateProducer);

        // Execute
        FieldsProducer result = this.sparsePostingsFormat.fieldsProducer(mockReadState);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparsePostingsProducer);
        verify(mockDelegate).fieldsProducer(mockReadState);
    }

    @SneakyThrows
    public void testFieldsProducerIOException() {
        // Setup
        IOException expectedException = new IOException("Test exception");
        when(mockDelegate.fieldsProducer(mockReadState)).thenThrow(expectedException);

        // Execute & Verify
        IOException exception = expectThrows(IOException.class, () -> { this.sparsePostingsFormat.fieldsProducer(mockReadState); });
        assertEquals("Test exception", exception.getMessage());
    }
}
