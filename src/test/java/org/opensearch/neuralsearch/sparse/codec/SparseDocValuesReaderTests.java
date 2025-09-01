/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.Bits;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseDocValuesReaderTests extends OpenSearchTestCase {

    private SparseDocValuesReader sparseDocValuesReader;
    @Mock
    private FieldInfo fieldInfo;
    @Mock
    private FieldInfos fieldInfos;
    @Mock
    private MergeStateFacade mockMergeStateFacade;
    @Mock
    private DocValuesProducer mockDocValuesProducer;
    @Mock
    private SparseBinaryDocValuesPassThrough mockBinaryDocValues;
    @Mock
    private SparseBinaryDocValuesPassThrough mockBinaryDocValues2;
    @Mock
    private SegmentInfo mockSegmentInfo;
    @Mock
    private MergeState.DocMap mockDocMap;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        when(fieldInfo.getName()).thenReturn("test_field");
        when(mockMergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[] { mockDocValuesProducer });
        when(mockMergeStateFacade.getFieldInfos()).thenReturn(new FieldInfos[] { fieldInfos });
        when(fieldInfos.fieldInfo(anyString())).thenReturn(fieldInfo);
        when(fieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);
        when(mockDocValuesProducer.getBinary(any())).thenReturn(mockBinaryDocValues, mockBinaryDocValues2);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(mockSegmentInfo);
        Bits bits = mock(Bits.class);
        when(bits.get(anyInt())).thenReturn(true);
        when(mockMergeStateFacade.getLiveDocs()).thenReturn(new Bits[] { bits });
        when(mockBinaryDocValues.nextDoc()).thenReturn(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NO_MORE_DOCS);
        when(mockBinaryDocValues.docID()).thenReturn(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NO_MORE_DOCS);
        when(mockBinaryDocValues2.nextDoc()).thenReturn(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NO_MORE_DOCS);
        when(mockBinaryDocValues2.docID()).thenReturn(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NO_MORE_DOCS);
        when(mockMergeStateFacade.getDocMaps()).thenReturn(new MergeState.DocMap[] { mockDocMap });
        when(mockDocMap.get(eq(1))).thenReturn(1);
        when(mockDocMap.get(eq(2))).thenReturn(2);
        sparseDocValuesReader = new SparseDocValuesReader(mockMergeStateFacade);
    }

    public void testGetBinary_emptyDocValuesProducers() throws IOException {
        when(mockMergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[0]);
        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        assertEquals(NO_MORE_DOCS, result.nextDoc());
    }

    public void testGetBinary_nullDocValuesProducers() throws IOException {
        when(mockMergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[] { null });
        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        assertEquals(NO_MORE_DOCS, result.nextDoc());
    }

    public void testGetBinary_nullFieldInfo() throws IOException {
        when(fieldInfos.fieldInfo(anyString())).thenReturn(null);
        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        assertEquals(NO_MORE_DOCS, result.nextDoc());
        verify(fieldInfos).fieldInfo(anyString());
        verify(mockDocValuesProducer, never()).getBinary(any());
    }

    public void testGetBinary_getDocValuesTypeNotBinary() throws IOException {
        when(fieldInfo.getDocValuesType()).thenReturn(DocValuesType.NUMERIC);
        // Execute
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);

        assertEquals(NO_MORE_DOCS, result.nextDoc());
        verify(fieldInfo).getDocValuesType();
        verify(mockDocValuesProducer, never()).getBinary(any());
    }

    public void testGetBinary_binaryDocValuesTypeUnexpected() throws IOException {
        BinaryDocValues docValues = mock(BinaryDocValues.class);
        when(mockDocValuesProducer.getBinary(any())).thenReturn(docValues);
        when(docValues.nextDoc()).thenReturn(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NO_MORE_DOCS);
        when(docValues.docID()).thenReturn(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NO_MORE_DOCS);
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);
        verify(mockBinaryDocValues, never()).getSegmentInfo();
        verify(mockMergeStateFacade, times(2)).getLiveDocs();
    }

    public void testGetBinary_happyCase() throws IOException {
        BinaryDocValues result = sparseDocValuesReader.getBinary(fieldInfo);
        verify(mockBinaryDocValues).getSegmentInfo();
        verify(mockMergeStateFacade, times(2)).getLiveDocs();
        assertEquals(0, result.nextDoc());
    }

    public void testGetMergeState() {
        sparseDocValuesReader = new SparseDocValuesReader(mockMergeStateFacade);
        assertEquals(mockMergeStateFacade, sparseDocValuesReader.getMergeStateFacade());
    }
}
