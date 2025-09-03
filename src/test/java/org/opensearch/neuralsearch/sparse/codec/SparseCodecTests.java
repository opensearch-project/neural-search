/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseCodecTests extends AbstractSparseTestBase {

    @Mock
    private Codec mockDelegate;
    @Mock
    PostingsFormat mockPostingsFormat;
    @Mock
    DocValuesFormat mockDocValuesFormat;

    private SparseCodec sparseCodec;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        when(mockDelegate.postingsFormat()).thenReturn(mockPostingsFormat);
        when(mockDelegate.docValuesFormat()).thenReturn(mockDocValuesFormat);
        when(mockPostingsFormat.getName()).thenReturn("mockPostingsFormat");
        when(mockDocValuesFormat.getName()).thenReturn("mockDocValuesFormat");

        sparseCodec = new SparseCodec(mockDelegate);
    }

    public void testConstructor_withoutDelegate() {
        SparseCodec codec = new SparseCodec();

        assertNotNull(codec);
        assertEquals("Sparse10010Codec", codec.getName());
    }

    public void testConstructor_withDelegate() {
        SparseCodec codec = new SparseCodec(mockDelegate);

        assertNotNull(codec);
        assertEquals("Sparse10010Codec", codec.getName());
    }

    public void testDocValuesFormat_returnsDelegate() {
        DocValuesFormat result = sparseCodec.docValuesFormat();

        assertNotNull(result);
        assertTrue(result instanceof SparseDocValuesFormat);

        verify(mockDelegate, times(1)).docValuesFormat();
    }

    public void testPostingsFormat_returnsDelegate() {
        PostingsFormat result = sparseCodec.postingsFormat();

        assertNotNull(result);
        assertTrue(result instanceof SparsePostingsFormat);

        verify(mockDelegate, times(1)).postingsFormat();
    }
}
