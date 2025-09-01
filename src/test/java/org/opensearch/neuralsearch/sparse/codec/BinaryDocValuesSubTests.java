/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.MergeState;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;

import java.io.IOException;

import static org.mockito.Mockito.when;

public class BinaryDocValuesSubTests extends AbstractSparseTestBase {

    @Mock
    private MergeState.DocMap mockDocMap;
    @Mock
    private BinaryDocValues mockBinaryDocValues;
    @Mock
    private CacheKey mockCacheKey;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testConstructor_success() {
        when(mockBinaryDocValues.docID()).thenReturn(-1);

        BinaryDocValuesSub sub = new BinaryDocValuesSub(mockDocMap, mockBinaryDocValues, mockCacheKey);

        assertNotNull(sub);
        assertEquals(mockBinaryDocValues, sub.getValues());
        assertEquals(mockCacheKey, sub.getKey());
        assertEquals(0, sub.getDocId());
    }

    public void testConstructor_withNullValues() {
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> new BinaryDocValuesSub(mockDocMap, null, mockCacheKey)
        );

        assertEquals("Doc values is either null or docID is not -1 ", exception.getMessage());
    }

    public void testConstructor_withInvalidDocID() {
        when(mockBinaryDocValues.docID()).thenReturn(5);

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> new BinaryDocValuesSub(mockDocMap, mockBinaryDocValues, mockCacheKey));

        assertEquals("Doc values is either null or docID is not -1 ", exception.getMessage());
    }

    @SneakyThrows
    public void testNextDoc_success() {
        when(mockBinaryDocValues.docID()).thenReturn(-1);
        when(mockBinaryDocValues.nextDoc()).thenReturn(10);

        BinaryDocValuesSub sub = new BinaryDocValuesSub(mockDocMap, mockBinaryDocValues, mockCacheKey);
        int result = sub.nextDoc();

        assertEquals(10, result);
        assertEquals(10, sub.getDocId());
    }

    @SneakyThrows
    public void testNextDoc_throwsIOException() {
        when(mockBinaryDocValues.docID()).thenReturn(-1);
        IOException expectedException = new IOException("Test exception");
        when(mockBinaryDocValues.nextDoc()).thenThrow(expectedException);

        BinaryDocValuesSub sub = new BinaryDocValuesSub(mockDocMap, mockBinaryDocValues, mockCacheKey);

        IOException exception = expectThrows(IOException.class, sub::nextDoc);
        assertEquals("Test exception", exception.getMessage());
    }
}
