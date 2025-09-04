/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparsePostingsEnumTests extends AbstractSparseTestBase {

    @Mock
    private CacheKey mockCacheKey;
    @Mock
    private PostingClusters mockClusters;
    @Mock
    private DocumentCluster mockDocumentCluster;
    @Mock
    private DocWeightIterator mockDocWeightIterator;
    @Mock
    private IteratorWrapper<DocumentCluster> mockClusterIterator;

    private SparsePostingsEnum sparsePostingsEnum;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        when(mockClusters.iterator()).thenReturn(mockClusterIterator);
        when(mockClusterIterator.next()).thenReturn(mockDocumentCluster);
        when(mockClusterIterator.hasNext()).thenReturn(true);
        when(mockDocumentCluster.getDisi()).thenReturn(mockDocWeightIterator);
        when(mockDocWeightIterator.docID()).thenReturn(0);

        sparsePostingsEnum = new SparsePostingsEnum(mockClusters, mockCacheKey);
    }

    @SneakyThrows
    public void testConstructor() {
        SparsePostingsEnum sparsePostingsEnum = new SparsePostingsEnum(mockClusters, mockCacheKey);

        assertEquals(mockClusters, sparsePostingsEnum.getClusters());
        assertEquals(mockCacheKey, sparsePostingsEnum.getCacheKey());
    }

    public void testClusterIterator() {
        // reset mockClusters because setUp calls it
        reset(mockClusters);
        when(mockClusters.iterator()).thenReturn(mockClusterIterator);

        IteratorWrapper<DocumentCluster> result = sparsePostingsEnum.clusterIterator();

        verify(mockClusters, times(1)).iterator();
        assertEquals(mockClusterIterator, result);
    }

    public void testSize() {
        int expectedSize = 100;
        when(mockClusters.getSize()).thenReturn(expectedSize);

        assertEquals(expectedSize, sparsePostingsEnum.size());
        verify(mockClusters, times(1)).getSize();
    }

    public void testFreq() throws IOException {
        byte expectedWeight = 100;
        when(mockDocWeightIterator.weight()).thenReturn(expectedWeight);

        int result = sparsePostingsEnum.freq();

        verify(mockDocWeightIterator, times(1)).weight();
        assertEquals(ByteQuantizer.getUnsignedByte(expectedWeight), result);
    }

    public void testNextPosition() {
        Exception exception = expectThrows(UnsupportedOperationException.class, () -> sparsePostingsEnum.nextPosition());
        assertNull(exception.getMessage());
    }

    public void testStartOffset() {
        Exception exception = expectThrows(UnsupportedOperationException.class, () -> sparsePostingsEnum.startOffset());
        assertNull(exception.getMessage());

    }

    public void testEndOffset() {
        Exception exception = expectThrows(UnsupportedOperationException.class, () -> sparsePostingsEnum.endOffset());
        assertNull(exception.getMessage());
    }

    public void testGetPayload() {
        Exception exception = expectThrows(UnsupportedOperationException.class, () -> sparsePostingsEnum.getPayload());
        assertNull(exception.getMessage());
    }

    public void testDocID() {
        int expectedDocID = 10;
        when(mockDocWeightIterator.docID()).thenReturn(expectedDocID);

        assertEquals(expectedDocID, sparsePostingsEnum.docID());
        verify(mockDocWeightIterator, times(1)).docID();
    }

    @SneakyThrows
    public void testDocID_currentDocWeightNull() {
        when(mockClusters.iterator()).thenReturn(mockClusterIterator);
        when(mockClusterIterator.next()).thenReturn(mockDocumentCluster);
        when(mockClusterIterator.hasNext()).thenReturn(true);
        when(mockDocumentCluster.getDisi()).thenReturn(null);

        sparsePostingsEnum = new SparsePostingsEnum(mockClusters, mockCacheKey);

        // -1 is the default value when current doc weight is null
        assertEquals(-1, sparsePostingsEnum.docID());
        verify(mockDocWeightIterator, never()).docID();
    }

    public void testNextDoc_noMoreDocs() throws IOException {
        when(mockDocWeightIterator.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(mockClusterIterator.hasNext()).thenReturn(false);

        int result = sparsePostingsEnum.nextDoc();

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, result);
        verify(mockDocWeightIterator).nextDoc();
        verify(mockClusterIterator).hasNext();
    }

    public void testNextDoc_currentCluster() throws IOException {
        int expectedDocId = 10;
        when(mockDocWeightIterator.nextDoc()).thenReturn(expectedDocId);

        int result = sparsePostingsEnum.nextDoc();

        assertEquals(expectedDocId, result);
        verify(mockDocWeightIterator).nextDoc();
    }

    public void testNextDoc_nextCluster() throws IOException {
        // reset mockDocWeightIterator and mockClusterIterator because setUp calls it
        reset(mockDocWeightIterator);
        reset(mockClusterIterator);

        when(mockDocWeightIterator.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(mockClusterIterator.hasNext()).thenReturn(true);

        // configure the next cluster
        int expectedDocId = 10;
        DocWeightIterator mockSecondDocWeightIterator = mock(DocWeightIterator.class);
        DocumentCluster mockSecondCluster = mock(DocumentCluster.class);
        when(mockSecondCluster.getDisi()).thenReturn(mockSecondDocWeightIterator);
        when(mockSecondDocWeightIterator.nextDoc()).thenReturn(expectedDocId);

        when(mockClusterIterator.next()).thenReturn(mockSecondCluster);

        int result = sparsePostingsEnum.nextDoc();

        assertEquals(expectedDocId, result);
        verify(mockDocWeightIterator, times(1)).nextDoc();
        verify(mockClusterIterator, times(1)).next();
        verify(mockSecondDocWeightIterator, times(1)).nextDoc();
    }

    public void testAdvance() {
        Exception exception = expectThrows(UnsupportedOperationException.class, () -> sparsePostingsEnum.advance(5));
        assertNull(exception.getMessage());
    }

    public void testCost() {
        assertEquals(0, sparsePostingsEnum.cost());
    }
}
