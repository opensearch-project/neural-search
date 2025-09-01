/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm.seismic;

import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.CircuitBreakerManager;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusteringTaskTests extends AbstractSparseTestBase {
    private BytesRef term;
    private List<DocWeight> docs;
    private CacheKey key;
    private SeismicPostingClusterer seismicPostingClusterer;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        term = new BytesRef("test_term");
        key = mock(CacheKey.class);
        docs = Arrays.asList(new DocWeight(1, (byte) 1), new DocWeight(2, (byte) 2));
        seismicPostingClusterer = mock(SeismicPostingClusterer.class);
        CircuitBreakerManager.setCircuitBreaker(mock(CircuitBreaker.class));
    }

    public void testConstructor_withValidInputs_createsTask() {
        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);

        assertNotNull(task);
    }

    public void testConstructor_withEmptyDocs_createsTask() {
        docs = Collections.emptyList();

        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);

        assertNotNull(task);
    }

    public void testGet_withValidClustering_returnsPostingClusters() throws IOException {
        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class), mock(DocumentCluster.class));
        when(seismicPostingClusterer.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);
        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(2, result.getClusters().size());
        verify(seismicPostingClusterer, times(1)).cluster(any());
    }

    public void testGet_withEmptyDocs_returnsPostingClusters() throws IOException {
        docs = Collections.emptyList();

        List<DocumentCluster> expectedClusters = Collections.emptyList();
        when(seismicPostingClusterer.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);
        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(0, result.getClusters().size());
        verify(seismicPostingClusterer, times(1)).cluster(any());
    }

    public void testGet_withIOException_throwsRuntimeException() throws IOException {
        doThrow(new IOException("Test exception")).when(seismicPostingClusterer).cluster(any());

        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);

        try {
            task.get();
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IOException);
            assertEquals("Test exception", e.getCause().getMessage());
        }
    }

    public void testGet_callsClusteringWithCorrectDocs() throws IOException {
        docs = Arrays.asList(new DocWeight(1, (byte) 1), new DocWeight(2, (byte) 2), new DocWeight(3, (byte) 3));

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class));
        when(seismicPostingClusterer.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);
        task.get();

        verify(seismicPostingClusterer, times(1)).cluster(docs);
    }

    public void testGet_withSingleDoc_returnsPostingClusters() throws IOException {
        docs = Arrays.asList(new DocWeight(42, (byte) 5));

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class));
        when(seismicPostingClusterer.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(term, docs, key, seismicPostingClusterer);
        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(1, result.getClusters().size());
        verify(seismicPostingClusterer, times(1)).cluster(docs);
    }

    public void testGet_preservesOriginalTermBytes() throws IOException {
        BytesRef originalTerm = new BytesRef("original_term");

        List<DocumentCluster> expectedClusters = Arrays.asList(mock(DocumentCluster.class));
        when(seismicPostingClusterer.cluster(any())).thenReturn(expectedClusters);

        ClusteringTask task = new ClusteringTask(originalTerm, docs, key, seismicPostingClusterer);

        // Modify original term to ensure deep copy was made
        originalTerm.bytes[0] = (byte) 'X';

        PostingClusters result = task.get();

        assertNotNull(result);
        assertNotNull(result.getClusters());
        assertEquals(1, result.getClusters().size());
    }
}
