/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostingClustersTests extends AbstractSparseTestBase {

    public void testConstructorWithNullClusters() {
        PostingClusters postingClusters = new PostingClusters(null);
        assertEquals(0, postingClusters.getSize());
        assertNull(postingClusters.getClusters());
    }

    public void testConstructorWithEmptyClusters() {
        List<DocumentCluster> clusters = new ArrayList<>();
        PostingClusters postingClusters = new PostingClusters(clusters);
        assertEquals(0, postingClusters.getSize());
        assertEquals(clusters, postingClusters.getClusters());
    }

    public void testConstructorWithClusters() {
        DocumentCluster cluster1 = mock(DocumentCluster.class);
        DocumentCluster cluster2 = mock(DocumentCluster.class);
        when(cluster1.size()).thenReturn(3);
        when(cluster2.size()).thenReturn(2);

        List<DocumentCluster> clusters = Arrays.asList(cluster1, cluster2);
        PostingClusters postingClusters = new PostingClusters(clusters);

        assertEquals(5, postingClusters.getSize());
        assertEquals(clusters, postingClusters.getClusters());
    }

    public void testIterator() {
        DocumentCluster cluster1 = mock(DocumentCluster.class);
        DocumentCluster cluster2 = mock(DocumentCluster.class);
        List<DocumentCluster> clusters = Arrays.asList(cluster1, cluster2);

        PostingClusters postingClusters = new PostingClusters(clusters);
        IteratorWrapper<DocumentCluster> iterator = postingClusters.iterator();

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertEquals(cluster1, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(cluster2, iterator.next());
    }

    public void testRamBytesUsed() {
        DocumentCluster cluster1 = mock(DocumentCluster.class);
        DocumentCluster cluster2 = mock(DocumentCluster.class);
        when(cluster1.ramBytesUsed()).thenReturn(100L);
        when(cluster2.ramBytesUsed()).thenReturn(200L);

        List<DocumentCluster> clusters = Arrays.asList(cluster1, cluster2);
        PostingClusters postingClusters = new PostingClusters(clusters);

        long ramUsed = postingClusters.ramBytesUsed();
        assertTrue(ramUsed > 300L); // Should include shallow size + cluster sizes
    }
}
