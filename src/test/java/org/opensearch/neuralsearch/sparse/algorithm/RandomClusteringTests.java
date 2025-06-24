/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RandomClusteringTests extends AbstractSparseTestBase {

    @Mock
    private SparseVectorReader reader;

    private List<DocFreq> docFreqs;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Prepare test data
        docFreqs = preparePostings(0, 10, 1, 20, 2, 30, 3, 40, 4, 50);
    }

    @Test
    public void testClusterWithClusterRatio0() throws IOException {
        // Create RandomClustering with beta=1 (single cluster)
        RandomClustering clustering = new RandomClustering(1.0f, 0, reader);

        // Call cluster method
        List<DocumentCluster> clusters = clustering.cluster(docFreqs);

        // Verify that only one cluster is returned
        assertEquals(1, clusters.size());

        // Verify that the cluster contains all documents
        DocumentCluster cluster = clusters.get(0);
        assertEquals(docFreqs.size(), cluster.size());
        assertTrue(cluster.isShouldNotSkip());

        // Verify all documents are in the cluster
        List<Integer> docIds = new ArrayList<>();
        cluster.iterator().forEachRemaining(df -> docIds.add(df.getDocID()));
        assertEquals(docFreqs.size(), docIds.size());
        for (DocFreq df : docFreqs) {
            assertTrue(docIds.contains(df.getDocID()));
        }
        verify(reader, times(0)).read(anyInt());
    }

    @Test
    public void testClusterWithMultipleClusters() throws IOException {
        // Create mock vectors for each document
        SparseVector vector0 = createVector(1, 10, 2, 20);
        SparseVector vector1 = createVector(2, 15, 3, 25);
        SparseVector vector2 = createVector(3, 30, 4, 10);
        SparseVector vector3 = createVector(4, 40, 5, 5);
        SparseVector vector4 = createVector(5, 50, 6, 15);

        // Set up the reader to return the vectors
        when(reader.read(0)).thenReturn(vector0);
        when(reader.read(1)).thenReturn(vector1);
        when(reader.read(2)).thenReturn(vector2);
        when(reader.read(3)).thenReturn(vector3);
        when(reader.read(4)).thenReturn(vector4);

        RandomClustering clustering = new RandomClustering(1f, 0.5f, reader);

        // Call cluster method
        List<DocumentCluster> clusters = clustering.cluster(docFreqs);

        // Verify that multiple clusters are returned
        assertEquals(3, clusters.size());

        // Verify that each cluster has a summary
        for (DocumentCluster cluster : clusters) {
            assertNotNull(cluster.getSummary());
            assertFalse(cluster.isShouldNotSkip());
        }

        // Verify that all documents are assigned to clusters
        int totalDocs = 0;
        for (DocumentCluster cluster : clusters) {
            totalDocs += cluster.size();
        }
        assertEquals(docFreqs.size(), totalDocs);
    }

    @Test
    public void testClusterWithEmptyDocFreqs() throws IOException {
        // Create RandomClustering
        RandomClustering clustering = new RandomClustering(1.0f, 0.1f, reader);

        // Call cluster method with empty list
        List<DocumentCluster> clusters = clustering.cluster(Collections.emptyList());

        // Verify that an empty list of clusters is returned
        assertTrue(clusters.isEmpty());
        verify(reader, never()).read(anyInt());
    }

    @Test(expected = NullPointerException.class)
    public void testClusterWithNullReader() throws IOException {
        // Create RandomClustering with null reader
        new RandomClustering(10, 1.0f, null);
    }

    @Test
    public void testClusterWithNullVectors() throws IOException {
        // Set up the reader to return null for some documents
        when(reader.read(0)).thenReturn(createVector(1, 10));
        when(reader.read(1)).thenReturn(null); // Null vector
        when(reader.read(2)).thenReturn(createVector(3, 30));
        when(reader.read(3)).thenReturn(null); // Null vector
        when(reader.read(4)).thenReturn(createVector(5, 50));

        // Create RandomClustering
        RandomClustering clustering = new RandomClustering(2, 0.5f, reader);

        // Call cluster method
        List<DocumentCluster> clusters = clustering.cluster(docFreqs);

        // Verify that clusters are created
        assertFalse(clusters.isEmpty());

        // Verify that documents with null vectors are not included in any cluster
        List<Integer> allAssignedDocs = new ArrayList<>();
        for (DocumentCluster cluster : clusters) {
            cluster.iterator().forEachRemaining(df -> allAssignedDocs.add(df.getDocID()));
        }

        // Doc IDs 1 and 3 should not be in any cluster
        assertFalse(allAssignedDocs.contains(1));
        assertFalse(allAssignedDocs.contains(3));

        // We should have 3 docs in clusters (out of 5 total)
        assertEquals(3, allAssignedDocs.size());
    }

    @Test
    public void testClusterWithDifferentParameters() throws IOException {
        // Create mock vectors
        for (int i = 0; i < 5; i++) {
            when(reader.read(i)).thenReturn(createVector(i, 10 * (i + 1)));
        }

        // Test with different lambda values
        RandomClustering clustering1 = new RandomClustering(5, 0.5f, reader);
        List<DocumentCluster> clusters1 = clustering1.cluster(docFreqs);

        RandomClustering clustering2 = new RandomClustering(10, 0.5f, reader);
        List<DocumentCluster> clusters2 = clustering2.cluster(docFreqs);

        // Higher lambda should result in fewer clusters
        assertTrue(clusters1.size() >= clusters2.size());

        // Test with different alpha values
        RandomClustering clustering3 = new RandomClustering(5, 0.3f, reader);
        List<DocumentCluster> clusters3 = clustering3.cluster(docFreqs);

        // Verify that clusters are created with different alpha
        assertFalse(clusters3.isEmpty());
    }

    @Test
    public void testClusterAssignment() throws IOException {
        // Create vectors with clear similarity patterns
        // Vector 0 and 1 are similar, 2 and 3 are similar, 4 is different
        SparseVector vector0 = createVector(1, 10, 2, 20);
        SparseVector vector1 = createVector(1, 15, 2, 25); // Similar to vector0
        SparseVector vector2 = createVector(3, 30, 4, 40);
        SparseVector vector3 = createVector(3, 35, 4, 45); // Similar to vector2
        SparseVector vector4 = createVector(5, 50, 6, 60); // Different from others

        when(reader.read(0)).thenReturn(vector0);
        when(reader.read(1)).thenReturn(vector1);
        when(reader.read(2)).thenReturn(vector2);
        when(reader.read(3)).thenReturn(vector3);
        when(reader.read(4)).thenReturn(vector4);

        // Create clustering with 3 clusters
        RandomClustering clustering = new RandomClustering(2, 1.0f, reader);
        List<DocumentCluster> clusters = clustering.cluster(docFreqs);

        // Verify that we have clusters
        assertEquals(3, clusters.size());

        // Verify that each document is assigned to exactly one cluster
        List<Integer> allAssignedDocs = new ArrayList<>();
        for (DocumentCluster cluster : clusters) {
            cluster.iterator().forEachRemaining(df -> allAssignedDocs.add(df.getDocID()));
        }
        assertEquals(5, allAssignedDocs.size());

        // Check for duplicates (no document should be in multiple clusters)
        long uniqueCount = allAssignedDocs.stream().distinct().count();
        assertEquals(allAssignedDocs.size(), uniqueCount);
    }
}
