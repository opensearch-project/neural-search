/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm.seismic;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.ClusteringAlgorithm;
import org.opensearch.neuralsearch.sparse.algorithm.PostingsProcessingUtils;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SeismicPostingClustererAlgorithmTests extends AbstractSparseTestBase {

    @Mock
    private ClusteringAlgorithm mockClusteringAlgorithm;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testClusterWithEmptyPostings() throws IOException {
        // Setup
        int lambda = 5;
        SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(lambda, mockClusteringAlgorithm);

        // Execute
        List<DocumentCluster> result = seismicPostingClusterer.cluster(Collections.emptyList());

        // Verify
        assertTrue("Empty postings should return empty clusters", result.isEmpty());
        verify(mockClusteringAlgorithm, never()).cluster(anyList());
    }

    public void testClusterWithZeroNPosting() throws IOException {
        // Setup
        int lambda = 5;
        SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(0, mockClusteringAlgorithm);
        List<DocWeight> postings = preparePostings(1, 10, 2, 20, 3, 30, 4, 40, 5, 50);

        // Execute
        List<DocumentCluster> result = seismicPostingClusterer.cluster(postings);

        // Verify
        assertTrue("Empty nPostings should return empty clusters", result.isEmpty());
        verify(mockClusteringAlgorithm, never()).cluster(anyList());
    }

    public void testClusterWithFewerThanMinimalDocSize() throws IOException {
        // Setup
        int lambda = 5;
        SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(lambda, mockClusteringAlgorithm);

        // Create a list with fewer than MINIMAL_DOC_SIZE_TO_CLUSTER (10) postings
        List<DocWeight> postings = preparePostings(1, 10, 2, 20, 3, 30, 4, 40, 5, 50);

        // Execute
        List<DocumentCluster> result = seismicPostingClusterer.cluster(postings);

        // Verify
        assertEquals("Should return a single cluster", 1, result.size());
        assertTrue("Cluster should be marked as shouldNotSkip", result.get(0).isShouldNotSkip());
        assertNull("Cluster summary should be null", result.get(0).getSummary());
        verify(mockClusteringAlgorithm, never()).cluster(anyList());
    }

    public void testClusterWithEnoughPostings() throws IOException {
        // Setup
        int lambda = 10;
        SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(lambda, mockClusteringAlgorithm);

        // Create a list with more than MINIMAL_DOC_SIZE_TO_CLUSTER (10) postings
        List<DocWeight> postings = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            postings.add(new DocWeight(i, (byte) (i * 10)));
        }

        // Create expected result
        List<DocWeight> preprocessedPostings = PostingsProcessingUtils.getTopK(postings, lambda);
        List<DocumentCluster> expectedClusters = Collections.singletonList(new DocumentCluster(null, preprocessedPostings, false));

        // Mock clustering behavior
        when(mockClusteringAlgorithm.cluster(any())).thenReturn(expectedClusters);

        // Execute
        List<DocumentCluster> result = seismicPostingClusterer.cluster(postings);

        // Verify
        assertEquals("Should return clusters from clustering algorithm", expectedClusters, result);
        verify(mockClusteringAlgorithm).cluster(preprocessedPostings);
    }

    public void testPreprocessLimitsToLambda() throws IOException {
        // Setup
        int lambda = 10;
        SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(lambda, mockClusteringAlgorithm);

        // Create a list with more postings than lambda
        List<DocWeight> postings = preparePostings(1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80, 9, 90, 10, 100);

        // Create expected result - only top lambda (3) postings should be kept
        List<DocumentCluster> expectedClusters = Collections.singletonList(new DocumentCluster(null, Collections.emptyList(), false));

        // Mock clustering behavior
        when(mockClusteringAlgorithm.cluster(any())).thenReturn(expectedClusters);

        // Execute
        seismicPostingClusterer.cluster(postings);

        // Verify that clustering was called with a list of size lambda
        verify(mockClusteringAlgorithm).cluster(argThat(list -> list.size() == lambda));
    }

    public void testClusterPreservesPostingOrder() throws IOException {
        // Setup
        int lambda = 5;
        ClusteringAlgorithm mockClusteringAlgorithm = mock(ClusteringAlgorithm.class);
        SeismicPostingClusterer seismicPostingClusterer = new SeismicPostingClusterer(lambda, mockClusteringAlgorithm);

        // Create a list of postings
        List<DocWeight> postings = preparePostings(1, 50, 2, 40, 3, 30, 4, 20, 5, 10);
        List<DocWeight> originalPostings = new ArrayList<>(postings);

        // Mock clustering behavior
        when(mockClusteringAlgorithm.cluster(any())).thenReturn(Collections.emptyList());

        // Execute
        seismicPostingClusterer.cluster(postings);

        // Verify that the original list was not modified
        assertEquals("Original postings list should not be modified", originalPostings, postings);
    }
}
