/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.DocFreq;

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

public class PostingClusteringTests extends AbstractSparseTestBase {

    @Mock
    private Clustering mockClustering;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testClusterWithEmptyPostings() throws IOException {
        // Setup
        int lambda = 5;
        PostingClustering postingClustering = new PostingClustering(lambda, mockClustering);

        // Execute
        List<DocumentCluster> result = postingClustering.cluster(Collections.emptyList());

        // Verify
        assertTrue("Empty postings should return empty clusters", result.isEmpty());
        verify(mockClustering, never()).cluster(anyList());
    }

    public void testClusterWithFewerThanMinimalDocSize() throws IOException {
        // Setup
        int lambda = 5;
        PostingClustering postingClustering = new PostingClustering(lambda, mockClustering);

        // Create a list with fewer than MINIMAL_DOC_SIZE_TO_CLUSTER (10) postings
        List<DocFreq> postings = preparePostings(1, 10, 2, 20, 3, 30, 4, 40, 5, 50);

        // Execute
        List<DocumentCluster> result = postingClustering.cluster(postings);

        // Verify
        assertEquals("Should return a single cluster", 1, result.size());
        assertTrue("Cluster should be marked as shouldNotSkip", result.get(0).isShouldNotSkip());
        assertNull("Cluster summary should be null", result.get(0).getSummary());
        verify(mockClustering, never()).cluster(anyList());
    }

    public void testClusterWithEnoughPostings() throws IOException {
        // Setup
        int lambda = 10;
        PostingClustering postingClustering = new PostingClustering(lambda, mockClustering);

        // Create a list with more than MINIMAL_DOC_SIZE_TO_CLUSTER (10) postings
        List<DocFreq> postings = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            postings.add(new DocFreq(i, (byte) (i * 10)));
        }

        // Create expected result
        List<DocFreq> preprocessedPostings = PostingsProcessor.getTopK(postings, lambda);
        List<DocumentCluster> expectedClusters = Collections.singletonList(new DocumentCluster(null, preprocessedPostings, false));

        // Mock clustering behavior
        when(mockClustering.cluster(any())).thenReturn(expectedClusters);

        // Execute
        List<DocumentCluster> result = postingClustering.cluster(postings);

        // Verify
        assertEquals("Should return clusters from clustering algorithm", expectedClusters, result);
        verify(mockClustering).cluster(preprocessedPostings);
    }

    public void testPreprocessLimitsToLambda() throws IOException {
        // Setup
        int lambda = 10;
        PostingClustering postingClustering = new PostingClustering(lambda, mockClustering);

        // Create a list with more postings than lambda
        List<DocFreq> postings = preparePostings(1, 10, 2, 20, 3, 30, 4, 40, 5, 50, 6, 60, 7, 70, 8, 80, 9, 90, 10, 100);

        // Create expected result - only top lambda (3) postings should be kept
        List<DocumentCluster> expectedClusters = Collections.singletonList(new DocumentCluster(null, Collections.emptyList(), false));

        // Mock clustering behavior
        when(mockClustering.cluster(any())).thenReturn(expectedClusters);

        // Execute
        postingClustering.cluster(postings);

        // Verify that clustering was called with a list of size lambda
        verify(mockClustering).cluster(argThat(list -> list.size() == lambda));
    }

    public void testClusterPreservesPostingOrder() throws IOException {
        // Setup
        int lambda = 5;
        Clustering mockClustering = mock(Clustering.class);
        PostingClustering postingClustering = new PostingClustering(lambda, mockClustering);

        // Create a list of postings
        List<DocFreq> postings = preparePostings(1, 50, 2, 40, 3, 30, 4, 20, 5, 10);
        List<DocFreq> originalPostings = new ArrayList<>(postings);

        // Mock clustering behavior
        when(mockClustering.cluster(any())).thenReturn(Collections.emptyList());

        // Execute
        postingClustering.cluster(postings);

        // Verify that the original list was not modified
        assertEquals("Original postings list should not be modified", originalPostings, postings);
    }
}
