/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.lucene.search.DocIdSetIterator;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Mockito;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

public class PostingsProcessorTests extends AbstractSparseTestBase {

    @Mock
    private DocumentCluster cluster;
    @Mock
    private SparseVectorReader reader;
    @Mock
    private DocFreqIterator iterator;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        lenient().when(cluster.getDisi()).thenReturn(iterator);
    }

    public void testGetTopKWhenKLargerThanListSize() {
        // Create a list of DocFreq objects
        List<DocFreq> postings = preparePostings(1, 10, 2, 20, 3, 30);

        // Call getTopK with K larger than list size
        List<DocFreq> result = PostingsProcessor.getTopK(postings, 5);

        // Verify that the original list is returned
        assertEquals(postings.size(), result.size());
        assertTrue(result.containsAll(postings));
    }

    public void testGetTopKWhenKSmallerThanListSize() {
        // Create a list of DocFreq objects with different frequencies
        List<DocFreq> postings = preparePostings(1, 10, 2, 50, 3, 30, 4, 40, 5, 20);
        // Call getTopK with K smaller than list size
        int k = 3;
        List<DocFreq> result = PostingsProcessor.getTopK(postings, k);

        // Verify that only K elements are returned
        assertEquals(k, result.size());

        // Verify that the top K elements by frequency are returned
        // The priority queue keeps the highest frequencies, so we should have 50, 40, 30
        boolean containsDocId2 = false;
        boolean containsDocId3 = false;
        boolean containsDocId4 = false;

        for (DocFreq docFreq : result) {
            if (docFreq.getDocID() == 2) containsDocId2 = true;
            if (docFreq.getDocID() == 3) containsDocId3 = true;
            if (docFreq.getDocID() == 4) containsDocId4 = true;
        }

        assertTrue("Result should contain docId 2", containsDocId2);
        assertTrue("Result should contain docId 3", containsDocId3);
        assertTrue("Result should contain docId 4", containsDocId4);
    }

    public void testGetTopKWithEmptyList() {
        // Call getTopK with an empty list
        List<DocFreq> result = PostingsProcessor.getTopK(Collections.emptyList(), 5);

        // Verify that an empty list is returned
        assertTrue(result.isEmpty());
    }

    public void testSummarize() throws IOException {
        // Set up the iterator to return two documents
        when(iterator.nextDoc()).thenReturn(0, 1, 2, DocIdSetIterator.NO_MORE_DOCS); // Return docIds 0, 1, then NO_MORE_DOCS (-1)
        when(iterator.docID()).thenReturn(0, 1, 2);

        // Create sparse vectors for the documents
        SparseVector vector1 = createVector(1, 10, 2, 20);
        SparseVector vector2 = createVector(2, 15, 3, 25);
        SparseVector vector3 = createVector(3, 35);

        // Set up the reader to return the vectors
        when(reader.read(0)).thenReturn(vector1);
        when(reader.read(1)).thenReturn(vector2);
        when(reader.read(2)).thenReturn(vector3);

        // Call summarize with alpha = 1.0 (include all tokens)
        PostingsProcessor.summarize(cluster, reader, 1.0f);

        // Verify that setSummary was called with a SparseVector containing the expected items
        Mockito.verify(cluster).setSummary(Mockito.argThat(vector -> {
            List<SparseVector.Item> summaryItems = new ArrayList<>();
            vector.iterator().forEachRemaining(summaryItems::add);

            // Check that we have 3 items (tokens 1, 2, 3)
            if (summaryItems.size() != 3) return false;

            // Check that the frequencies are correct (max of the frequencies for each token)
            boolean hasToken1WithFreq10 = false;
            boolean hasToken2WithFreq20 = false;
            boolean hasToken3WithFreq25 = false;

            for (SparseVector.Item item : summaryItems) {
                if (item.getToken() == 1 && item.getIntFreq() == 10) hasToken1WithFreq10 = true;
                if (item.getToken() == 2 && item.getIntFreq() == 20) hasToken2WithFreq20 = true;
                if (item.getToken() == 3 && item.getIntFreq() == 35) hasToken3WithFreq25 = true;
            }

            return hasToken1WithFreq10 && hasToken2WithFreq20 && hasToken3WithFreq25;
        }));
    }

    public void testSummarizeWithAlphaLessThanOne() throws IOException {
        // Set up the iterator to return one document
        when(iterator.nextDoc()).thenReturn(0, DocIdSetIterator.NO_MORE_DOCS); // Return docId 0, then NO_MORE_DOCS (-1)
        when(iterator.docID()).thenReturn(0);

        // Create a sparse vector with items of different frequencies
        SparseVector vector = createVector(1, 50, 2, 30, 3, 10);

        // Set up the reader to return the vector
        when(reader.read(0)).thenReturn(vector);

        // Call summarize with alpha = 0.5 (include tokens that make up 50% of total frequency)
        PostingsProcessor.summarize(cluster, reader, 0.5f);

        // Verify that setSummary was called with a SparseVector containing only the high frequency item
        // Total frequency is 50+30+10=90, 50% of that is 45, so only token 1 with freq 50 should be included
        Mockito.verify(cluster).setSummary(Mockito.argThat(summaryVector -> {
            List<SparseVector.Item> summaryItems = new ArrayList<>();
            summaryVector.iterator().forEachRemaining(summaryItems::add);

            // Check that we have 1 item (token 1)
            if (summaryItems.size() != 1) return false;

            // Check that the item has token 1 with frequency 50
            return summaryItems.get(0).getToken() == 1 && summaryItems.get(0).getIntFreq() == 50;
        }));
    }

    public void testSummarizeWithNullVector() throws IOException {
        // Create a new mock cluster and iterator for this test
        DocumentCluster localCluster = Mockito.mock(DocumentCluster.class);
        DocFreqIterator localIterator = Mockito.mock(DocFreqIterator.class);

        // Set up the iterator to return one document
        when(localCluster.getDisi()).thenReturn(localIterator);
        when(localIterator.nextDoc()).thenReturn(0, DocIdSetIterator.NO_MORE_DOCS); // Return docId 0, then NO_MORE_DOCS (-1)
        when(localIterator.docID()).thenReturn(0);

        // Set up the reader to return null for the document
        when(reader.read(0)).thenReturn(null);

        // Call summarize
        PostingsProcessor.summarize(localCluster, reader, 1.0f);

        // Verify that setSummary was called with an empty SparseVector
        Mockito.verify(localCluster).setSummary(Mockito.argThat(vector -> {
            return !vector.iterator().hasNext(); // Check that the vector is empty
        }));
    }
}
