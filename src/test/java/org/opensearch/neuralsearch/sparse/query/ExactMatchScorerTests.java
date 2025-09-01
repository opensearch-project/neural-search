/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

public class ExactMatchScorerTests extends AbstractSparseTestBase {

    @Mock
    private SparseVectorReader mockReader;

    @Mock
    private Similarity.SimScorer mockSimScorer;

    private BitSetIterator bitSetIterator;
    private SparseVector queryVector;
    private ExactMatchScorer scorer;

    /**
     * Set up test environment
     * - Initialize BitSetIterator with documents 0, 2, 5
     * - Create query vector with dimensions [1, 3, 5] and values [1, 2, 3]
     * - Configure mocks for reader and simScorer
     */
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        // Create a BitSet with documents 0, 2, 5
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(0);
        bitSet.set(2);
        bitSet.set(5);
        bitSetIterator = new BitSetIterator(bitSet, 3);

        // Create query vector
        queryVector = createVector(1, 1, 3, 2, 5, 3);

        // Configure mocks
        when(mockSimScorer.score(anyInt(), anyInt())).thenReturn(1.0f);
    }

    public void testConstructorAndBasicMethods() {
        scorer = new ExactMatchScorer(bitSetIterator, queryVector, mockReader, mockSimScorer);

        // Test initial state
        assertEquals(-1, scorer.docID());

        // Test iterator returns the candidate iterator
        DocIdSetIterator iterator = scorer.iterator();
        assertNotNull(iterator);
        assertEquals(bitSetIterator, iterator);

        // Test getMaxScore always returns 0
        try {
            assertEquals(0.0f, scorer.getMaxScore(5), 0.0f);
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    public void testScoreWithNullDocVector() throws IOException {
        // Configure reader to return null for document vector
        when(mockReader.read(anyInt())).thenReturn(null);

        scorer = new ExactMatchScorer(bitSetIterator, queryVector, mockReader, mockSimScorer);

        // Advance to first document
        scorer.iterator().nextDoc();

        // Score should be 0 when document vector is null
        assertEquals(0.0f, scorer.score(), 0.0f);
    }

    public void testScoreWithDocVector() throws IOException {
        // Create document vector
        SparseVector docVector = createVector(1, 2, 3, 1, 5, 3);

        // Configure reader to return the document vector
        when(mockReader.read(0)).thenReturn(docVector);

        // Configure simScorer to return a specific score
        int expectedScore = 13;
        when(mockSimScorer.score(expectedScore, 0)).thenReturn((float) expectedScore);

        scorer = new ExactMatchScorer(bitSetIterator, queryVector, mockReader, mockSimScorer);

        // Advance to first document
        scorer.iterator().nextDoc();

        // Score should match the expected score
        assertEquals(expectedScore, scorer.score(), 0.0f);
    }

    public void testIterationAndScoring() throws IOException {
        // Create document vectors for different docs
        SparseVector docVector0 = createVector(1, 1, 3, 2);
        SparseVector docVector2 = createVector(1, 2, 5, 3);
        SparseVector docVector5 = createVector(3, 1, 5, 2);

        // Configure reader to return different vectors for different docs
        when(mockReader.read(0)).thenReturn(docVector0);
        when(mockReader.read(2)).thenReturn(docVector2);
        when(mockReader.read(5)).thenReturn(docVector5);

        // Configure simScorer to return different scores based on dot product
        when(mockSimScorer.score(5, 0)).thenReturn(1.0f);
        when(mockSimScorer.score(11, 0)).thenReturn(2.0f);
        when(mockSimScorer.score(8, 0)).thenReturn(3.0f);

        scorer = new ExactMatchScorer(bitSetIterator, queryVector, mockReader, mockSimScorer);

        // Test iteration and scoring for all documents
        DocIdSetIterator iterator = scorer.iterator();

        // First document (id=0)
        assertEquals(0, iterator.nextDoc());
        assertEquals(0, scorer.docID());
        assertEquals(1.0f, scorer.score(), 0.0f);

        // Second document (id=2)
        assertEquals(2, iterator.nextDoc());
        assertEquals(2, scorer.docID());
        assertEquals(2.0f, scorer.score(), 0.0f);

        // Third document (id=5)
        assertEquals(5, iterator.nextDoc());
        assertEquals(5, scorer.docID());
        assertEquals(3.0f, scorer.score(), 0.0f);

        // No more documents
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testDotProductCalculation() throws IOException {
        // Create document vector with specific values to test dot product calculation
        SparseVector docVector = createVector(1, 2, 3, 3, 5, 1);

        // Expected dot product: (1*2) + (3*2) + (3*1) = 2 + 6 + 3 = 11
        int expectedDotProduct = 11;

        // Configure reader to return the document vector
        when(mockReader.read(0)).thenReturn(docVector);

        // Verify the dot product is correctly calculated and passed to simScorer
        when(mockSimScorer.score(expectedDotProduct, 0)).thenReturn(3.5f);

        scorer = new ExactMatchScorer(bitSetIterator, queryVector, mockReader, mockSimScorer);

        // Advance to first document
        scorer.iterator().nextDoc();

        // Score should match the expected score based on dot product
        assertEquals(3.5f, scorer.score(), 0.0f);
    }
}
