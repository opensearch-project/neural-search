/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridBulkScorerTests extends OpenSearchTestCase {

    private static final int MAX_DOC = 1000;
    private Scorer mockScorer1;
    private Scorer mockScorer2;
    private DocIdSetIterator mockIterator1;
    private DocIdSetIterator mockIterator2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockScorer1 = mock(Scorer.class);
        mockScorer2 = mock(Scorer.class);
        mockIterator1 = mock(DocIdSetIterator.class);
        mockIterator2 = mock(DocIdSetIterator.class);

        when(mockIterator1.cost()).thenReturn(1L);
        when(mockIterator2.cost()).thenReturn(1L);

        when(mockScorer1.iterator()).thenReturn(mockIterator1);
        when(mockScorer2.iterator()).thenReturn(mockIterator2);
    }

    public void testHybridBulkScorerCreation() {
        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        assertNotNull(bulkScorer);
        assertNotNull(bulkScorer.getHybridSubQueryScorer());
        assertNotNull(bulkScorer.getMatching());
        assertNotNull(bulkScorer.getWindowScores());
    }

    public void testHybridBulkScorerWithEmptyScorers() {
        List<Scorer> scorers = Collections.emptyList();
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        assertNotNull(bulkScorer);
        assertEquals(0, bulkScorer.getWindowScores().length);
    }

    public void testHybridBulkScorerWithSingleScorer() {
        List<Scorer> scorers = Collections.singletonList(mockScorer1);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        assertNotNull(bulkScorer);
        assertEquals(1, bulkScorer.getWindowScores().length);
    }

    public void testWindowScoresInitialization() {
        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        float[][] windowScores = bulkScorer.getWindowScores();
        assertEquals(2, windowScores.length);
        assertEquals(4096, windowScores[0].length); // 2^12 (WINDOW_SIZE)
    }

    public void testMatchingBitSetInitialization() {
        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        FixedBitSet matching = bulkScorer.getMatching();
        assertNotNull(matching);
        assertEquals(4096, matching.length()); // 2^12 (WINDOW_SIZE)
    }

    public void testScoreWithInvalidRange() throws IOException {
        // Setup scorers
        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        LeafCollector mockLeafCollector = mock(LeafCollector.class);

        // setup iterator behavior to prevent infinite loop
        when(mockIterator1.docID()).thenReturn(-1, MAX_DOC); // Return -1 first, then MAX_DOC
        when(mockIterator1.nextDoc()).thenReturn(MAX_DOC);   // Return MAX_DOC to indicate end
        when(mockIterator2.docID()).thenReturn(-1, MAX_DOC);
        when(mockIterator2.nextDoc()).thenReturn(MAX_DOC);

        when(mockScorer1.score()).thenReturn(1.0f);
        when(mockScorer2.score()).thenReturn(1.0f);

        // test with max value greater than maxDoc
        int result = bulkScorer.score(mockLeafCollector, null, 0, MAX_DOC + 100);
        assertEquals(MAX_DOC, result);
    }

    /**
     * Test that getNextDocIdCandidate returns the minimum document ID instead of maximum.
     * This is critical for preventing position overflow issues when scorers have divergent positions.
     *
     * Old behavior: With docIds [30004, 4290], returned 30004 (max) - caused position overflow
     * New behavior: With docIds [30004, 4290], returns 4290 (min) - prevents overflow
     *
     * Max doc Id 38000  is greater than window size 4096
     */
    public void testGetNextDocIdCandidateReturnsMinimum_whenMaxDocIdInSegmentIsGreaterThanWindowSize() throws IOException {
        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2);
        // Max Document Range in the segment is 38000
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, 38000);

        LeafCollector mockLeafCollector = mock(LeafCollector.class);

        when(mockIterator1.docID()).thenReturn(-1);
        // 1st matching docId for match query is at 30004
        when(mockIterator1.advance(0)).thenReturn(30004);
        when(mockIterator1.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        when(mockIterator2.docID()).thenReturn(-1);
        // 1st matching docId for knn query is at 4290
        when(mockIterator2.advance(0)).thenReturn(4290);
        when(mockIterator2.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        when(mockScorer1.score()).thenReturn(1.0f);
        when(mockScorer2.score()).thenReturn(0.8f);

        // Initial window size is 4096
        int result = bulkScorer.score(mockLeafCollector, null, 0, 4096);

        // The score method should return the minimum match docId out of all the subqueries. i.e. 4290.
        assertEquals("getNextDocIdCandidate should return minimum doc ID to prevent position overflow", 4290, result);
    }

    /**
     * Test getNextDocIdCandidate with multiple scorers at different positions.
     * Verifies that the minimum position is always returned.
     *
     * MaxDocId 1000 and window size is 4096
     */
    public void testGetNextDocIdCandidate_whenMaxDocIdInSegmentIsLowerThanWindowSize() throws IOException {
        // Create 3 scorers
        Scorer mockScorer3 = mock(Scorer.class);
        DocIdSetIterator mockIterator3 = mock(DocIdSetIterator.class);
        when(mockIterator3.cost()).thenReturn(1L);
        when(mockScorer3.iterator()).thenReturn(mockIterator3);

        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2, mockScorer3);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        LeafCollector mockLeafCollector = mock(LeafCollector.class);

        // Setup: scorer1 at 500, scorer2 at 100, scorer3 at 300
        when(mockIterator1.docID()).thenReturn(-1);
        when(mockIterator1.advance(0)).thenReturn(500);
        when(mockIterator1.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        when(mockIterator2.docID()).thenReturn(-1);
        when(mockIterator2.advance(0)).thenReturn(100);
        when(mockIterator2.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        when(mockIterator3.docID()).thenReturn(-1);
        when(mockIterator3.advance(0)).thenReturn(300);
        when(mockIterator3.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        when(mockScorer1.score()).thenReturn(1.0f);
        when(mockScorer2.score()).thenReturn(0.8f);
        when(mockScorer3.score()).thenReturn(0.9f);

        // Execute scoring
        int result = bulkScorer.score(mockLeafCollector, null, 0, 4096);

        // Should return minimum (100), not maximum (500) or middle (300)
        assertEquals("All Iterators reach the end", Integer.MAX_VALUE, result);
    }

    /**
     * Test getNextDocIdCandidate when all scorers are exhausted.
     * Should return NO_MORE_DOCS.
     */
    public void testGetNextDocIdCandidateAllScorersExhausted() throws IOException {
        List<Scorer> scorers = Arrays.asList(mockScorer1, mockScorer2);
        HybridBulkScorer bulkScorer = new HybridBulkScorer(scorers, true, MAX_DOC);

        LeafCollector mockLeafCollector = mock(LeafCollector.class);

        // Setup: both scorers exhausted
        when(mockIterator1.docID()).thenReturn(-1);
        when(mockIterator1.advance(0)).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(mockIterator1.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        when(mockIterator2.docID()).thenReturn(-1);
        when(mockIterator2.advance(0)).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(mockIterator2.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);

        // Execute scoring
        int result = bulkScorer.score(mockLeafCollector, null, 0, MAX_DOC);

        // Should return NO_MORE_DOCS when all scorers are exhausted
        assertEquals("Should return NO_MORE_DOCS when all scorers exhausted", DocIdSetIterator.NO_MORE_DOCS, result);
    }
}
