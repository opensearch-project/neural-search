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
}
