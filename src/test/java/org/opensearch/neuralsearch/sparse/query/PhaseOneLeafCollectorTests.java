/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorable;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PhaseOneLeafCollectorTests extends AbstractSparseTestBase {

    public void testSetScorer() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);
        Scorable scorer = mock(Scorable.class);
        collector.setScorer(scorer);
    }

    public void testCollectSingleDoc() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(5.0f);
        collector.setScorer(scorer);

        collector.collect(1);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        assertEquals(1, results.nextDoc());
        assertEquals(5.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, results.nextDoc());
    }

    public void testCollectMultipleDocs() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(10.0f).thenReturn(20.0f).thenReturn(30.0f);
        collector.setScorer(scorer);

        collector.collect(3);
        collector.collect(1);
        collector.collect(5);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        // Results should be sorted by doc ID ascending
        assertEquals(1, results.nextDoc());
        assertEquals(20.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(3, results.nextDoc());
        assertEquals(10.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(5, results.nextDoc());
        assertEquals(30.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, results.nextDoc());
    }

    public void testHeapOverflow() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(2);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(10.0f).thenReturn(30.0f).thenReturn(20.0f);
        collector.setScorer(scorer);

        collector.collect(1);
        collector.collect(2);
        collector.collect(3);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        // Only top 2 scores should be retained (30.0f and 20.0f)
        assertEquals(2, results.nextDoc());
        assertEquals(30.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(3, results.nextDoc());
        assertEquals(20.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, results.nextDoc());
    }

    public void testEmptyResults() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, results.nextDoc());
    }

    public void testSingleCapacity() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(1);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(5.0f).thenReturn(15.0f).thenReturn(10.0f);
        collector.setScorer(scorer);

        collector.collect(1);
        collector.collect(2);
        collector.collect(3);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        // Only highest score (15.0f) should be retained
        assertEquals(2, results.nextDoc());
        assertEquals(15.0f, results.score(), DELTA_FOR_ASSERTION);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, results.nextDoc());
    }

    public void testEncodeDecodeLargeDocId() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(123.456f);
        collector.setScorer(scorer);

        int largeDocId = Integer.MAX_VALUE - 1;
        collector.collect(largeDocId);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        assertEquals(largeDocId, results.nextDoc());
        assertEquals(123.456f, results.score(), DELTA_FOR_ASSERTION);
    }

    public void testEncodeDecodeZeroScore() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(0.0f);
        collector.setScorer(scorer);

        collector.collect(42);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        assertEquals(42, results.nextDoc());
        assertEquals(0.0f, results.score(), DELTA_FOR_ASSERTION);
    }

    public void testEncodeDecodeNegativeScore() throws IOException {
        PhaseOneLeafCollector collector = new PhaseOneLeafCollector(10);
        Scorable scorer = mock(Scorable.class);
        when(scorer.score()).thenReturn(-99.5f);
        collector.setScorer(scorer);

        collector.collect(7);

        ResultsDocValueIterator<Float> results = collector.getPhaseOneResults();
        assertEquals(7, results.nextDoc());
        assertEquals(-99.5f, results.score(), DELTA_FOR_ASSERTION);
    }
}
