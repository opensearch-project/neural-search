/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TwoPhaseScorerTests extends AbstractSparseTestBase {

    private Scorer createMockScorer(List<Integer> docs, List<Float> scores) throws IOException {
        Scorer scorer = mock(Scorer.class);
        final int[] idx = { -1 };
        DocIdSetIterator disi = new DocIdSetIterator() {
            @Override
            public int docID() {
                return idx[0] < 0 ? -1 : idx[0] >= docs.size() ? NO_MORE_DOCS : docs.get(idx[0]);
            }

            @Override
            public int nextDoc() {
                idx[0]++;
                return docID();
            }

            @Override
            public int advance(int target) {
                while (idx[0] + 1 < docs.size() && docs.get(idx[0] + 1) < target) {
                    idx[0]++;
                }
                return nextDoc();
            }

            @Override
            public long cost() {
                return docs.size();
            }
        };
        when(scorer.iterator()).thenReturn(disi);
        when(scorer.score()).thenAnswer(inv -> idx[0] < 0 || idx[0] >= scores.size() ? 0f : scores.get(idx[0]));
        return scorer;
    }

    public void testConstructorWithoutFilter() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(1, 3, 5), Arrays.asList(10f, 30f, 50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 5), Arrays.asList(1f, 5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        assertEquals(-1, scorer.docID());
        assertNotNull(scorer.iterator());
    }

    public void testConstructorWithFilter() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(1, 3, 5, 7), Arrays.asList(10f, 30f, 50f, 70f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 3, 5), Arrays.asList(1f, 3f, 5f));

        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(1);
        bitSet.set(5);
        BitSetIterator filterIterator = new BitSetIterator(bitSet, 2);

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, filterIterator, 10);

        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(5, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testGetMaxScore() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(1), Arrays.asList(10f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1), Arrays.asList(1f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        assertEquals(0f, scorer.getMaxScore(100), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocLessThanPhaseTwoDoc() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(1, 3), Arrays.asList(10f, 30f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(5), Arrays.asList(5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        scorer.iterator().nextDoc();
        assertEquals(10f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocGreaterThanPhaseTwoDocAndAdvanceMatches() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(5), Arrays.asList(50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 5), Arrays.asList(1f, 5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        scorer.iterator().nextDoc();
        assertEquals(55f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocGreaterThanPhaseTwoDocAndAdvanceDoesNotMatch() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(5), Arrays.asList(50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 7), Arrays.asList(1f, 7f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        scorer.iterator().nextDoc();
        assertEquals(50f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocEqualsPhaseTwoDoc() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(3), Arrays.asList(30f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(3), Arrays.asList(3f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        scorer.iterator().nextDoc();
        assertEquals(33f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testIterationWithMultipleDocs() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(1, 3, 5, 7), Arrays.asList(10f, 30f, 50f, 70f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(3, 7), Arrays.asList(3f, 7f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        DocIdSetIterator iterator = scorer.iterator();

        assertEquals(1, iterator.nextDoc());
        assertEquals(1, scorer.docID());
        assertEquals(10f, scorer.score(), DELTA_FOR_ASSERTION);

        assertEquals(3, iterator.nextDoc());
        assertEquals(3, scorer.docID());
        assertEquals(33f, scorer.score(), DELTA_FOR_ASSERTION);

        assertEquals(5, iterator.nextDoc());
        assertEquals(5, scorer.docID());
        assertEquals(50f, scorer.score(), DELTA_FOR_ASSERTION);

        assertEquals(7, iterator.nextDoc());
        assertEquals(7, scorer.docID());
        assertEquals(77f, scorer.score(), DELTA_FOR_ASSERTION);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testMaxSizeLimitsResults() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(10f, 20f, 30f, 40f, 50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(1f, 2f, 3f, 4f, 5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 2);

        DocIdSetIterator iterator = scorer.iterator();
        int count = 0;
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            count++;
        }
        assertEquals(2, count);
    }

    public void testEmptyPhaseOneResults() throws IOException {
        Scorer phaseOneScorer = createMockScorer(Arrays.asList(), Arrays.asList());
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1), Arrays.asList(1f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneScorer, phaseTwoScorer, null, 10);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
    }
}
