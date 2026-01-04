/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TwoPhaseScorerTests extends AbstractSparseTestBase {

    private ResultsDocValueIterator<Float> createResultsIterator(List<Integer> docs, List<Float> scores) {
        List<Pair<Integer, Float>> results = IntStream.range(0, docs.size())
            .mapToObj(i -> Pair.of(docs.get(i), scores.get(i)))
            .collect(Collectors.toList());
        return new ResultsDocValueIterator<>(results);
    }

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

    public void testConstructor() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(1, 3, 5), Arrays.asList(10f, 30f, 50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 5), Arrays.asList(1f, 5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        assertEquals(-1, scorer.docID());
        assertNotNull(scorer.iterator());
    }

    public void testGetMaxScore() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(1), Arrays.asList(10f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1), Arrays.asList(1f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        assertEquals(0f, scorer.getMaxScore(100), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocLessThanPhaseTwoDoc() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(1, 3), Arrays.asList(10f, 30f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(5), Arrays.asList(5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        scorer.iterator().nextDoc();
        assertEquals(10f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocGreaterThanPhaseTwoDocAndAdvanceMatches() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(5), Arrays.asList(50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 5), Arrays.asList(1f, 5f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        scorer.iterator().nextDoc();
        assertEquals(55f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocGreaterThanPhaseTwoDocAndAdvanceDoesNotMatch() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(5), Arrays.asList(50f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1, 7), Arrays.asList(1f, 7f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        scorer.iterator().nextDoc();
        assertEquals(50f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testScorePhaseOneDocEqualsPhaseTwoDoc() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(3), Arrays.asList(30f));
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(3), Arrays.asList(3f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        scorer.iterator().nextDoc();
        assertEquals(33f, scorer.score(), DELTA_FOR_ASSERTION);
    }

    public void testIterationWithMultipleDocs() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(
            Arrays.asList(1, 3, 5, 7),
            Arrays.asList(10f, 30f, 50f, 70f)
        );
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(3, 7), Arrays.asList(3f, 7f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

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

    public void testEmptyPhaseOneResults() throws IOException {
        ResultsDocValueIterator<Float> phaseOneIterator = createResultsIterator(Arrays.asList(), Arrays.asList());
        Scorer phaseTwoScorer = createMockScorer(Arrays.asList(1), Arrays.asList(1f));

        TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneIterator, phaseTwoScorer);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, scorer.iterator().nextDoc());
    }
}
