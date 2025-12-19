/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSetIterator;

import java.io.IOException;
import java.util.List;

/**
 * Scorer implementing two-phase sparse query execution.
 * Phase one collects top candidates, phase two refines scores.
 */
public class TwoPhaseScorer extends Scorer {
    private final Scorer phaseOneScorer;
    private final Scorer phaseTwoScorer;

    private final ResultsDocValueIterator<Float> phaseOneResultsIterator;
    private final DocIdSetIterator phaseTwoDocIterator;

    /**
     * Creates a two-phase scorer.
     *
     * @param phaseOneScorer scorer for initial candidate selection
     * @param phaseTwoScorer scorer for score refinement
     * @param filterBitSetIterator optional filter for phase one
     * @param maxSize maximum candidates from phase one
     * @throws IOException if scorer initialization fails
     */
    public TwoPhaseScorer(@NonNull Scorer phaseOneScorer, Scorer phaseTwoScorer, BitSetIterator filterBitSetIterator, int maxSize)
        throws IOException {
        this.phaseOneScorer = phaseOneScorer;
        this.phaseTwoScorer = phaseTwoScorer;
        List<Pair<Integer, Float>> results = phaseOneSearchUpfront(filterBitSetIterator, maxSize);
        phaseOneResultsIterator = new ResultsDocValueIterator<>(results);
        phaseTwoDocIterator = phaseTwoScorer.iterator();
    }

    // filter is applied on phase one
    private List<Pair<Integer, Float>> phaseOneSearchUpfront(BitSetIterator filterBitSetIterator, int resultSize) throws IOException {
        HeapWrapper<Float> resultHeap = new HeapWrapper<>(resultSize);
        DocIdSetIterator disi = null;
        if (filterBitSetIterator != null) {
            disi = ConjunctionUtils.intersectIterators(List.of(phaseOneScorer.iterator(), filterBitSetIterator));
        } else {
            disi = phaseOneScorer.iterator();
        }
        int docId = 0;
        while ((docId = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            float score = phaseOneScorer.score();
            resultHeap.add(Pair.of(docId, score));
        }
        return resultHeap.toOrderedList();
    }

    @Override
    public int docID() {
        return phaseOneResultsIterator.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return phaseOneResultsIterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        int phaseOneDocId = phaseOneResultsIterator.docID();
        int phaseTwoDocId = phaseTwoDocIterator.docID();
        if (phaseOneDocId < phaseTwoDocId) {
            return phaseOneResultsIterator.score();
        } else if (phaseOneDocId > phaseTwoDocId) {
            phaseTwoDocIterator.advance(phaseOneDocId);
            phaseTwoDocId = phaseTwoDocIterator.docID();
        }
        if (phaseOneDocId == phaseTwoDocId) {
            return phaseOneResultsIterator.score() + phaseTwoScorer.score();
        } else {
            return phaseOneResultsIterator.score();
        }
    }
}
