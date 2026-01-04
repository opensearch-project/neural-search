/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

/**
 * Scorer implementing two-phase sparse query execution.
 * Phase one collects top candidates, phase two refines scores.
 */
@Getter
public class TwoPhaseScorer extends Scorer {
    private final ResultsDocValueIterator<Float> phaseOneResultsIterator;
    private final Scorer phaseTwoScorer;
    private final DocIdSetIterator phaseTwoDocIterator;

    /**
     * Creates a two-phase scorer.
     *
     * @param phaseOneResultsIterator scorer for initial candidate selection
     * @param phaseTwoScorer scorer for score refinement
     */
    public TwoPhaseScorer(@NonNull ResultsDocValueIterator<Float> phaseOneResultsIterator, Scorer phaseTwoScorer) {
        this.phaseTwoScorer = phaseTwoScorer;
        this.phaseTwoDocIterator = phaseTwoScorer.iterator();
        this.phaseOneResultsIterator = phaseOneResultsIterator;
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
