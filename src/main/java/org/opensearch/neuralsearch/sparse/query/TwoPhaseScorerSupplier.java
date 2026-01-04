/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * Supplier for TwoPhaseScorer instances.
 * Manages scorer creation with configurable expansion ratio.
 */
public class TwoPhaseScorerSupplier extends ScorerSupplier {
    private final BulkScorer phaseOneBulkScorer;
    private final PhaseOneLeafCollector phaseOneLeafCollector;
    private final Scorer phaseTwoScorer;

    /**
     * Creates a two-phase scorer supplier.
     *
     * @param phaseOneScorerSupplier supplier for phase one scorer
     * @param phaseTwoScorerSupplier supplier for phase two scorer
     * @param filterBitSetIterator optional filter for candidate selection
     * @param querySize base query size, multiplied by expansion ratio
     * @throws IOException if scorer creation fails
     */
    public TwoPhaseScorerSupplier(
        ScorerSupplier phaseOneScorerSupplier,
        ScorerSupplier phaseTwoScorerSupplier,
        BitSetIterator filterBitSetIterator,
        int querySize
    ) throws IOException {
        this.phaseOneBulkScorer = phaseOneScorerSupplier.bulkScorer();
        this.phaseOneLeafCollector = new PhaseOneLeafCollector(querySize);
        this.phaseTwoScorer = phaseTwoScorerSupplier.get(0);
    }

    @Override
    public Scorer get(long leadCost) throws IOException {
        return null;
    }

    @Override
    public BulkScorer bulkScorer() throws IOException {
        return new BulkScorer() {
            // We ignore the max value as our algorithm can't limit the docId to range of (min, max)
            // so, to ensure it's only called once, we return the maxDoc
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                // run phase 1:
                int ret = phaseOneBulkScorer.score(phaseOneLeafCollector, acceptDocs, min, max);
                // iterate phase result
                ResultsDocValueIterator<Float> phaseOneResultsIterator = phaseOneLeafCollector.getPhaseOneResults();
                TwoPhaseScorer scorer = new TwoPhaseScorer(phaseOneResultsIterator, phaseTwoScorer);
                collector.setScorer(scorer);
                int docId = phaseOneResultsIterator.nextDoc();
                while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                    collector.collect(docId);
                    docId = phaseOneResultsIterator.nextDoc();
                }
                return ret;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public long cost() {
        return 0;
    }
}
