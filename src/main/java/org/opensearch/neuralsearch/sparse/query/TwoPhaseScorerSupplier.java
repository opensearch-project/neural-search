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
    private final Scorer scorer;

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
        scorer = new TwoPhaseScorer(phaseOneScorerSupplier.get(0), phaseTwoScorerSupplier.get(0), filterBitSetIterator, querySize);
    }

    @Override
    public Scorer get(long leadCost) throws IOException {
        return scorer;
    }

    @Override
    public BulkScorer bulkScorer() throws IOException {
        return new BulkScorer() {
            // We ignore the max value as our algorithm can't limit the docId to range of (min, max)
            // so, to ensure it's only called once, we return the maxDoc
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                collector.setScorer(scorer);
                DocIdSetIterator iter = scorer.iterator();
                int docId = iter.nextDoc();
                while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                    collector.collect(docId);
                    docId = iter.nextDoc();
                }
                return DocIdSetIterator.NO_MORE_DOCS;
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
