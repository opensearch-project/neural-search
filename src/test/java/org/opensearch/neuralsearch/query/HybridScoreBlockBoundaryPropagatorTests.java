/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HybridScoreBlockBoundaryPropagatorTests extends OpenSearchQueryTestCase {

    public void testAdvanceShallow_whenMinCompetitiveScoreSet_thenSuccessful() throws IOException {
        Scorer scorer1 = new MockScorer(10, 0.6f);
        Scorer scorer2 = new MockScorer(40, 1.5f);
        Scorer scorer3 = new MockScorer(30, 2f);
        Scorer scorer4 = new MockScorer(120, 4f);

        List<Scorer> scorers = Arrays.asList(scorer1, scorer2, scorer3, scorer4);
        Collections.shuffle(scorers, random());
        HybridScoreBlockBoundaryPropagator propagator = new HybridScoreBlockBoundaryPropagator(scorers);
        assertEquals(10, propagator.advanceShallow(0));

        propagator.setMinCompetitiveScore(0.1f);
        assertEquals(10, propagator.advanceShallow(0));

        propagator.setMinCompetitiveScore(0.8f);
        assertEquals(30, propagator.advanceShallow(0));

        propagator.setMinCompetitiveScore(1.4f);
        assertEquals(30, propagator.advanceShallow(0));

        propagator.setMinCompetitiveScore(1.9f);
        assertEquals(30, propagator.advanceShallow(0));

        propagator.setMinCompetitiveScore(2.5f);
        assertEquals(120, propagator.advanceShallow(0));

        propagator.setMinCompetitiveScore(7f);
        assertEquals(120, propagator.advanceShallow(0));
    }

    private static class MockWeight extends Weight {

        MockWeight() {
            super(new MatchNoDocsQuery());
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            return null;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext leafReaderContext) {
            return null;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

    private static class MockScorer extends Scorer {

        final int boundary;
        final float maxScore;

        MockScorer(int boundary, float maxScore) throws IOException {
            super();
            this.boundary = boundary;
            this.maxScore = maxScore;
        }

        @Override
        public int docID() {
            return 0;
        }

        @Override
        public float score() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMinCompetitiveScore(float minCompetitiveScore) {}

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return maxScore;
        }

        @Override
        public int advanceShallow(int target) {
            assert target <= boundary;
            return boundary;
        }
    }
}
