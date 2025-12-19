/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.BitSetIterator;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TwoPhaseScorerSupplierTests extends AbstractSparseTestBase {

    public void testGet() throws IOException {
        ScorerSupplier phaseOneSupplier = createMockScorerSupplier(new int[] { 1, 3, 5 }, new float[] { 1.0f, 2.0f, 3.0f });
        ScorerSupplier phaseTwoSupplier = createMockScorerSupplier(new int[] { 1, 5 }, new float[] { 0.5f, 0.5f });

        TwoPhaseScorerSupplier supplier = new TwoPhaseScorerSupplier(phaseOneSupplier, phaseTwoSupplier, null, 10);

        Scorer scorer = supplier.get(0);
        assertNotNull(scorer);
    }

    public void testCost() throws IOException {
        ScorerSupplier phaseOneSupplier = createMockScorerSupplier(new int[] { 1 }, new float[] { 1.0f });
        ScorerSupplier phaseTwoSupplier = createMockScorerSupplier(new int[] {}, new float[] {});

        TwoPhaseScorerSupplier supplier = new TwoPhaseScorerSupplier(phaseOneSupplier, phaseTwoSupplier, null, 10);

        assertEquals(0, supplier.cost());
    }

    public void testBulkScorer() throws IOException {
        ScorerSupplier phaseOneSupplier = createMockScorerSupplier(new int[] { 1, 3, 5 }, new float[] { 3.0f, 2.0f, 1.0f });
        ScorerSupplier phaseTwoSupplier = createMockScorerSupplier(new int[] { 1, 3 }, new float[] { 0.5f, 0.5f });

        TwoPhaseScorerSupplier supplier = new TwoPhaseScorerSupplier(phaseOneSupplier, phaseTwoSupplier, null, 10);
        BulkScorer bulkScorer = supplier.bulkScorer();

        assertNotNull(bulkScorer);
        assertEquals(0, bulkScorer.cost());

        List<Integer> collectedDocs = new ArrayList<>();
        LeafCollector collector = new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {
                collectedDocs.add(doc);
            }
        };

        int result = bulkScorer.score(collector, null, 0, Integer.MAX_VALUE);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, result);
        assertEquals(List.of(1, 3, 5), collectedDocs);
    }

    public void testWithFilter() throws IOException {
        ScorerSupplier phaseOneSupplier = createMockScorerSupplier(new int[] { 1, 3, 5, 7 }, new float[] { 1.0f, 2.0f, 3.0f, 4.0f });
        ScorerSupplier phaseTwoSupplier = createMockScorerSupplier(new int[] { 1, 5 }, new float[] { 0.5f, 0.5f });

        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(1);
        bitSet.set(5);
        BitSetIterator filterIterator = new BitSetIterator(bitSet, 2);

        TwoPhaseScorerSupplier supplier = new TwoPhaseScorerSupplier(phaseOneSupplier, phaseTwoSupplier, filterIterator, 10);

        Scorer scorer = supplier.get(0);
        DocIdSetIterator iter = scorer.iterator();

        assertEquals(1, iter.nextDoc());
        assertEquals(5, iter.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());
    }

    private ScorerSupplier createMockScorerSupplier(int[] docIds, float[] scores) throws IOException {
        ScorerSupplier supplier = mock(ScorerSupplier.class);
        Scorer scorer = mock(Scorer.class);
        when(supplier.get(anyLong())).thenReturn(scorer);

        DocIdSetIterator iterator = new DocIdSetIterator() {
            private int idx = -1;

            @Override
            public int docID() {
                return idx < 0 ? -1 : (idx >= docIds.length ? NO_MORE_DOCS : docIds[idx]);
            }

            @Override
            public int nextDoc() {
                idx++;
                return docID();
            }

            @Override
            public int advance(int target) {
                while (idx < docIds.length && docID() < target) {
                    idx++;
                }
                return docID();
            }

            @Override
            public long cost() {
                return docIds.length;
            }
        };

        when(scorer.iterator()).thenReturn(iterator);
        when(scorer.score()).thenAnswer(inv -> {
            int currentIdx = 0;
            int currentDocId = iterator.docID();
            for (int i = 0; i < docIds.length; i++) {
                if (docIds[i] == currentDocId) {
                    currentIdx = i;
                    break;
                }
            }
            return currentIdx < scores.length ? scores[currentIdx] : 0f;
        });

        return supplier;
    }
}
