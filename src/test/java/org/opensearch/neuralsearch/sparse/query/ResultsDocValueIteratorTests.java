/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.List;

public class ResultsDocValueIteratorTests extends AbstractSparseTestBase {

    public void testNextDoc() throws IOException {
        ResultsDocValueIterator<Integer> iterator = new ResultsDocValueIterator<>(List.of(Pair.of(1, 10), Pair.of(3, 30), Pair.of(5, 50)));

        assertEquals(-1, iterator.docID());
        assertEquals(1, iterator.nextDoc());
        assertEquals(1, iterator.docID());
        assertEquals(10F, iterator.score(), 0F);
        assertEquals(3, iterator.nextDoc());
        assertEquals(3, iterator.docID());
        assertEquals(30F, iterator.score(), 0F);
        assertEquals(5, iterator.nextDoc());
        assertEquals(5, iterator.docID());
        assertEquals(50F, iterator.score(), 0F);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }

    public void testAdvance() throws IOException {
        ResultsDocValueIterator<Integer> iterator = new ResultsDocValueIterator<>(
            List.of(Pair.of(1, 10), Pair.of(3, 30), Pair.of(5, 50), Pair.of(7, 70))
        );

        assertEquals(1, iterator.nextDoc());
        // A target at or behind the current doc leaves the cursor untouched.
        assertEquals(1, iterator.advance(0));
        assertEquals(3, iterator.advance(3));
        assertEquals(3, iterator.docID());
        // No entry for 6, so the first one past it wins.
        assertEquals(7, iterator.advance(6));
        assertEquals(7, iterator.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(10));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }

    public void testScoreIsZeroOutsideTheResults() throws IOException {
        ResultsDocValueIterator<Integer> iterator = new ResultsDocValueIterator<>(
            List.of(Pair.of(1, 10), Pair.of(3, 30), Pair.of(5, 50), Pair.of(7, 70))
        );

        // Before the first nextDoc there is no current entry.
        assertEquals(0F, iterator.score(), 0F);
        assertEquals(1, iterator.nextDoc());
        assertEquals(10F, iterator.score(), 0F);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(100));
        assertEquals(0F, iterator.score(), 0F);
    }

    public void testFloatScoresAreNotTruncated() throws IOException {
        ResultsDocValueIterator<Float> iterator = new ResultsDocValueIterator<>(List.of(Pair.of(1, 1.5F), Pair.of(2, 2.25F)));

        assertEquals(1, iterator.nextDoc());
        assertEquals(1.5F, iterator.score(), 0F);
        assertEquals(2, iterator.nextDoc());
        assertEquals(2.25F, iterator.score(), 0F);
    }

    public void testEmptyResults() throws IOException {
        ResultsDocValueIterator<Integer> iterator = new ResultsDocValueIterator<>(List.of());

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertEquals(0F, iterator.score(), 0F);
    }

    public void testCost() throws IOException {
        ResultsDocValueIterator<Integer> iterator = new ResultsDocValueIterator<>(List.of(Pair.of(1, 10)));

        assertEquals(0L, iterator.cost());
        assertEquals(1, iterator.nextDoc());
        assertEquals(0L, iterator.cost());
    }
}
