/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ResultsDocValueIteratorTests extends AbstractSparseTestBase {

    public void testResultsDocValueIterator() throws IOException {
        // Create test results
        List<Pair<Integer, Integer>> results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));

        // Create iterator
        ResultsDocValueIterator iterator = new ResultsDocValueIterator(results);

        // Test nextDoc
        assertEquals(-1, iterator.docID());
        assertEquals(1, iterator.nextDoc());
        assertEquals(1, iterator.docID());
        assertEquals(1, iterator.docID());
        assertEquals(10f, iterator.score(), DELTA_FOR_ASSERTION);
        assertEquals(3, iterator.nextDoc());
        assertEquals(3, iterator.docID());
        assertEquals(30f, iterator.score(), DELTA_FOR_ASSERTION);
        assertEquals(5, iterator.nextDoc());
        assertEquals(5, iterator.docID());
        assertEquals(50f, iterator.score(), DELTA_FOR_ASSERTION);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }

    public void testResultsDocValueIterator_advance() throws IOException {
        // Create new iterator for advance test
        List<Pair<Integer, Integer>> results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));
        results.add(Pair.of(7, 70));

        ResultsDocValueIterator iterator = new ResultsDocValueIterator(results);
        // Test advance
        assertEquals(1, iterator.nextDoc());
        assertEquals(1, iterator.advance(0));
        assertEquals(3, iterator.advance(3));
        assertEquals(3, iterator.docID());
        assertEquals(7, iterator.advance(6));
        assertEquals(7, iterator.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(10));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }

    public void testResultsDocValueIterator_score() throws IOException {
        // Create new iterator for advance test
        List<Pair<Integer, Integer>> results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));
        results.add(Pair.of(7, 70));

        ResultsDocValueIterator<Integer> iterator = new ResultsDocValueIterator<>(results);
        assertEquals(1, iterator.nextDoc());
        assertEquals(10f, iterator.score(), DELTA_FOR_ASSERTION);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(100));
        assertEquals(0, iterator.cost());
        assertEquals(0f, iterator.score(), DELTA_FOR_ASSERTION);
    }

}
