/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.io.IOException;
import java.util.List;

/**
 * Iterator over pre-computed search results with score retrieval.
 */
public class ResultsDocValueIterator<V extends Number> extends DocIdSetIterator {
    private final IteratorWrapper<Pair<Integer, V>> resultsIterator;
    private int docId;

    /**
     * Creates iterator from list of document ID and score pairs.
     */
    public ResultsDocValueIterator(List<Pair<Integer, V>> results) {
        resultsIterator = new IteratorWrapper<>(results.iterator());
        docId = -1;
    }

    @Override
    public int docID() {
        return docId;
    }

    @Override
    public int nextDoc() throws IOException {
        if (resultsIterator.next() == null) {
            docId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        docId = resultsIterator.getCurrent().getLeft();
        return docId;
    }

    @Override
    public int advance(int target) throws IOException {
        if (target <= docId) {
            return docId;
        }
        while (resultsIterator.hasNext()) {
            Pair<Integer, V> pair = resultsIterator.next();
            if (pair.getKey() >= target) {
                docId = pair.getKey();
                return docId;
            }
        }
        docId = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return 0;
    }

    public float score() {
        if (resultsIterator.getCurrent() == null || docId == NO_MORE_DOCS) {
            return 0F;
        } else {
            return resultsIterator.getCurrent().getValue().floatValue();
        }
    }
}
