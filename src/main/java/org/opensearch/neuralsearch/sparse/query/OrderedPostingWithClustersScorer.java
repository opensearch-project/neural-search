/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.algorithm.SeismicBaseScorer;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.List;

public class OrderedPostingWithClustersScorer extends SeismicBaseScorer {

    private final Similarity.SimScorer simScorer;
    private final IteratorWrapper<Pair<Integer, Integer>> resultsIterator;

    public OrderedPostingWithClustersScorer(
        String fieldName,
        SparseQueryContext sparseQueryContext,
        SparseVector queryVector,
        LeafReader leafReader,
        Bits acceptedDocs,
        SparseVectorReader reader,
        Similarity.SimScorer simScorer
    ) throws IOException {
        super(leafReader, fieldName, sparseQueryContext, leafReader.maxDoc(), queryVector, reader, acceptedDocs);
        this.simScorer = simScorer;
        List<Pair<Integer, Integer>> results = searchUpfront(sparseQueryContext.getK());
        resultsIterator = new IteratorWrapper<>(results.iterator());
    }

    @Override
    public int docID() {
        if (resultsIterator.getCurrent() == null) {
            return -1;
        } else if (!resultsIterator.hasNext()) {
            return DocIdSetIterator.NO_MORE_DOCS;
        } else {
            return resultsIterator.getCurrent().getKey();
        }
    }

    @Override
    public DocIdSetIterator iterator() {
        return new DocIdSetIterator() {
            @Override
            public int docID() {
                if (resultsIterator.getCurrent() == null) {
                    return -1;
                } else if (!resultsIterator.hasNext()) {
                    return DocIdSetIterator.NO_MORE_DOCS;
                } else {
                    return resultsIterator.getCurrent().getKey();
                }
            }

            @Override
            public int nextDoc() throws IOException {
                if (resultsIterator.next() == null) {
                    return NO_MORE_DOCS;
                }
                return resultsIterator.getCurrent().getLeft();
            }

            @Override
            public int advance(int target) throws IOException {
                if (target <= docID()) {
                    return docID();
                }
                while (resultsIterator.hasNext()) {
                    Pair<Integer, Integer> pair = resultsIterator.next();
                    if (pair.getKey() >= target) {
                        return pair.getKey();
                    }
                }
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        if (resultsIterator.getCurrent() == null) {
            return 0;
        } else {
            return this.simScorer.score(resultsIterator.getCurrent().getValue(), 0);
        }
    }
}
