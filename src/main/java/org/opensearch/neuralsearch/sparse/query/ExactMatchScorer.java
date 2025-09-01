/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.NonNull;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;

/**
 * Exact match scorer for sparse vector query
 */
public class ExactMatchScorer extends Scorer {
    @NonNull
    private final BitSetIterator candidate;
    private final byte[] queryDenseVector;
    @NonNull
    private final SparseVectorReader reader;
    @NonNull
    private final Similarity.SimScorer simScorer;

    public ExactMatchScorer(
        @NonNull BitSetIterator candidate,
        SparseVector queryVector,
        @NonNull SparseVectorReader reader,
        @NonNull Similarity.SimScorer simScorer
    ) {

        this.candidate = candidate;
        this.queryDenseVector = queryVector.toDenseVector();
        this.reader = reader;
        this.simScorer = simScorer;
    }

    @Override
    public int docID() {
        return candidate.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return candidate;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        int docId = docID();
        SparseVector docVector = reader.read(docId);
        if (docVector == null) {
            return 0;
        }
        return simScorer.score(docVector.dotProduct(queryDenseVector), 0);
    }
}
