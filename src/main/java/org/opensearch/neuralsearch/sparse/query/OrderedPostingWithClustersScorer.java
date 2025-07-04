/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.List;

public class OrderedPostingWithClustersScorer extends SeismicBaseScorer {

    private final Similarity.SimScorer simScorer;
    private final DocIdSetIterator conjunctionDisi;

    public OrderedPostingWithClustersScorer(
        String fieldName,
        SparseQueryContext sparseQueryContext,
        SparseVector queryVector,
        LeafReader leafReader,
        Bits acceptedDocs,
        SparseVectorReader reader,
        Similarity.SimScorer simScorer,
        BitSetIterator filterBitSetIterator
    ) throws IOException {
        super(leafReader, fieldName, sparseQueryContext, leafReader.maxDoc(), queryVector, reader, acceptedDocs);
        this.simScorer = simScorer;
        List<Pair<Integer, Integer>> results = searchUpfront(sparseQueryContext.getK());
        ResultsDocValueIterator resultsIterator = new ResultsDocValueIterator(results);
        if (filterBitSetIterator != null) {
            conjunctionDisi = ConjunctionUtils.intersectIterators(List.of(resultsIterator, filterBitSetIterator));
        } else {
            conjunctionDisi = resultsIterator;
        }
    }

    @Override
    public int docID() {
        return conjunctionDisi.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return conjunctionDisi;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        return this.simScorer.score(conjunctionDisi.cost(), 0);
    }
}
