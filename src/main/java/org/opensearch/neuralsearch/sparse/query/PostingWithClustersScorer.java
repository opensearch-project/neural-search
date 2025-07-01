/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.algorithm.SeismicBaseScorer;
import org.opensearch.neuralsearch.sparse.common.Profiling;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;

/**
 * A scorer that simulates the query algorithm in seismic.
 * For each query token: we get its posting with clusters. We compute score = dp(cluster_summary, query) and
 * only if heap.size >= k and score > (heap.peek() / heap_factor), we'll evaluate each doc in the cluster,
 * otherwise, we skip the whole cluster.
 */
@Log4j2
public class PostingWithClustersScorer extends SeismicBaseScorer {
    private int score;
    private final Similarity.SimScorer simScorer;

    public PostingWithClustersScorer(
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
    }

    @Override
    public int docID() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocIdSetIterator iterator() {
        return new DocIdSetIterator() {
            private int subScorersIndex = 0;

            @Override
            public int docID() {
                if (subScorersIndex == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }
                if (subScorersIndex >= subScorers.size()) {
                    return NO_MORE_DOCS;
                }
                return subScorers.get(subScorersIndex).iterator().docID();
            }

            @Override
            public int nextDoc() throws IOException {
                while (subScorersIndex < subScorers.size()) {
                    long start = Profiling.INSTANCE.begin(Profiling.ItemId.NEXTDOC);
                    Scorer scorer = subScorers.get(subScorersIndex);
                    int docId = scorer.iterator().nextDoc();
                    // reach the end of current cluster;
                    if (docId == NO_MORE_DOCS) {
                        ++subScorersIndex;
                        continue;
                    }
                    Profiling.INSTANCE.end(Profiling.ItemId.NEXTDOC, start);
                    start = Profiling.INSTANCE.begin(Profiling.ItemId.ACCEPTED);
                    // doc marked as deleted
                    if (acceptedDocs != null && !acceptedDocs.get(docId)) {
                        continue;
                    }
                    Profiling.INSTANCE.end(Profiling.ItemId.ACCEPTED, start);
                    start = Profiling.INSTANCE.begin(Profiling.ItemId.VISITED);
                    // already visited this docId
                    if (visitedDocId.get(docId)) {
                        continue;
                    }
                    visitedDocId.set(docId);
                    Profiling.INSTANCE.end(Profiling.ItemId.VISITED, start);
                    start = Profiling.INSTANCE.begin(Profiling.ItemId.READ);
                    SparseVector doc = reader.read(docId);
                    if (doc == null) {
                        continue;
                    }
                    Profiling.INSTANCE.end(Profiling.ItemId.READ, start);
                    start = Profiling.INSTANCE.begin(Profiling.ItemId.DP);
                    score = doc.dotProduct(queryDenseVector);
                    Profiling.INSTANCE.end(Profiling.ItemId.DP, start);
                    start = Profiling.INSTANCE.begin(Profiling.ItemId.HEAP);
                    scoreHeap.add(Pair.of(docId, score));
                    Profiling.INSTANCE.end(Profiling.ItemId.HEAP, start);
                    return docId;
                }
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                return 0;
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
        return this.simScorer.score(score, 0);
    }
}
