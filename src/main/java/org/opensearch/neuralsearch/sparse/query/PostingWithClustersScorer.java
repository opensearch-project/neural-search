/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.codec.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A scorer that simulates the query algorithm in seismic.
 * For each query token: we get its posting with clusters. We compute score = dp(cluster_summary, query) and
 * only if heap.size >= k and score > (heap.peek() / heap_factor), we'll evaluate each doc in the cluster,
 * otherwise, we skip the whole cluster.
 */
public class PostingWithClustersScorer extends Scorer {

    private final String fieldName;
    private final SparseQueryContext sparseQueryContext;
    private final SparseVector queryVector;
    private final float[] queryDenseVector;
    // The heap to maintain docId and its similarity score with query
    private final PriorityQueue<Pair<Integer, Float>> scoreHeap = new PriorityQueue<>((a, b) -> Float.compare(a.getRight(), b.getRight()));
    private final LongBitSet visitedDocId;
    private SparseVectorForwardIndex.SparseVectorForwardIndexReader reader;
    private List<Scorer> subScorers = new ArrayList<>();
    private Terms terms;
    private float score;
    private final Bits acceptedDocs;

    public PostingWithClustersScorer(
        String fieldName,
        SparseQueryContext sparseQueryContext,
        SparseVector queryVector,
        LeafReaderContext context,
        Bits acceptedDocs
    ) throws IOException {
        this.sparseQueryContext = sparseQueryContext;
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.queryDenseVector = queryVector.toDenseVector();
        this.visitedDocId = new LongBitSet(context.reader().maxDoc());
        this.acceptedDocs = acceptedDocs;
        initialize(context.reader());
    }

    private void initialize(LeafReader leafReader) throws IOException {
        terms = Terms.getTerms(leafReader, fieldName);
        assert terms != null : "Terms must not be null";
        BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldName);
        for (String token : sparseQueryContext.getTokens()) {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term = new BytesRef(token);
            if (!termsEnum.seekExact(term)) {
                continue;
            }
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            if (postingsEnum instanceof SparsePostingsEnum) {
                SparsePostingsEnum sparsePostingsEnum = (SparsePostingsEnum) postingsEnum;
                if (null == reader) {
                    reader = InMemorySparseVectorForwardIndex.getOrCreate(sparsePostingsEnum.getIndexKey()).getForwardIndexReader();
                }
                subScorers.add(new SingleScorer(sparsePostingsEnum, term));
            }
        }
    }

    private boolean isHeapFull() {
        return scoreHeap.size() == sparseQueryContext.getK();
    }

    private void addToHeap(Pair<Integer, Float> pair) {
        scoreHeap.add(pair);
        if (isHeapFull()) {
            scoreHeap.poll();
        }
    }

    @Override
    public int docID() {
        return iterator().docID();
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
                if (subScorersIndex == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }
                if (subScorersIndex >= subScorers.size()) {
                    subScorersIndex = NO_MORE_DOCS;
                    return NO_MORE_DOCS;
                }
                Scorer scorer = subScorers.get(subScorersIndex);
                int docId = scorer.iterator().nextDoc();
                if (docId == NO_MORE_DOCS) {
                    subScorersIndex++;
                    return nextDoc();
                } else {
                    if (visitedDocId.get(docId)) {
                        return nextDoc();
                    }
                    if (acceptedDocs != null && !acceptedDocs.get(docId)) {
                        return nextDoc();
                    }
                    visitedDocId.set(docId);
                    SparseVector doc = reader.readSparseVector(docId);
                    score = doc.dotProduct(queryDenseVector);
                    addToHeap(Pair.of(docId, score));
                    return docId;
                }
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
        return score;
    }

    class SingleScorer extends Scorer {
        private IteratorWrapper<DocumentCluster> clusterIter;
        private DocFreqIterator docs = null;

        public SingleScorer(SparsePostingsEnum postingsEnum, BytesRef term) throws IOException {
            clusterIter = postingsEnum.clusterIterator();
        }

        @Override
        public int docID() {
            if (docs == null) {
                return -1;
            }
            return docs.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                @Override
                public int docID() {
                    return docs.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    if (docs != null) {
                        int docId = docs.nextDoc();
                        if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                            return docId;
                        }
                    }
                    // current cluster run out docs
                    DocumentCluster cluster = clusterIter.next();
                    if (cluster == null) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    // should not skip cluster
                    if (cluster.isShouldNotSkip()) {
                        docs = cluster.getDisi();
                        return nextDoc();
                    }
                    // check dot product between cluster summary and query vector
                    while (cluster != null) {
                        assert cluster.getSummary() != null;
                        float score = cluster.getSummary().dotProduct(queryDenseVector);
                        if (scoreHeap.size() == sparseQueryContext.getK()
                            && score < scoreHeap.peek().getRight() / sparseQueryContext.getHeapFactor()) {
                            cluster = clusterIter.next();
                        } else {
                            docs = cluster.getDisi();
                            return nextDoc();
                        }
                    }
                    return DocIdSetIterator.NO_MORE_DOCS;
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
            return 0;
        }
    }
}
