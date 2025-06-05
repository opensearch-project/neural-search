/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.extern.log4j.Log4j2;
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
import org.opensearch.neuralsearch.sparse.common.Profiling;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

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
@Log4j2
public class PostingWithClustersScorer extends Scorer {

    private final String fieldName;
    private final SparseQueryContext sparseQueryContext;
    private final SparseVector queryVector;
    private final float[] queryDenseVector;
    // The heap to maintain docId and its similarity score with query
    private final PriorityQueue<Pair<Integer, Float>> scoreHeap = new PriorityQueue<>((a, b) -> Float.compare(a.getRight(), b.getRight()));
    private final LongBitSet visitedDocId;
    private SparseVectorReader reader;
    private List<Scorer> subScorers = new ArrayList<>();
    private Terms terms;
    private float score;
    private final Bits acceptedDocs;
    private float heapThreshold = Float.MIN_VALUE;

    public PostingWithClustersScorer(
        String fieldName,
        SparseQueryContext sparseQueryContext,
        SparseVector queryVector,
        LeafReaderContext context,
        Bits acceptedDocs,
        SparseVectorReader reader
    ) throws IOException {
        this.sparseQueryContext = sparseQueryContext;
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.queryDenseVector = queryVector.toDenseVector();
        this.visitedDocId = new LongBitSet(context.reader().maxDoc());
        this.acceptedDocs = acceptedDocs;
        this.reader = reader;
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
            if (!(postingsEnum instanceof SparsePostingsEnum)) {
                log.error("posting enum is not SparsePostingsEnum, actual type: {}", postingsEnum.getClass().getName());
                return;
            }
            SparsePostingsEnum sparsePostingsEnum = (SparsePostingsEnum) postingsEnum;
            log.debug(
                "query token: {}, posting doc size: {}, cluster size: {}",
                token,
                sparsePostingsEnum.size(),
                sparsePostingsEnum.getClusters().getClusters().size()
            );
            if (null == reader) {
                SparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.get(sparsePostingsEnum.getIndexKey());
                if (index != null) {
                    SparseVectorForwardIndex.SparseVectorForwardIndexReader indexReader = index.getForwardIndexReader();
                    reader = (docId) -> { return indexReader.readSparseVector(docId); };
                } else {
                    reader = (docId) -> { return null; };
                }
            }
            subScorers.add(new SingleScorer(sparsePostingsEnum, term));
        }
    }

    private void addToHeap(Pair<Integer, Float> pair) {
        if (pair.getRight() > heapThreshold) {
            scoreHeap.add(pair);
            if (scoreHeap.size() > sparseQueryContext.getK()) {
                scoreHeap.poll();
                heapThreshold = scoreHeap.peek().getRight();
            }
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
                    addToHeap(Pair.of(docId, score));
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

                private DocumentCluster nextQualifiedCluster() {
                    DocumentCluster cluster = clusterIter.next();
                    while (cluster != null) {
                        if (cluster.isShouldNotSkip()) {
                            return cluster;
                        }
                        float score = cluster.getSummary().dotProduct(queryDenseVector);
                        if (scoreHeap.size() == sparseQueryContext.getK()
                            && score < scoreHeap.peek().getRight() / sparseQueryContext.getHeapFactor()) {
                            cluster = clusterIter.next();
                        } else {
                            return cluster;
                        }
                    }
                    return null;
                }

                @Override
                public int docID() {
                    return docs.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    DocumentCluster cluster = null;
                    if (docs == null) {
                        cluster = nextQualifiedCluster();
                    } else {
                        int docId = docs.nextDoc();
                        if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                            return docId;
                        }
                        cluster = nextQualifiedCluster();
                    }
                    if (cluster == null) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    docs = cluster.getDisi();
                    // every cluster should have at least one doc
                    return docs.nextDoc();
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
