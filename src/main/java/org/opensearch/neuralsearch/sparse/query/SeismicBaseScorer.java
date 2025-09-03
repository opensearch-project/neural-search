/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Base scorer for seismic sparse vector queries with cluster-based optimization.
 * Maintains score heap and visited document tracking for efficient search.
 */
@Log4j2
public abstract class SeismicBaseScorer extends Scorer {
    private final static int SEISMIC_HEAP_SIZE = 10;
    protected final HeapWrapper scoreHeap;
    protected final LongBitSet visitedDocId;
    protected final String fieldName;
    protected final SparseQueryContext sparseQueryContext;
    protected final byte[] queryDenseVector;
    protected final Bits acceptedDocs;
    @Getter
    protected SparseVectorReader reader;
    protected List<Scorer> subScorers = new ArrayList<>();

    /**
     * Creates base scorer with query context and initializes sub-scorers for each token.
     */
    public SeismicBaseScorer(
        LeafReader leafReader,
        String fieldName,
        SparseQueryContext sparseQueryContext,
        int maxDocCount,
        SparseVector queryVector,
        @NonNull SparseVectorReader reader,
        Bits acceptedDocs
    ) throws IOException {
        visitedDocId = new LongBitSet(maxDocCount);
        this.fieldName = fieldName;
        this.sparseQueryContext = sparseQueryContext;
        this.queryDenseVector = queryVector.toDenseVector();
        this.reader = reader;
        this.acceptedDocs = acceptedDocs;
        scoreHeap = new HeapWrapper(SEISMIC_HEAP_SIZE);
        initialize(leafReader);
    }

    protected void initialize(LeafReader leafReader) throws IOException {
        Terms terms = Terms.getTerms(leafReader, fieldName);
        if (terms == null) {
            log.error(String.format(Locale.ROOT, "Terms in field %s is null!", fieldName));
            return;
        }

        for (String token : sparseQueryContext.getTokens()) {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term = new BytesRef(token);
            if (!termsEnum.seekExact(term)) {
                continue;
            }
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            if (!(postingsEnum instanceof SparsePostingsEnum sparsePostingsEnum)) {
                log.error(
                    "posting enum is not SparsePostingsEnum, actual type: {}",
                    postingsEnum == null ? null : postingsEnum.getClass().getName()
                );
                return;
            }
            subScorers.add(new SingleScorer(sparsePostingsEnum));
        }
    }

    /**
     * Performs upfront search across all sub-scorers and returns top results.
     */
    protected List<Pair<Integer, Integer>> searchUpfront(int resultSize) throws IOException {
        HeapWrapper resultHeap = new HeapWrapper(resultSize);
        for (Scorer scorer : subScorers) {
            DocIdSetIterator iterator = scorer.iterator();
            int docId = 0;
            while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (acceptedDocs != null && !acceptedDocs.get(docId)) {
                    continue;
                }
                if (visitedDocId.get(docId)) {
                    continue;
                }
                visitedDocId.set(docId);
                SparseVector doc = reader.read(docId);
                if (doc == null) {
                    continue;
                }
                int score = doc.dotProduct(queryDenseVector);
                scoreHeap.add(Pair.of(docId, score));
                resultHeap.add(Pair.of(docId, score));
            }
        }
        return resultHeap.toOrderedList();
    }

    protected static PriorityQueue<Pair<Integer, Integer>> makeHeap() {
        return new PriorityQueue<>(Comparator.comparingInt(Pair::getRight));
    }

    /**
     * Wrapper for priority queue maintaining top-K results with threshold optimization.
     */
    protected static class HeapWrapper {
        private final PriorityQueue<Pair<Integer, Integer>> heap = makeHeap();
        private float heapThreshold = Integer.MIN_VALUE;
        private final int k;

        HeapWrapper(int k) {
            this.k = k;
        }

        public boolean isFull() {
            return heap.size() == this.k;
        }

        /**
         * Adds pair to heap if score exceeds threshold, maintaining size limit.
         */
        public void add(Pair<Integer, Integer> pair) {
            if (pair.getRight() > heapThreshold) {
                heap.add(pair);
                if (heap.size() > k) {
                    heap.poll();
                    assert heap.peek() != null;
                    heapThreshold = heap.peek().getRight();
                }
            }
        }

        /**
         * Returns heap contents as ordered list sorted by document ID.
         */
        public List<Pair<Integer, Integer>> toOrderedList() {
            List<Pair<Integer, Integer>> list = new ArrayList<>(heap);
            list.sort((a, b) -> Float.compare(a.getLeft(), b.getLeft()));
            return list;
        }

        public int size() {
            return heap.size();
        }

        public Pair<Integer, Integer> peek() {
            return heap.peek();
        }
    }

    /**
     * Scorer for individual query tokens using cluster-based iteration.
     */
    class SingleScorer extends Scorer {
        private final IteratorWrapper<DocumentCluster> clusterIter;
        private DocWeightIterator docs = null;

        public SingleScorer(SparsePostingsEnum postingsEnum) throws IOException {
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

                /**
                 * Finds next cluster that qualifies based on score threshold and heap factor.
                 */
                private DocumentCluster nextQualifiedCluster() {
                    if (clusterIter == null) {
                        return null;
                    }
                    DocumentCluster cluster = clusterIter.next();
                    while (cluster != null) {
                        if (cluster.isShouldNotSkip()) {
                            return cluster;
                        }
                        int score = cluster.getSummary().dotProduct(queryDenseVector);
                        if (scoreHeap.isFull()
                            && score < Objects.requireNonNull(scoreHeap.peek()).getRight() / sparseQueryContext.getHeapFactor()) {
                            cluster = clusterIter.next();
                        } else {
                            return cluster;
                        }
                    }
                    return null;
                }

                @Override
                public int docID() {
                    if (docs == null) {
                        return -1;
                    }
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

    /**
     * Iterator over pre-computed search results with score retrieval via cost().
     */
    public static class ResultsDocValueIterator extends DocIdSetIterator {
        private final IteratorWrapper<Pair<Integer, Integer>> resultsIterator;
        private int docId;

        /**
         * Creates iterator from list of document ID and score pairs.
         */
        public ResultsDocValueIterator(List<Pair<Integer, Integer>> results) {
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
                Pair<Integer, Integer> pair = resultsIterator.next();
                if (pair.getKey() >= target) {
                    docId = pair.getKey();
                    return docId;
                }
            }
            docId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        /**
         * Returns pre-stored score for current document via cost method.
         */
        @Override
        public long cost() {
            if (resultsIterator.getCurrent() == null || docId == NO_MORE_DOCS) {
                return 0;
            } else {
                return resultsIterator.getCurrent().getValue();
            }
        }
    }
}
