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
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Base scorer for seismic sparse vector queries with cluster-based optimization.
 * Maintains score heap and visited document tracking for efficient search.
 */
@Log4j2
public abstract class SeismicBaseScorer extends Scorer {
    private final static int SEISMIC_HEAP_SIZE = 10;
    protected final HeapWrapper<Integer> scoreHeap;
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
        scoreHeap = new HeapWrapper<Integer>(SEISMIC_HEAP_SIZE);
        initialize(leafReader);
    }

    protected void initialize(LeafReader leafReader) throws IOException {
        Terms terms = Terms.getTerms(leafReader, fieldName);
        for (String token : sparseQueryContext.getTokens()) {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term = new BytesRef(token);
            if (!termsEnum.seekExact(term)) {
                continue;
            }
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            if (!(postingsEnum instanceof SparsePostingsEnum sparsePostingsEnum)) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "posting enum is not SparsePostingsEnum, actual type: %s",
                        postingsEnum == null ? null : postingsEnum.getClass().getName()
                    )
                );
            }
            subScorers.add(new SingleScorer(sparsePostingsEnum));
        }
    }

    /**
     * Performs upfront search across all sub-scorers and returns top results.
     */
    protected List<Pair<Integer, Integer>> searchUpfront(int resultSize) throws IOException {
        HeapWrapper<Integer> resultHeap = new HeapWrapper<>(resultSize);
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
                            && score < (Integer) (Objects.requireNonNull(scoreHeap.peek()).getRight()) / sparseQueryContext
                                .getHeapFactor()) {
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
}
