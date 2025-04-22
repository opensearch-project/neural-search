/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.io.IOException;

/**
 * PostingsEnum for sparse vector
 */
public class SparsePostingsEnum extends PostingsEnum {
    private final PostingClusters clusters;
    @Getter
    private final InMemoryKey.IndexKey indexKey;
    private IteratorWrapper<DocumentCluster> currentCluster;
    private DocFreqIterator currentDocFreq;

    public SparsePostingsEnum(PostingClusters clusters, InMemoryKey.IndexKey indexKey) {
        this.clusters = clusters;
        this.indexKey = indexKey;
        currentCluster = clusterIterator();
        positionIterators();
    }

    private void positionIterators() {
        if (currentDocFreq == null && currentCluster != null) {
            currentDocFreq = currentCluster.next().getDisi();
        }
        while (currentDocFreq.docID() == DocIdSetIterator.NO_MORE_DOCS) {
            if (!currentCluster.hasNext()) {
                break;
            }
            currentDocFreq = currentCluster.next().getDisi();
        }
    }

    public IteratorWrapper<DocumentCluster> clusterIterator() {
        return new IteratorWrapper<DocumentCluster>(this.clusters.iterator());
    }

    @Override
    public int freq() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int startOffset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef getPayload() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
        if (currentDocFreq == null) {
            return -1;
        }
        return currentDocFreq.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        if (currentDocFreq == null) {
            return -1;
        }
        int doc = currentDocFreq.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            positionIterators();
            return currentDocFreq.nextDoc();
        }
        return doc;
    }

    public DocFreq docFreq() {
        if (currentDocFreq == null) {
            return null;
        }
        return currentDocFreq.docFreq();
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return 0;
    }
}
