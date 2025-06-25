/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;

import java.io.IOException;

/**
 * PostingsEnum for sparse vector
 */
public class SparsePostingsEnum extends PostingsEnum {
    @Getter
    private final PostingClusters clusters;
    @Getter
    private final InMemoryKey.IndexKey indexKey;
    private IteratorWrapper<DocumentCluster> currentCluster;
    private DocFreqIterator currentDocFreq;

    public SparsePostingsEnum(PostingClusters clusters, InMemoryKey.IndexKey indexKey) throws IOException {
        this.clusters = clusters;
        this.indexKey = indexKey;
        currentCluster = clusterIterator();
        currentDocFreq = currentCluster.next().getDisi();
    }

    public IteratorWrapper<DocumentCluster> clusterIterator() {
        return this.clusters.iterator();
    }

    public int size() {
        return clusters.getSize();
    }

    @Override
    public int freq() throws IOException {
        assert this.currentDocFreq != null;
        return ByteQuantizer.getUnsignedByte(this.currentDocFreq.freq());
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
        assert currentDocFreq != null;
        int doc = currentDocFreq.nextDoc();
        while (doc == DocIdSetIterator.NO_MORE_DOCS) {
            if (!currentCluster.hasNext()) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            currentDocFreq = currentCluster.next().getDisi();
            doc = currentDocFreq.nextDoc();
        }
        return doc;
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
