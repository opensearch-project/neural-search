/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;

/**
 * An enumerator for seismic's posting list with clusters, it traverses clusters and docs.
 */
public class SparsePostingsEnum extends PostingsEnum {
    @Getter
    private final PostingClusters clusters;
    @Getter
    private final CacheKey cacheKey;
    private final IteratorWrapper<DocumentCluster> currentCluster;
    private DocWeightIterator currentDocWeight;

    public SparsePostingsEnum(PostingClusters clusters, CacheKey cacheKey) throws IOException {
        this.clusters = clusters;
        this.cacheKey = cacheKey;
        currentCluster = clusterIterator();
        currentDocWeight = currentCluster.next().getDisi();
    }

    public IteratorWrapper<DocumentCluster> clusterIterator() {
        return this.clusters.iterator();
    }

    public int size() {
        return clusters.getSize();
    }

    @Override
    public int freq() throws IOException {
        assert this.currentDocWeight != null;
        return ByteQuantizationUtil.getUnsignedByte(this.currentDocWeight.weight());
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
        if (currentDocWeight == null) {
            return -1;
        }
        return currentDocWeight.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        assert currentDocWeight != null;
        int doc = currentDocWeight.nextDoc();
        while (doc == DocIdSetIterator.NO_MORE_DOCS) {
            if (!currentCluster.hasNext()) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            currentDocWeight = currentCluster.next().getDisi();
            doc = currentDocWeight.nextDoc();
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
