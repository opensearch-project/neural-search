/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;

/**
 * This is a customized BinaryDocValues for sparse vector. It is used to merge doc values from multiple segments.
 */
public class SparseBinaryDocValues extends BinaryDocValues {
    private final DocIDMerger<BinaryDocValuesSub> docIDMerger;

    @Getter
    private long totalLiveDocs;

    SparseBinaryDocValues(DocIDMerger<BinaryDocValuesSub> docIdMerger) {
        this.docIDMerger = docIdMerger;
    }

    private BinaryDocValuesSub current;
    private int docID = -1;

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
            docID = NO_MORE_DOCS;
        } else {
            docID = current.mappedDocID;
        }
        return docID;
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return current.getValues().binaryValue();
    }

    public SparseVector cachedSparseVector() throws IOException {
        if (this.current == null) return null;
        InMemoryKey.IndexKey key = this.current.getKey();
        if (key == null) return null;
        InMemorySparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.get(key);
        if (index == null) return null;
        SparseVectorReader reader = index.getReader();
        int oldDocId = this.current.getDocId();
        return reader.read(oldDocId);
    }

    /**
     * Builder pattern like setter for setting totalLiveDocs. We can use setter also. But this way the code is clean.
     * @param totalLiveDocs int
     * @return {@link SparseBinaryDocValues}
     */
    public SparseBinaryDocValues setTotalLiveDocs(long totalLiveDocs) {
        this.totalLiveDocs = totalLiveDocs;
        return this;
    }
}
