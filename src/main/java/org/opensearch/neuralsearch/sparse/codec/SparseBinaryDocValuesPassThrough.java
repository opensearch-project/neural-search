/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;

@AllArgsConstructor
public class SparseBinaryDocValuesPassThrough extends BinaryDocValues implements SparseVectorReader {
    private final BinaryDocValues delegate;
    @Getter
    private final SegmentInfo segmentInfo;

    @Override
    public BytesRef binaryValue() throws IOException {
        return this.delegate.binaryValue();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return this.delegate.advanceExact(target);
    }

    @Override
    public int docID() {
        return this.delegate.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return this.delegate.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return this.delegate.advance(target);
    }

    @Override
    public long cost() {
        return this.delegate.cost();
    }

    @Override
    public SparseVector read(int docId) throws IOException {
        if (!advanceExact(docId)) {
            return null;
        }
        BytesRef bytesRef = binaryValue();
        if (bytesRef == null) {
            return null;
        }
        return new SparseVector(bytesRef);
    }
}
