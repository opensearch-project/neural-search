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
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;

/**
 * A pass-through wrapper for BinaryDocValues that provides sparse vector reading capabilities.
 */
@AllArgsConstructor
public class SparseBinaryDocValuesPassThrough extends BinaryDocValues implements SparseVectorReader {

    /** The underlying BinaryDocValues instance that handles the actual binary data operations */
    private final BinaryDocValues delegate;

    /** The segment information associated with this binary doc values instance */
    @Getter
    private final SegmentInfo segmentInfo;

    /** The byte quantizer instance */
    private final ByteQuantizer byteQuantizer;

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
    public synchronized SparseVector read(int docId) throws IOException {
        if (!advanceExact(docId)) {
            return null;
        }
        BytesRef bytesRef = binaryValue();
        if (bytesRef == null) {
            return null;
        }
        return new SparseVector(bytesRef, byteQuantizer);
    }
}
