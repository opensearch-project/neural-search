/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizerUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DocValues producer for sparse vector fields that wraps a delegate producer
 * and provides sparse-specific binary doc values handling.
 */
public class SparseDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer delegate;
    @Getter
    private final SegmentReadState state;

    private final Map<FieldInfo, ByteQuantizer> byteQuantizerMap = new HashMap<>();

    /**
     * Creates a new sparse doc values producer.
     *
     * @param state the segment read state
     * @param delegate the underlying doc values producer to delegate to
     */
    public SparseDocValuesProducer(SegmentReadState state, DocValuesProducer delegate) {
        super();
        this.state = state;
        this.delegate = delegate;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return this.delegate.getNumeric(field);
    }

    /**
     * Returns binary doc values for sparse vector fields with pass-through handling.
     */
    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        if (!byteQuantizerMap.containsKey(field)) {
            byteQuantizerMap.put(field, ByteQuantizerUtil.getByteQuantizerIngest(field));
        }

        return new SparseBinaryDocValuesPassThrough(this.delegate.getBinary(field), state.segmentInfo, byteQuantizerMap.get(field));
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return this.delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return this.delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return this.delegate.getSortedSet(field);
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return this.delegate.getSkipper(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        this.delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
