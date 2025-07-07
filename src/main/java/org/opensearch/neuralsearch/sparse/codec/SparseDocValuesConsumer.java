/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.MergeHelper;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;

/**
 * A DocValuesConsumer that writes sparse doc values to a segment.
 */
@Log4j2
public class SparseDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SegmentWriteState state;

    public SparseDocValuesConsumer(SegmentWriteState state, DocValuesConsumer delegate) {
        super();
        this.delegate = delegate;
        this.state = state;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addBinaryField(field, valuesProducer);
        // check field is the sparse field, otherwise return
        if (!SparseTokensField.isSparseField(field)) {
            return;
        }
        addBinary(field, valuesProducer, false);
    }

    private void addBinary(FieldInfo field, DocValuesProducer valuesProducer, boolean isMerge) throws IOException {
        if (!PredicateUtils.shouldRunSeisPredicate.test(this.state.segmentInfo, field)) {
            return;
        }
        BinaryDocValues binaryDocValues = valuesProducer.getBinary(field);
        InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(this.state.segmentInfo, field);
        int docCount = this.state.segmentInfo.maxDoc();
        SparseVectorWriter writer = InMemorySparseVectorForwardIndex.getOrCreate(key, docCount).getWriter();
        if (writer == null) {
            throw new IllegalStateException("Forward index writer is null");
        }
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            boolean written = false;
            if (isMerge) {
                SparseBinaryDocValues sparseBinaryDocValues = (SparseBinaryDocValues) binaryDocValues;
                SparseVector vector = sparseBinaryDocValues.cachedSparseVector();
                if (vector != null) {
                    writer.write(docId, vector);
                    written = true;
                }
            }
            if (!written) {
                BytesRef bytesRef = binaryDocValues.binaryValue();
                writer.write(docId, bytesRef);
            }
            docId = binaryDocValues.nextDoc();
        }
        if (isMerge) {
            if (valuesProducer instanceof SparseDocValuesReader) {
                SparseDocValuesReader reader = (SparseDocValuesReader) valuesProducer;
                MergeHelper.clearInMemoryData(reader.getMergeState(), field, SparseVectorForwardIndex::removeIndex);
            }
        }
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        this.delegate.merge(mergeState);
        try {
            assert mergeState != null;
            assert mergeState.mergeFieldInfos != null;
            for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY && SparseTokensField.isSparseField(fieldInfo)) {
                    addBinary(fieldInfo, new SparseDocValuesReader(mergeState), true);
                }
            }
        } catch (Exception e) {
            log.error("Merge sparse doc values error", e);
        }
    }
}
