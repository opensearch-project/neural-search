/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;

/**
 * A DocValuesConsumer that writes sparse doc values to a segment.
 */
@Log4j2
public class SparseDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SegmentWriteState state;
    private final MergeHelper mergeHelper;

    public SparseDocValuesConsumer(
        @NonNull SegmentWriteState state,
        @NonNull DocValuesConsumer delegate,
        @NonNull MergeHelper mergeHelper
    ) {
        super();
        this.delegate = delegate;
        this.state = state;
        this.mergeHelper = mergeHelper;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addBinaryField(field, valuesProducer);
        // check field is the sparse field, otherwise return
        if (!SparseVectorField.isSparseField(field)) {
            return;
        }
        addSparseVectorBinary(field, valuesProducer, false);
    }

    private void addSparseVectorBinary(FieldInfo field, DocValuesProducer valuesProducer, boolean isMerge) throws IOException {
        if (!PredicateUtils.shouldRunSeisPredicate.test(this.state.segmentInfo, field)) {
            return;
        }
        BinaryDocValues binaryDocValues = valuesProducer.getBinary(field);
        CacheKey key = new CacheKey(this.state.segmentInfo, field);
        int docCount = this.state.segmentInfo.maxDoc();
        SparseVectorWriter writer = ForwardIndexCache.getInstance().getOrCreate(key, docCount).getWriter();
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            boolean written = false;
            if (isMerge) {
                SparseBinaryDocValues sparseBinaryDocValues = (SparseBinaryDocValues) binaryDocValues;
                SparseVector vector = sparseBinaryDocValues.cachedSparseVector();
                if (vector != null) {
                    writer.insert(docId, vector);
                    written = true;
                }
            }
            if (!written) {
                BytesRef bytesRef = binaryDocValues.binaryValue();
                writer.insert(docId, new SparseVector(bytesRef));
            }
            docId = binaryDocValues.nextDoc();
        }
        if (isMerge) {
            if (valuesProducer instanceof SparseDocValuesReader reader) {
                mergeHelper.clearCacheData(reader.getMergeStateFacade(), field, ForwardIndexCache.getInstance()::onIndexRemoval);
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
            MergeStateFacade mergeStateFacade = mergeHelper.convertToMergeStateFacade(mergeState);
            FieldInfos mergeFieldInfos = mergeStateFacade.getMergeFieldInfos();
            if (mergeFieldInfos == null) {
                return;
            }
            for (FieldInfo fieldInfo : mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY && SparseVectorField.isSparseField(fieldInfo)) {
                    addSparseVectorBinary(fieldInfo, mergeHelper.newSparseDocValuesReader(mergeStateFacade), true);
                }
            }
        } catch (Exception e) {
            log.error("Merge sparse doc values error", e);
        }
    }
}
