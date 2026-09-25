/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A DocValuesConsumer that writes sparse doc values to a segment.
 */
@Log4j2
public class BaseSparseDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SparseVectorBinaryConsumer sparseDocValuesConsumer;
    private final SparseVectorBinaryConsumer nativeDocValuesConsumer;
    private final MergeHelper mergeHelper;

    public BaseSparseDocValuesConsumer(
        @NonNull DocValuesConsumer delegate,
        @NonNull SparseVectorBinaryConsumer sparseDocValuesConsumer,
        @NonNull SparseVectorBinaryConsumer nativeDocValuesConsumer,
        @NonNull MergeHelper mergeHelper
    ) {
        super();
        this.delegate = delegate;
        this.sparseDocValuesConsumer = sparseDocValuesConsumer;
        this.nativeDocValuesConsumer = nativeDocValuesConsumer;
        this.mergeHelper = mergeHelper;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addBinaryField(field, valuesProducer);
        SparseEngine sparseEngine = SparseFieldUtils.getSparseEngine(field);
        if (sparseEngine == SparseEngine.NATIVE) {
            this.nativeDocValuesConsumer.addBinaryField(field, valuesProducer);
        } else {
            this.sparseDocValuesConsumer.addBinaryField(field, valuesProducer);
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
        try {
            this.delegate.merge(mergeState);
            assert mergeState != null;
            MergeStateFacade mergeStateFacade = mergeHelper.convertToMergeStateFacade(mergeState);
            FieldInfos mergeFieldInfos = mergeStateFacade.getMergeFieldInfos();
            if (mergeFieldInfos == null) {
                return;
            }
            List<FieldInfo> sparseFieldInfos = new ArrayList<>();
            List<FieldInfo> nativeFieldInfos = new ArrayList<>();
            for (FieldInfo fieldInfo : mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY && SparseVectorField.isSparseField(fieldInfo)) {
                    SparseEngine sparseEngine = SparseFieldUtils.getSparseEngine(fieldInfo);
                    if (sparseEngine == SparseEngine.NATIVE) {
                        nativeFieldInfos.add(fieldInfo);
                    } else {
                        sparseFieldInfos.add(fieldInfo);
                    }
                }
            }
            if (!sparseFieldInfos.isEmpty()) {
                this.sparseDocValuesConsumer.merge(sparseFieldInfos, mergeStateFacade);
            }
            if (!nativeFieldInfos.isEmpty()) {
                this.nativeDocValuesConsumer.merge(nativeFieldInfos, mergeStateFacade);
            }
        } catch (Exception e) {
            // Rethrow: swallowing this leaves the side files half written while Lucene believes the
            // merge succeeded, so it commits a segment whose sparse files are truncated. Only the
            // exception reaching Lucene aborts the merge and deletes what was written.
            log.error("Merge sparse doc values error", e);
            throw e;
        }
    }
}
