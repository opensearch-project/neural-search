/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It produces SparseDocValues when segments merge happens. It does the actual work for sparse vector merge.
 */
public class SparseDocValuesReader extends EmptyDocValuesProducer {
    @Getter
    private final MergeStateFacade mergeStateFacade;

    public SparseDocValuesReader(MergeStateFacade mergeStateFacade) {
        this.mergeStateFacade = mergeStateFacade;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        long totalLiveDocs = 0;
        int size = this.mergeStateFacade.getDocValuesProducers().length;
        List<BinaryDocValuesSub> subs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            DocValuesProducer docValuesProducer = mergeStateFacade.getDocValuesProducers()[i];
            if (docValuesProducer == null) {
                continue;
            }
            BinaryDocValues values = null;
            FieldInfo readerFieldInfo = mergeStateFacade.getFieldInfos()[i].fieldInfo(field.getName());
            if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                values = docValuesProducer.getBinary(readerFieldInfo);
            }
            if (values != null) {
                CacheKey key = null;
                if (values instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough) {
                    key = new CacheKey(sparseBinaryDocValuesPassThrough.getSegmentInfo(), field);
                }
                totalLiveDocs = totalLiveDocs + getLiveDocsCount(values, mergeStateFacade.getLiveDocs()[i]);
                // docValues will be consumed when liveDocs are not null, hence resetting the docsValues
                // pointer.
                values = mergeStateFacade.getLiveDocs()[i] != null ? docValuesProducer.getBinary(readerFieldInfo) : values;
                subs.add(new BinaryDocValuesSub(mergeStateFacade.getDocMaps()[i], values, key));
            }
        }
        if (subs.isEmpty()) {
            return new SparseBinaryDocValues(null);
        }
        return new SparseBinaryDocValues(DocIDMerger.of(subs, mergeStateFacade.needIndexSort())).setTotalLiveDocs(totalLiveDocs);
    }

    private long getLiveDocsCount(final BinaryDocValues binaryDocValues, final Bits liveDocsBits) throws IOException {
        long liveDocs = 0;
        if (liveDocsBits != null) {
            int docId;
            for (docId = binaryDocValues.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = binaryDocValues.nextDoc()) {
                if (liveDocsBits.get(docId)) {
                    liveDocs++;
                }
            }
        } else {
            liveDocs = binaryDocValues.cost();
        }
        return liveDocs;
    }
}
