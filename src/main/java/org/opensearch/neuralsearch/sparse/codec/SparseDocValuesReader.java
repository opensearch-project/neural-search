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
import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It produces SparseDocValues when segments merge happens. It does the actual work for sparse vector merge.
 */
public class SparseDocValuesReader extends EmptyDocValuesProducer {
    @Getter
    private final MergeState mergeState;

    public SparseDocValuesReader(MergeState state) {
        this.mergeState = state;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        long totalLiveDocs = 0;
        try {
            List<BinaryDocValuesSub> subs = new ArrayList<>(this.mergeState.docValuesProducers.length);
            for (int i = 0; i < this.mergeState.docValuesProducers.length; i++) {
                BinaryDocValues values = null;
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                if (docValuesProducer != null) {
                    FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(field.name);
                    if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                        values = docValuesProducer.getBinary(readerFieldInfo);
                    }
                    if (values != null) {
                        InMemoryKey.IndexKey key = null;
                        if (values instanceof SparseBinaryDocValuesPassThrough) {
                            SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough = (SparseBinaryDocValuesPassThrough) values;
                            key = new InMemoryKey.IndexKey(sparseBinaryDocValuesPassThrough.getSegmentInfo(), field);
                        }
                        totalLiveDocs = totalLiveDocs + getLiveDocsCount(values, this.mergeState.liveDocs[i]);
                        // docValues will be consumed when liveDocs are not null, hence resetting the docsValues
                        // pointer.
                        values = this.mergeState.liveDocs[i] != null ? docValuesProducer.getBinary(readerFieldInfo) : values;
                        subs.add(new BinaryDocValuesSub(mergeState.docMaps[i], values, key));
                    }
                }
            }
            return new SparseBinaryDocValues(DocIDMerger.of(subs, mergeState.needsIndexSort)).setTotalLiveDocs(totalLiveDocs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
