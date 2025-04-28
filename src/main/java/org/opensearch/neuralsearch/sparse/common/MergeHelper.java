/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;

import java.io.IOException;
import java.util.function.Consumer;

public class MergeHelper {
    public static void clearInMemoryData(MergeState mergeState, FieldInfo fieldInfo, Consumer<InMemoryKey.IndexKey> consumer)
        throws IOException {
        for (DocValuesProducer producer : mergeState.docValuesProducers) {
            for (FieldInfo field : mergeState.mergeFieldInfos) {
                if (!SparseTokensField.isSparseField(field) || (fieldInfo != null && field != fieldInfo)) {
                    continue;
                }
                BinaryDocValues binaryDocValues = producer.getBinary(field);
                if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                    continue;
                }
                SparseBinaryDocValuesPassThrough binaryDocValuesPassThrough = (SparseBinaryDocValuesPassThrough) binaryDocValues;
                InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(binaryDocValuesPassThrough.getSegmentInfo(), field);
                consumer.accept(key);
            }
        }
    }
}
