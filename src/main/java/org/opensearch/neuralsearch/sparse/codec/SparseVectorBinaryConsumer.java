/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

/**
 * A writer of sparse vector side files, invoked by {@link BaseSparseDocValuesConsumer} once per
 * flush and once per merge. Implementations decide what to build from the vectors — Lucene-side
 * clustered postings or a native engine file — and which fields they own.
 */
public abstract class SparseVectorBinaryConsumer {
    /**
     * Writes one field's sparse data for a flush.
     *
     * @param field         the field being flushed
     * @param valuesProducer source of the field's binary doc values
     */
    abstract public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException;

    /**
     * Writes the sparse data for the merged segment.
     *
     * @param fieldInfos the sparse fields present in the merge
     * @param facade     the merge state the source segments are read through
     */
    abstract public void merge(List<FieldInfo> fieldInfos, MergeStateFacade facade) throws IOException;
}
