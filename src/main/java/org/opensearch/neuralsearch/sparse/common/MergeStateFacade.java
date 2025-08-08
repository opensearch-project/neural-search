/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Bits;

/**
 * Since {@link MergeState}'s fields are not capsulated and doesn't have accessors, it's not easy to
 * mock its functionalities in UTs, thus, we wrap it in this class to provide better testability.
 */
@AllArgsConstructor
public class MergeStateFacade {
    @NonNull
    private final MergeState mergeState;

    public DocValuesProducer[] getDocValuesProducers() {
        return mergeState.docValuesProducers;
    }

    public FieldInfos getMergeFieldInfos() {
        return mergeState.mergeFieldInfos;
    }

    public int[] getMaxDocs() {
        return mergeState.maxDocs;
    }

    public MergeState.DocMap[] getDocMaps() {
        return mergeState.docMaps;
    }

    public Bits[] getLiveDocs() {
        return mergeState.liveDocs;
    }
}
