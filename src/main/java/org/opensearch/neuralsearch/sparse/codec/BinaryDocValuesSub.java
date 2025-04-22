/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.MergeState;

import java.io.IOException;

/**
 * It holds binary doc values sub for each segment. It is used to merge doc values from multiple segments.
 */
public class BinaryDocValuesSub extends DocIDMerger.Sub {

    private final BinaryDocValues values;

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values) {
        super(docMap);
        if (values == null || (values.docID() != -1)) {
            throw new IllegalStateException("Doc values is either null or docID is not -1 ");
        }
        this.values = values;
    }

    @Override
    public int nextDoc() throws IOException {
        return values.nextDoc();
    }

    public BinaryDocValues getValues() {
        return values;
    }
}
