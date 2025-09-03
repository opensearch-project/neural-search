/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.MergeState;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;

import java.io.IOException;

/**
 * It holds binary doc values sub for each segment. It is used to merge doc values from multiple segments.
 */
@Getter
public class BinaryDocValuesSub extends DocIDMerger.Sub {

    private final BinaryDocValues values;
    private final CacheKey key;
    private int docId = 0;

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values, CacheKey key) {
        super(docMap);
        if (values == null || (values.docID() != -1)) {
            throw new IllegalStateException("Doc values is either null or docID is not -1 ");
        }
        this.values = values;
        this.key = key;
    }

    @Override
    public int nextDoc() throws IOException {
        docId = values.nextDoc();
        return docId;
    }
}
