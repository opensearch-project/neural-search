/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;

/**
 * Interface for sparse vector forward index
 */
public interface SparseVectorForwardIndex {
    SparseVectorReader getReader();  // covariant return type

    SparseVectorWriter getWriter();  // covariant return type

    interface SparseVectorWriter {
        void write(int docId, SparseVector vector) throws IOException;

        void write(int docId, BytesRef data) throws IOException;
    }

    static void removeIndex(InMemoryKey.IndexKey key) {
        InMemorySparseVectorForwardIndex.removeIndex(key);
    }
}
