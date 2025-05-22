/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

/**
 * Interface for sparse vector forward index
 */
public interface SparseVectorForwardIndex extends ForwardIndex {
    @Override
    SparseVectorForwardIndexReader getForwardIndexReader();  // covariant return type

    @Override
    SparseVectorForwardIndexWriter getForwardIndexWriter();  // covariant return type

    interface SparseVectorForwardIndexReader extends ForwardIndex.ForwardIndexReader {
        SparseVector readSparseVector(int docId);
    }

    interface SparseVectorForwardIndexWriter extends ForwardIndex.ForwardIndexWriter {
        void write(int docId, SparseVector vector);
    }

    static void removeIndex(InMemoryKey.IndexKey key) {
        InMemorySparseVectorForwardIndex.removeIndex(key);
    }
}
