/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

/**
 * Interface for sparse vector forward index
 */
public interface SparseVectorForwardIndex {
    SparseVectorReader getReader();  // covariant return type

    SparseVectorWriter getWriter();  // covariant return type

    static void removeIndex(InMemoryKey.IndexKey key) {
        InMemorySparseVectorForwardIndex.removeIndex(key);
    }
}
