/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;

/**
 * Extension of SparseVectorWriter that supports cache management operations.
 * Provides memory management for cached sparse vectors.
 */
public interface CacheableSparseVectorWriter extends SparseVectorWriter {
    /**
     * Removes a sparse vector from the cached forward index.
     *
     * @param docId The document ID of the sparse vector to remove
     * @return The number of RAM bytes freed by this operation
     */
    long erase(int docId);
}
