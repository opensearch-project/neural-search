/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.accessor;

/**
 * Interface for sparse vector forward index.
 * A forward index provides direct mapping from document IDs to their corresponding sparse vector representations.
 * This interface defines methods to access readers and writers for the sparse vector data,
 * as well as utility methods for index management.
 */
public interface SparseVectorForwardIndex {

    /**
     * Returns a reader for accessing sparse vectors from the forward index.
     * The reader provides methods to retrieve sparse vector data by document ID.
     *
     * @return A SparseVectorReader instance for reading sparse vector data
     */
    SparseVectorReader getReader();  // covariant return type

    /**
     * Returns a writer for adding or updating sparse vectors in the forward index.
     * The writer provides methods to store sparse vector data associated with document IDs.
     *
     * @return A SparseVectorWriter instance for writing sparse vector data
     */
    SparseVectorWriter getWriter();  // covariant return type
}
