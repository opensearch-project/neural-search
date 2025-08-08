/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.accessor;

import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;

/**
 * A functional interface for reading sparse vector data associated with document IDs.
 * This interface provides a single method to retrieve sparse vector representations.
 *
 * Implementations of this interface are responsible for efficiently retrieving
 * sparse vector data, which may involve accessing disk, memory, or other storage media.
 *
 */
@FunctionalInterface
public interface SparseVectorReader {

    /**
     * Reads and returns the sparse vector associated with the specified document ID.
     *
     * @param docId The document identifier for which to retrieve the sparse vector
     * @return The SparseVector object containing the vector data for the document
     * @throws IOException If an error occurs during the reading operation, such as
     *                     when accessing the underlying storage medium
     */
    SparseVector read(int docId) throws IOException;
}
