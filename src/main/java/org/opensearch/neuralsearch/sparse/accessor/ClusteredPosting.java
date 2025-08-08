/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.accessor;

/**
 * Interface for clustered posting lists implementation.
 * Document IDs are partitioned in a way that improves retrieval
 * efficiency by clustering similar documents together.
 */
public interface ClusteredPosting {

    /**
     * Returns a reader for accessing the clustered posting list data.
     *
     * @return A ClusteredPostingReader instance for reading from the posting list
     */
    ClusteredPostingReader getReader();  // covariant return type

    /**
     * Returns a writer for modifying the clustered posting list data.
     *
     * @return A ClusteredPostingWriter instance for writing to the posting list
     */
    ClusteredPostingWriter getWriter();  // covariant return type
}
