/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.accessor;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Set;

/**
 * Interface for reading from clustered posting lists.
 * Provides methods to access term-based document clusters and metadata
 * from the underlying storage format used by the clustered posting implementation.
 */
public interface ClusteredPostingReader {

    /**
     * Reads and returns the document clusters associated with a specific term.
     *
     * @param term The term for which to retrieve document clusters, represented as a BytesRef
     * @return PostingClusters object containing the document clusters for the specified term
     */
    PostingClusters read(BytesRef term) throws IOException;

    /**
     * Returns the set of all terms stored in the clustered posting list.
     *
     * @return A Set of BytesRef objects representing all available terms
     */
    Set<BytesRef> getTerms();

    /**
     * Returns the total number of terms in the clustered posting list.
     *
     * @return The count of terms as a long value
     */
    long size();
}
