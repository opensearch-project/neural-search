/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;

/**
 * Extension of ClusteredPostingWriter that supports cache management operations.
 * Provides memory management for cached posting lists.
 */
public interface CacheableClusteredPostingWriter extends ClusteredPostingWriter {
    /**
     * Removes a term and its associated document clusters from the posting list.
     *
     * @param term The term to be removed
     * @return The number of RAM bytes freed by this operation
     */
    long erase(BytesRef term);
}
