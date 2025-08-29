/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

/**
 * Interface for objects that can be used as keys in an LRU cache.
 * Implementations must provide a method to retrieve the associated cache key.
 */
public interface LruCacheKey {
    /**
     * Returns the cache key associated with this object.
     *
     * @return the cache key
     */
    CacheKey getCacheKey();
}
