/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SparseCache<T extends Accountable> implements Accountable {

    protected final Map<CacheKey, T> cacheMap = new ConcurrentHashMap<>();

    /**
     * Remove a specific index from cache.
     * This method delegates to the CacheSparseVectorForwardIndex implementation
     * to clean up resources associated with the specified cache key.
     * This function is marked as synchronized for thread safety.
     *
     * @param key The CacheKey that identifies the index to be removed
     */
    public synchronized void removeIndex(@NonNull CacheKey key) {
        T value = cacheMap.remove(key);
        if (value == null) {
            return;
        }
        long ramBytesUsed = value.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(key);
        CircuitBreakerManager.releaseBytes(ramBytesUsed);
        LruTermCache.getInstance().removeIndex(key);
        LruDocumentCache.getInstance().removeIndex(key);
    }

    /**
     * Gets an existing instance.
     *
     * @param key The CacheKey that identifies the index
     * @return The instance associated with the key, or null if not found
     */
    public T get(@NonNull CacheKey key) {
        return cacheMap.get(key);
    }

    @Override
    public long ramBytesUsed() {
        long mem = RamUsageEstimator.shallowSizeOf(cacheMap);
        for (Map.Entry<CacheKey, T> entry : cacheMap.entrySet()) {
            mem += RamUsageEstimator.shallowSizeOf(entry.getKey());
            mem += entry.getValue().ramBytesUsed();
        }
        return mem;
    }
}
