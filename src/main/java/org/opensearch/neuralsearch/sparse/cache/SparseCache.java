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
import java.util.function.Function;

/**
 * Abstract base class for sparse data caching implementations.
 * Provides common functionality for caching forward index and clustered postings.
 * @param <T> The type of Accountable objects stored in the cache
 */
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
    public void removeIndex(@NonNull CacheKey key) {
        cacheMap.computeIfPresent(key, (k, value) -> {
            long ramBytesUsed = value.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(key);
            MemoryUsageManager.getInstance().getMemoryUsageTracker().safeRecord(-ramBytesUsed, CircuitBreakerManager::addWithoutBreaking);
            LruTermCache.getInstance().removeIndex(key);
            LruDocumentCache.getInstance().removeIndex(key);
            return null;
        });
    }

    /**
     * Retrieves an existing cached value for the given key or creates a new one if not present.
     * The method is thread-safe and handles memory accounting with the CircuitBreakerManager.
     *
     * @param key The cache key to look up or create an entry for
     * @param creator A function that creates a new value when the key is not found
     * @return The existing or newly created value associated with the key
     */
    @NonNull
    protected T getOrCreate(@NonNull CacheKey key, @NonNull Function<CacheKey, T> creator) {
        return cacheMap.computeIfAbsent(key, k -> {
            T value = creator.apply(k);
            CircuitBreakerManager.addWithoutBreaking(RamUsageEstimator.shallowSizeOf(k));
            return value;
        });
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
