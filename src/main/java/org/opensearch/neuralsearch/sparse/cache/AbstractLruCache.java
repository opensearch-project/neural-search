/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import lombok.extern.log4j.Log4j2;
import lombok.NonNull;

import java.util.Set;

/**
 * Abstract LRU cache implementation for sparse vector caches using ConcurrentLinkedHashMap.
 * This class provides common functionality for managing eviction of cache entries
 * based on least recently used policy with high-performance concurrent access.
 *
 * @param <Key> The type of key used for cache entries
 */
@Log4j2
public abstract class AbstractLruCache<Key extends LruCacheKey> {

    /**
     * Concurrent map to track access with LRU ordering
     * Uses ConcurrentLinkedHashMap for high-performance concurrent access
     */
    protected final ConcurrentLinkedHashMap<Key, Boolean> accessRecencyMap;

    protected AbstractLruCache() {
        this.accessRecencyMap = new ConcurrentLinkedHashMap.Builder<Key, Boolean>().maximumWeightedCapacity(Long.MAX_VALUE).build();
    }

    /**
     * Updates access to an item for a specific cache key.
     * This updates the item's position in the LRU order.
     * Uses ConcurrentLinkedHashMap's high performance operations.
     *
     * @param key The key being accessed
     */
    protected void updateAccess(Key key) {
        if (key == null) {
            return;
        }

        accessRecencyMap.put(key, true);
    }

    /**
     * Retrieves the least recently used key without affecting its position in the access order.
     * Uses ConcurrentLinkedHashMap's efficient ascendingKeySetWithLimit() method.
     *
     * @return The least recently used key, or null if the cache is empty
     */
    protected Key getLeastRecentlyUsedItem() {
        Set<Key> keySet = accessRecencyMap.ascendingKeySetWithLimit(1);
        return keySet.isEmpty() ? null : keySet.iterator().next();
    }

    /**
     * Evicts least recently used items from cache until the specified amount of RAM has been freed.
     * Uses ConcurrentLinkedHashMap for efficient concurrent eviction.
     *
     * @param ramBytesToRelease Number of bytes to evict
     */
    public void evict(long ramBytesToRelease) {
        if (ramBytesToRelease <= 0) {
            return;
        }

        long ramBytesReleased = 0;

        // Synchronizing the access recency map for thread safety
        synchronized (accessRecencyMap) {
            // Continue evicting until we've freed enough memory or the cache is empty
            while (ramBytesReleased < ramBytesToRelease) {
                // Get the least recently used item
                Key leastRecentlyUsedKey = getLeastRecentlyUsedItem();

                if (leastRecentlyUsedKey == null) {
                    // Cache is empty, nothing more to evict
                    break;
                }

                // Evict the item and track bytes freed
                ramBytesReleased += evictItem(leastRecentlyUsedKey);
            }
        }

        log.debug("Freed {} bytes of memory", ramBytesReleased);
    }

    /**
     * Evicts a specific item from the cache.
     * Uses ConcurrentLinkedHashMap's atomic remove operation.
     *
     * @param key The key to evict
     * @return number of bytes freed, or 0 if the item was not evicted
     */
    protected long evictItem(Key key) {
        if (accessRecencyMap.remove(key) == null) {
            return 0;
        }

        return doEviction(key);
    }

    /**
     * Removes all entries for a specific cache key when an index is removed.
     * Uses ConcurrentLinkedHashMap's concurrent iteration capabilities.
     *
     * @param cacheKey The cache key to remove
     */
    public void onIndexRemoval(@NonNull CacheKey cacheKey) {
        accessRecencyMap.entrySet().removeIf(entry -> entry.getKey().getCacheKey().equals(cacheKey));
    }

    /**
     * Performs the actual eviction of the item from cache.
     * Subclasses must implement this method to handle specific eviction logic.
     *
     * @param key The key to evict
     * @return number of bytes freed
     */
    protected abstract long doEviction(Key key);
}
