/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.extern.log4j.Log4j2;
import lombok.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Abstract LRU cache implementation for sparse vector caches.
 * This class provides common functionality for managing eviction of cache entries
 * based on least recently used policy.
 *
 * @param <Key> The type of key used for cache entries
 */
@Log4j2
public abstract class AbstractLruCache<Key extends LruCacheKey> {

    /**
     * Map to track access with LRU ordering
     * We use Map instead of set because only linked hash map supports tracking the access order of items
     */
    protected final Map<Key, Boolean> accessRecencyMap;

    protected AbstractLruCache() {
        this.accessRecencyMap = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true));
    }

    /**
     * Updates access to an item for a specific cache key.
     * This updates the item's position in the LRU order.
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
     *
     * @return The least recently used key, or null if the cache is empty
     */
    protected Key getLeastRecentlyUsedItem() {
        // With accessOrder is true in the LinkedHashMap, the first entry is the least recently used
        Iterator<Map.Entry<Key, Boolean>> iterator = accessRecencyMap.entrySet().iterator();
        if (iterator.hasNext()) {
            return iterator.next().getKey();
        }
        return null;
    }

    /**
     * Evicts least recently used items from cache until the specified amount of RAM has been freed.
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
     *
     * @param cacheKey The cache key to remove
     */
    public void removeIndex(@NonNull CacheKey cacheKey) {
        accessRecencyMap.keySet().removeIf(key -> key.getCacheKey().equals(cacheKey));
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
