/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.extern.log4j.Log4j2;
import lombok.NonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract LRU cache implementation for sparse vector caches.
 * This class provides common functionality for managing eviction of cache entries
 * based on least recently used policy.
 *
 * This implementation uses lock-free data structures for cache access tracking
 * to minimize contention under concurrent load, with locking only during eviction.
 *
 * @param <Key> The type of key used for cache entries
 */
@Log4j2
public abstract class AbstractLruCache<Key extends LruCacheKey> {

    /**
     * Lock-free cache storage for tracking which keys are currently in the cache
     * The Integer value represents the count of keys in the accessOrder deque
     */
    protected final ConcurrentHashMap<Key, Integer> cache;

    /**
     * Lock-free access order tracking using a deque
     * Keys are added to the end on access, and eviction removes from the front
     * May contain duplicate keys temporarily, which will be cleaned up during eviction
     */
    protected final ConcurrentLinkedDeque<Key> accessOrder;

    /**
     * Lock used only for eviction operations to ensure atomicity
     */
    private final ReentrantLock evictionLock;

    protected AbstractLruCache() {
        this.cache = new ConcurrentHashMap<>();
        this.accessOrder = new ConcurrentLinkedDeque<>();
        this.evictionLock = new ReentrantLock();
    }

    /**
     * Updates access to an item for a specific cache key.
     * This updates the item's position in the LRU order using a lock-free operation.
     * The key is added to the cache map with an incremented count and appended to the access order deque.
     * The count tracks how many duplicates of this key exist in the deque.
     *
     * @param key The key being accessed
     */
    protected void updateAccess(Key key) {
        if (key == null) {
            return;
        }

        cache.compute(key, (k, count) -> (count == null) ? 1 : count + 1);

        accessOrder.addLast(key);
    }

    /**
     * Retrieves the least recently used key from the front of the access order deque.
     * Uses count-based duplicate handling to find and clean up the true LRU item.
     *
     * This method polls from the deque front and handles duplicates by decrementing counts
     * until it finds a key with count = 1 (the true LRU item).
     *
     * @return The least recently used key, or null if the cache is empty
     */
    protected Key getLeastRecentlyUsedItem() {
        Key candidate;
        while ((candidate = accessOrder.pollFirst()) != null) {
            Integer count = cache.get(candidate);

            if (count == null) {
                continue;
            }

            if (count == 1) {
                return candidate;
            } else {
                cache.put(candidate, count - 1);
            }
        }
        return null;
    }

    /**
     * Evicts least recently used items from cache until the specified amount of RAM has been freed.
     * Uses lock to ensure all threads get a chance to evict when needed.
     *
     * @param ramBytesToRelease Number of bytes to evict
     */
    public void evict(long ramBytesToRelease) {
        if (ramBytesToRelease <= 0) {
            return;
        }

        evictionLock.lock();

        try {
            long ramBytesReleased = 0;

            // Continue evicting until we've freed enough memory or the cache is empty
            while (ramBytesReleased < ramBytesToRelease) {
                // Get and remove the least recently used item
                Key leastRecentlyUsedKey = getLeastRecentlyUsedItem();

                if (leastRecentlyUsedKey == null) {
                    // Cache is empty, nothing more to evict
                    break;
                }

                // Evict the item and track bytes freed
                ramBytesReleased += evictItem(leastRecentlyUsedKey);
            }

            log.debug("Freed {} bytes of memory", ramBytesReleased);
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Evicts a specific item from the cache.
     * Removes the key from both cache storage and access order deque to minimize memory usage.
     *
     * @param key The key to evict
     * @return number of bytes freed, or 0 if the item was not evicted
     */
    protected long evictItem(Key key) {
        if (cache.remove(key) == null) {
            return 0;
        }

        accessOrder.removeIf(k -> k.equals(key));

        return doEviction(key);
    }

    /**
     * Removes all entries for a specific cache key when an index is removed.
     * Acquires the eviction lock to ensure atomicity with ongoing evictions.
     *
     * @param cacheKey The cache key to remove
     */
    public void onIndexRemoval(@NonNull CacheKey cacheKey) {
        evictionLock.lock();
        try {
            cache.entrySet().removeIf(entry -> entry.getKey().getCacheKey().equals(cacheKey));

            accessOrder.removeIf(key -> key.getCacheKey().equals(cacheKey));
        } finally {
            evictionLock.unlock();
        }
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
