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
     * The Integer value represents the count of duplicates in the accessOrder deque
     */
    protected final ConcurrentHashMap<Key, Integer> cache;

    /**
     * Lock-free access order tracking using a deque
     * Keys are added to the end on access, and eviction removes from the front
     * May contain duplicate keys temporarily, which are cleaned up during eviction
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

        // Increment count in cache storage (lock-free)
        // This tracks how many times this key appears in the deque
        cache.compute(key, (k, count) -> (count == null) ? 1 : count + 1);

        // Add to access order tracking (lock-free)
        // This creates duplicates in the deque, but we track them with counts
        accessOrder.addLast(key);
    }

    /**
     * Retrieves and removes the least recently used key from the front of the access order deque.
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
                // Key was already evicted, continue to next
                continue;
            }

            if (count == 1) {
                // This is the last occurrence, return it as LRU
                return candidate;
            } else {
                // This is a duplicate, decrement count and continue
                cache.put(candidate, count - 1);
            }
        }
        return null;
    }

    /**
     * Evicts least recently used items from cache until the specified amount of RAM has been freed.
     * Uses tryLock to allow only one thread to evict at a time, with other threads skipping eviction.
     *
     * @param ramBytesToRelease Number of bytes to evict
     */
    public void evict(long ramBytesToRelease) {
        if (ramBytesToRelease <= 0) {
            return;
        }

        // Try to acquire the eviction lock
        // If another thread is already evicting, skip this eviction
        if (!evictionLock.tryLock()) {
            log.debug("Skipping eviction, another thread is already evicting");
            return;
        }

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
     * Removes the key from the cache storage only.
     * Stale entries in the accessOrder deque will be cleaned up naturally during subsequent evictions.
     *
     * @param key The key to evict
     * @return number of bytes freed, or 0 if the item was not evicted
     */
    protected long evictItem(Key key) {
        // Remove from cache storage
        if (cache.remove(key) == null) {
            return 0;
        }

        // Note: We don't remove from accessOrder deque here
        // Stale entries will be skipped in pollLeastRecentlyUsedItem()

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
            // Remove matching entries from cache storage
            cache.entrySet().removeIf(entry -> entry.getKey().getCacheKey().equals(cacheKey));

            // Note: Stale entries in accessOrder deque will be cleaned up naturally
            // during eviction as they won't be found in cache
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
