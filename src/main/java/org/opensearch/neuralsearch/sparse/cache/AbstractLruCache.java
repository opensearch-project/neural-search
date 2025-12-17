/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.extern.log4j.Log4j2;
import lombok.NonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract LRU cache implementation using Second Chance (Clock) algorithm for sparse vector caches.
 * This class provides common functionality for managing eviction of cache entries
 * based on an approximation of least recently used policy with lock-free access operations.
 *
 * The Second Chance algorithm uses a circular buffer with reference bits to approximate LRU
 * behavior while providing excellent concurrency characteristics.
 * https://www.geeksforgeeks.org/operating-systems/second-chance-or-clock-page-replacement-policy/
 *
 * @param <Key> The type of key used for cache entries
 */
@Log4j2
public abstract class AbstractLruCache<Key extends LruCacheKey> {

    /**
     * Hash map for cache storage and reference bit tracking
     * Boolean value represents the reference bit (true = recently accessed)
     */
    private final ConcurrentHashMap<Key, Boolean> cache;

    /**
     * Queue to maintain insertion/access order for efficient Second Chance algorithm
     * Keys are added when first accessed and re-added when accessed again
     */
    private final ConcurrentLinkedQueue<Key> accessQueue;

    /**
     * Lock used only for eviction operations to ensure atomicity
     */
    private final ReentrantLock evictionLock;

    protected AbstractLruCache() {
        this.cache = new ConcurrentHashMap<>();
        this.accessQueue = new ConcurrentLinkedQueue<>();
        this.evictionLock = new ReentrantLock();
    }

    /**
     * Updates access to an item for a specific cache key.
     * Following Second Chance algorithm: any access sets reference bit to true.
     * New keys are added to the queue (insertion order), existing keys just update reference bit.
     *
     * @param key The key being accessed
     */
    protected void updateAccess(Key key) {
        if (key == null) {
            return;
        }

        // Atomically add if absent, or get existing value if present
        Boolean previousValue = cache.putIfAbsent(key, true);
        
        if (previousValue == null) {
            // Key was newly added, add to queue
            accessQueue.offer(key);
        } else {
            // Key already existed, just update reference bit to true
            cache.put(key, true);
        }
    }

    /**
     * Retrieves the least recently used key using Second Chance algorithm with efficient queue-based approach.
     * Uses the access queue to avoid O(n) scanning of the entire cache.
     *
     * @return The least recently used key, or null if the cache is empty
     */
    protected Key getLeastRecentlyUsedItem() {
        if (cache.isEmpty()) {
            return null;
        }

        Key candidate;

        // Process queue until we find a victim or queue is empty
        while ((candidate = accessQueue.poll()) != null) {
            Boolean referenceBit = cache.get(candidate);

            if (referenceBit == null) {
                // Key was already evicted, continue to next
                continue;
            }

            if (!referenceBit) {
                // Found victim - reference bit is false (not recently accessed)
                return candidate;
            } else {
                // Give second chance - clear reference bit and re-add to queue
                cache.put(candidate, false);
                accessQueue.offer(candidate);
            }
        }

        // If we reach here, queue is empty but cache is not
        // This shouldn't happen in normal operation since accessQueue.offer() is always called
        // Return any key from cache as fallback
        return cache.keySet().iterator().hasNext() ? cache.keySet().iterator().next() : null;
    }

    /**
     * Evicts least recently used items from cache until the specified amount of RAM has been freed.
     * Uses Second Chance algorithm with lock-free access operations.
     *
     * @param ramBytesToRelease Number of bytes to evict
     */
    public void evict(long ramBytesToRelease) {
        if (ramBytesToRelease <= 0) {
            return;
        }

        // Use eviction lock to ensure atomic eviction operations
        evictionLock.lock();
        try {
            long ramBytesReleased = 0;

            // Continue evicting until we've freed enough memory or the cache is empty
            while (ramBytesReleased < ramBytesToRelease && !cache.isEmpty()) {
                // Find victim using Second Chance algorithm
                Key victimKey = getLeastRecentlyUsedItem();

                if (victimKey == null) {
                    // Cache is empty, nothing more to evict
                    break;
                }

                // Evict the item and track bytes freed
                ramBytesReleased += evictItem(victimKey);
            }

            log.debug("Freed {} bytes of memory using Second Chance algorithm", ramBytesReleased);
        } finally {
            evictionLock.unlock();
        }
    }

    /**
     * Evicts a specific item from the cache.
     * This method is used when a specific key needs to be removed.
     * Note: We don't remove from accessQueue here as stale entries will be skipped during eviction.
     *
     * @param key The key to evict
     * @return number of bytes freed, or 0 if the item was not evicted
     */
    protected long evictItem(Key key) {
        if (cache.remove(key) == null) {
            return 0; // Key not found
        }

        // Note: We don't remove from accessQueue here to avoid O(n) operation
        // Stale entries will be skipped in getLeastRecentlyUsedItem()

        return doEviction(key);
    }

    /**
     * Removes all entries for a specific cache key when an index is removed.
     * Uses eviction lock to ensure atomicity with ongoing evictions.
     *
     * @param cacheKey The cache key to remove
     */
    public void onIndexRemoval(@NonNull CacheKey cacheKey) {
        evictionLock.lock();
        try {
            // Remove all entries matching the cache key
            cache.entrySet().removeIf(entry -> entry.getKey().getCacheKey().equals(cacheKey));
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
