/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * This class manages clustered posting within cache.
 * Given the limited cache introduced by CacheKey, we will simply
 * use the addWithoutBreaking method to ensure success insertion in cacheMap.
 */
public class ClusteredPostingCache extends SparseCache<ClusteredPostingCacheItem> {

    private static ClusteredPostingCache INSTANCE;

    private ClusteredPostingCache() {
        MemoryUsageManager.getInstance()
            .getMemoryUsageTracker()
            .safeRecord(RamUsageEstimator.shallowSizeOf(cacheMap), CircuitBreakerManager::addWithoutBreaking);
    }

    public static ClusteredPostingCache getInstance() {
        if (INSTANCE == null) {
            synchronized (ClusteredPostingCache.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ClusteredPostingCache();
                }
            }
        }
        return INSTANCE;
    }

    @NonNull
    public ClusteredPostingCacheItem getOrCreate(@NonNull CacheKey key) {
        RamBytesRecorder globalRecorder = MemoryUsageManager.getInstance().getMemoryUsageTracker();
        return super.getOrCreate(key, k -> new ClusteredPostingCacheItem(k, globalRecorder));
    }
}
