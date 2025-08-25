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

    private static final ClusteredPostingCache INSTANCE = new ClusteredPostingCache();

    private ClusteredPostingCache() {
        CircuitBreakerManager.addWithoutBreaking(RamUsageEstimator.shallowSizeOf(cacheMap));
    }

    public static ClusteredPostingCache getInstance() {
        return INSTANCE;
    }

    @NonNull
    public ClusteredPostingCacheItem getOrCreate(@NonNull CacheKey key) {
        return cacheMap.computeIfAbsent(key, k -> {
            ClusteredPostingCacheItem clusteredPostingCacheItem = new ClusteredPostingCacheItem(key);
            CircuitBreakerManager.addWithoutBreaking(RamUsageEstimator.shallowSizeOf(key));
            return clusteredPostingCacheItem;
        });
    }
}
