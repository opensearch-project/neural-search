/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Registry class to manage forward index within cache.
 * Given the limited cache introduced by CacheKey, we will simply use the addWithoutBreaking
 * method to ensure success insertion in registry.
 */
public class ForwardIndexCache extends MemMonitoredCache<ForwardIndexCacheItem> {

    private static volatile ForwardIndexCache INSTANCE;

    protected ForwardIndexCache() {
        MemoryUsageManager.getInstance()
            .getMemoryUsageTracker()
            .recordWithoutValidation(RamUsageEstimator.shallowSizeOf(cacheMap), CircuitBreakerManager::addWithoutBreaking);
    }

    public static ForwardIndexCache getInstance() {
        if (INSTANCE == null) {
            synchronized (ForwardIndexCache.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ForwardIndexCache();
                }
            }
        }
        return INSTANCE;
    }

    @NonNull
    public ForwardIndexCacheItem getOrCreate(@NonNull CacheKey key, int docCount) {
        RamBytesRecorder globalRecorder = MemoryUsageManager.getInstance().getMemoryUsageTracker();
        return super.getOrCreate(key, k -> new ForwardIndexCacheItem(k, docCount, globalRecorder));
    }
}
