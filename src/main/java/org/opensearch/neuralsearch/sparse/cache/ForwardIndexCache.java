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
public class ForwardIndexCache extends SparseCache<ForwardIndexCacheItem> {

    private static final ForwardIndexCache INSTANCE = new ForwardIndexCache();

    private ForwardIndexCache() {
        CircuitBreakerManager.addWithoutBreaking(RamUsageEstimator.shallowSizeOf(cacheMap));
    }

    public static ForwardIndexCache getInstance() {
        return INSTANCE;
    }

    @NonNull
    public ForwardIndexCacheItem getOrCreate(@NonNull CacheKey key, int docCount) {
        return super.getOrCreate(key, k -> new ForwardIndexCacheItem(k, docCount));
    }
}
