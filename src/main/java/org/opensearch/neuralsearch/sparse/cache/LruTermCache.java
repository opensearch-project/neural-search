/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Value;
import org.apache.lucene.util.BytesRef;

/**
 * LRU cache implementation for posting list caches.
 */
public class LruTermCache extends AbstractLruCache<LruTermCache.TermKey> {
    private static final LruTermCache INSTANCE = new LruTermCache();

    private LruTermCache() {
        super();
    }

    public static LruTermCache getInstance() {
        return INSTANCE;
    }

    @Override
    protected long doEviction(TermKey termKey) {
        CacheKey cacheKey = termKey.getCacheKey();
        BytesRef term = termKey.getTerm();

        ClusteredPostingCacheItem clusteredPostingCacheItem = ClusteredPostingCache.getInstance().get(cacheKey);
        if (clusteredPostingCacheItem == null) {
            return 0;
        }
        return clusteredPostingCacheItem.getWriter().erase(term);
    }

    /**
     * Key class that combines a cache key and term for tracking LRU access.
     */
    @Value
    public static class TermKey implements LruCacheKey {
        CacheKey cacheKey;
        BytesRef term;
    }
}
