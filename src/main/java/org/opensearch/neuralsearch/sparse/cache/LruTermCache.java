/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.BytesRef;

/**
 * LRU cache implementation for posting list caches.
 */
@Log4j2
public class LruTermCache extends AbstractLruCache<LruTermCache.TermKey> {
    private static final LruTermCache INSTANCE = new LruTermCache();

    private LruTermCache() {
        super();
    }

    public static LruTermCache getInstance() {
        return INSTANCE;
    }

    /**
     * Updates access to a term for a specific cache key.
     *
     * @param cacheKey The index cache key
     * @param term The term being accessed
     */
    public void updateAccess(CacheKey cacheKey, BytesRef term) {
        if (cacheKey == null || term == null) {
            return;
        }

        super.updateAccess(new TermKey(cacheKey, term.clone()));
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
    @Getter
    @EqualsAndHashCode
    public static class TermKey implements LruCacheKey {
        private final CacheKey cacheKey;
        private final BytesRef term;

        public TermKey(CacheKey cacheKey, BytesRef term) {
            this.cacheKey = cacheKey;
            this.term = term;
        }
    }
}
