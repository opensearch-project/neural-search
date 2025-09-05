/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Value;

/**
 * LRU cache implementation for sparse vector caches.
 */
public class LruDocumentCache extends AbstractLruCache<LruDocumentCache.DocumentKey> {
    private static final LruDocumentCache INSTANCE = new LruDocumentCache();

    protected LruDocumentCache() {
        super();
    }

    public static LruDocumentCache getInstance() {
        return INSTANCE;
    }

    @Override
    protected long doEviction(DocumentKey documentKey) {
        CacheKey cacheKey = documentKey.getCacheKey();
        int docId = documentKey.getDocId();

        ForwardIndexCacheItem forwardIndexCacheItem = ForwardIndexCache.getInstance().get(cacheKey);
        if (forwardIndexCacheItem == null) {
            return 0;
        }
        return forwardIndexCacheItem.getWriter().erase(docId);
    }

    /**
     * Key class that combines a cache key and a document id for tracking LRU access.
     */
    @Value
    public static class DocumentKey implements LruCacheKey {
        CacheKey cacheKey;
        int docId;
    }
}
