/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LruDocumentCacheTests extends AbstractSparseTestBase {

    private CacheKey cacheKey1;
    private CacheKey cacheKey2;
    private TestLruDocumentCache testCache;

    @Before
    public void setUp() {
        super.setUp();

        // Prepare forward index and cache key
        cacheKey1 = prepareUniqueCacheKey(mock(SegmentInfo.class));
        cacheKey2 = prepareUniqueCacheKey(mock(SegmentInfo.class));
        ForwardIndexCache.getInstance().getOrCreate(cacheKey1, 10);

        testCache = new TestLruDocumentCache();
        testCache.clearAll();
    }

    /**
     * Test that getInstance returns the singleton instance
     */
    public void test_getInstance_returnsSingletonInstance() {
        LruDocumentCache instance1 = LruDocumentCache.getInstance();
        LruDocumentCache instance2 = LruDocumentCache.getInstance();

        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    /**
     * Test that doEviction correctly evicts a document
     */
    @SneakyThrows
    public void test_doEviction_erasesDocument() {
        LruDocumentCache.DocumentKey documentKey = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        SparseVector expectedVector = createVector(1, 10, 2, 20);
        ForwardIndexCache.getInstance().get(cacheKey1).getWriter().insert(1, expectedVector);

        testCache.doEviction(documentKey);

        assertNull(ForwardIndexCache.getInstance().get(cacheKey1).getReader().read(1));
    }

    /**
     * Test that doEviction doest nothing when the key is not within the forward index cache
     */
    @SneakyThrows
    public void test_doEviction_withNonExistentKey() {
        CacheKey nonExistentCacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "non_existent_field");
        LruDocumentCache.DocumentKey documentKey = new LruDocumentCache.DocumentKey(nonExistentCacheKey, 1);

        // Call doEviction
        long bytesFreed = testCache.doEviction(documentKey);

        // Verify zero bytes have been cleaned
        assertEquals(0, bytesFreed);
    }

    /**
     * Test that evict correctly evicts documents until enough memory is freed
     */
    @SneakyThrows
    public void test_evict_untilEnoughMemoryFreed() {
        TestLruDocumentCache testCacheSpy = spy(testCache);

        LruDocumentCache.DocumentKey documentKey1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey documentKey2 = new LruDocumentCache.DocumentKey(cacheKey1, 2);
        LruDocumentCache.DocumentKey documentKey3 = new LruDocumentCache.DocumentKey(cacheKey2, 2);

        testCacheSpy.updateAccess(documentKey1);
        testCacheSpy.updateAccess(documentKey2);
        testCacheSpy.updateAccess(documentKey3);

        when(testCacheSpy.doEviction(documentKey1)).thenReturn(10L);
        when(testCacheSpy.doEviction(documentKey2)).thenReturn(20L);
        when(testCacheSpy.doEviction(documentKey3)).thenReturn(30L);

        testCacheSpy.evict(30L);

        // The third document with documentKey3 should still be in the cache
        LruDocumentCache.DocumentKey remainingDoc = testCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingDoc);
        assertEquals(cacheKey2, remainingDoc.getCacheKey());
        assertEquals(2, remainingDoc.getDocId());
    }

    /**
     * Test that onIndexRemoval correctly removes all documents for an index
     */
    public void test_onIndexRemoval_removesAllDocumentsForIndex() {
        // Add documents to the cache for different indices
        LruDocumentCache.DocumentKey documentKey1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey documentKey2 = new LruDocumentCache.DocumentKey(cacheKey1, 2);
        LruDocumentCache.DocumentKey documentKey3 = new LruDocumentCache.DocumentKey(cacheKey2, 1);

        testCache.updateAccess(documentKey1);
        testCache.updateAccess(documentKey2);
        testCache.updateAccess(documentKey3);

        // Remove all documents for mockCacheKey1
        testCache.onIndexRemoval(cacheKey1);

        // Verify only documents for mockCacheKey2 remain
        LruDocumentCache.DocumentKey remainingDoc = testCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingDoc);
        assertEquals(cacheKey2, remainingDoc.getCacheKey());
        assertEquals(1, remainingDoc.getDocId());
    }

    /**
     * Test that onIndexRemoval throws NullPointerException when key is null
     */
    public void test_onIndexRemoval_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> testCache.onIndexRemoval(null));
        assertEquals("cacheKey is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test DocumentKey equals
     */
    public void test_DocumentKey_equals_returnsTrue_whenSame() {
        LruDocumentCache.DocumentKey key1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey key2 = new LruDocumentCache.DocumentKey(cacheKey1, 1);

        assertEquals(key1, key2);
    }

    /**
     * Test DocumentKey equals
     */
    public void test_DocumentKey_equals_returnsFalse_withDifferentDocId() {
        LruDocumentCache.DocumentKey key1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey key2 = new LruDocumentCache.DocumentKey(cacheKey1, 2);

        assertNotEquals(key1, key2);
    }

    /**
     * Test DocumentKey equals
     */
    public void test_DocumentKey_equals_returnsFalse_withDifferentCacheKey() {
        LruDocumentCache.DocumentKey key1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey key2 = new LruDocumentCache.DocumentKey(cacheKey2, 1);

        assertNotEquals(key1, key2);
    }

    /**
     * Test DocumentKey hashCode
     */
    public void test_DocumentKey_hashCodeEquals_returnsEqualValues_whenSame() {
        LruDocumentCache.DocumentKey key1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey key2 = new LruDocumentCache.DocumentKey(cacheKey1, 1);

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test DocumentKey hashCode
     */
    public void test_DocumentKey_hashCodeEquals_returnsDifferentValues_withDifferentDocId() {
        LruDocumentCache.DocumentKey key1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey key2 = new LruDocumentCache.DocumentKey(cacheKey1, 2);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test DocumentKey hashCode
     */
    public void test_DocumentKey_hashCodeEquals_returnsDifferentValues_withDifferentCacheKey() {
        LruDocumentCache.DocumentKey key1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey key2 = new LruDocumentCache.DocumentKey(cacheKey2, 1);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test DocumentKey get CacheKey
     */
    public void test_DocumentKey_getCacheKey() {
        LruDocumentCache.DocumentKey key = new LruDocumentCache.DocumentKey(cacheKey1, 42);

        assertEquals(cacheKey1, key.getCacheKey());
    }

    /**
     * Test DocumentKey get docId
     */
    public void test_DocumentKey_getDocId() {
        LruDocumentCache.DocumentKey key = new LruDocumentCache.DocumentKey(cacheKey1, 42);

        assertEquals(42, key.getDocId());
    }

    /**
     * Clear the LRU Document Cache and Forward Index Cache to avoid impact on other tests
     */
    @Override
    public void tearDown() throws Exception {
        testCache.clearAll();
        ForwardIndexCache.getInstance().onIndexRemoval(cacheKey1);
        super.tearDown();
    }

    private static class TestLruDocumentCache extends LruDocumentCache {

        public TestLruDocumentCache() {
            super();
        }

        public void clearAll() {
            super.evict(Long.MAX_VALUE);
        }
    }
}
