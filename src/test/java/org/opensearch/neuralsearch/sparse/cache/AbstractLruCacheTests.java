/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class AbstractLruCacheTests extends AbstractSparseTestBase {

    /**
     * Test that updateAccess correctly updates the access order in the LRU cache
     */
    public void test_updateAccess_returnsLeastRecentlyUsedItem() {
        TestLruCache testCache = new TestLruCache();
        TestLruCacheKey key1 = new TestLruCacheKey("key1");
        TestLruCacheKey key2 = new TestLruCacheKey("key2");

        // Add keys to the cache
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);

        // Verify key1 is the least recently used
        assertEquals(key1, testCache.getLeastRecentlyUsedItem());

        // Access key1 again to make it most recently used
        testCache.updateAccess(key1);

        // Now key2 should be the least recently used
        assertEquals(key2, testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that updateAccess handles null keys gracefully
     */
    public void test_updateAccess_withNullKey() {
        TestLruCache testCache = new TestLruCache();
        TestLruCacheKey key = new TestLruCacheKey("key");

        // Add a key to the cache
        testCache.updateAccess(key);

        // Try to update with null key
        testCache.updateAccess(null);

        // Verify the cache still has the original key
        assertEquals(1, testCache.cache.size());
        assertEquals(key, testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that getLeastRecentlyUsedItem returns null when the cache is empty
     */
    public void test_getLeastRecentlyUsedItem_returnsNullWithEmptyCache() {
        TestLruCache testCache = new TestLruCache();

        assertNull(testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that getLeastRecentlyUsedItem returns the correct item
     */
    public void test_getLeastRecentlyUsedItem() {
        TestLruCache testCache = new TestLruCache();
        TestLruCacheKey key1 = new TestLruCacheKey("key1");
        TestLruCacheKey key2 = new TestLruCacheKey("key2");
        TestLruCacheKey key3 = new TestLruCacheKey("key3");

        // Add keys in order
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);
        testCache.updateAccess(key3);

        // Verify key1 is the least recently used
        assertEquals(key1, testCache.getLeastRecentlyUsedItem());

        // Access key1 and key2 to make key3 the least recently used
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);

        // Now key3 should be the least recently used
        assertEquals(key3, testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that evict does nothing when ramBytesToRelease is zero or negative
     */
    public void test_evict_withZeroAndNegativeBytes() {
        TestLruCache testCache = spy(new TestLruCache());
        TestLruCacheKey key1 = new TestLruCacheKey("key1");
        TestLruCacheKey key2 = new TestLruCacheKey("key2");

        // Add some items to the cache
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);

        // Try to evict with zero bytes
        testCache.evict(0);
        assertEquals(2, testCache.cache.size());

        // Try to evict with negative bytes
        testCache.evict(-10);
        assertEquals(2, testCache.cache.size());

        // Verify doEviction was never called
        verify(testCache, never()).doEviction(any());
    }

    /**
     * Test that evict correctly evicts items until enough memory is freed
     */
    public void test_evict_untilEnoughMemoryFreed() {
        TestLruCache testCache = spy(new TestLruCache());
        TestLruCacheKey key1 = new TestLruCacheKey("key1");
        TestLruCacheKey key2 = new TestLruCacheKey("key2");
        TestLruCacheKey key3 = new TestLruCacheKey("key3");

        // Set up the test cache to return specific byte values for eviction
        testCache.bytesFreedPerEviction = 50;

        // Add items to the cache
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);
        testCache.updateAccess(key3);

        // Evict 120 bytes (should evict 3 items)
        testCache.evict(120);

        // Verify the cache is now empty
        assertTrue(testCache.cache.isEmpty());

        // Verify doEviction was called 3 times
        verify(testCache, times(1)).doEviction(key1);
        verify(testCache, times(1)).doEviction(key2);
        verify(testCache, times(1)).doEviction(key3);
    }

    /**
     * Test that evict stops when the cache becomes empty
     */
    public void test_evict_stopsWhenCacheEmpty() {
        TestLruCache testCache = spy(new TestLruCache());
        TestLruCacheKey key = new TestLruCacheKey("key");

        // Set up the test cache to return specific byte values for eviction
        testCache.bytesFreedPerEviction = 0;

        // Add one item to the cache
        testCache.updateAccess(key);

        // Try to evict 100 bytes (more than available)
        testCache.evict(100);

        // Verify the cache is now empty
        assertTrue(testCache.cache.isEmpty());

        // Verify doEviction was called only once
        verify(testCache, times(1)).doEviction(key);
    }

    /**
     * Test that evictItem correctly removes an item from the access map
     */
    public void test_evictItem() {
        TestLruCache testCache = new TestLruCache();
        TestLruCacheKey key1 = new TestLruCacheKey("key1");
        TestLruCacheKey key2 = new TestLruCacheKey("key2");

        // Add items to the cache
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);

        // Evict key1
        long bytesFreed = testCache.evictItem(key1);

        // Verify key1 was removed from the cache
        assertFalse(testCache.cache.containsKey(key1));
        assertTrue(testCache.cache.containsKey(key2));

        // Verify the correct number of bytes was returned
        assertEquals(testCache.bytesFreedPerEviction, bytesFreed);
    }

    /**
     * Test that evictItem returns 0 when the key doesn't exist
     */
    public void test_evictItem_withNonExistentKey() {
        TestLruCache testCache = spy(new TestLruCache());
        TestLruCacheKey key = new TestLruCacheKey("key");
        TestLruCacheKey nonExistentKey = new TestLruCacheKey("nonexistent");

        // Add an item to the cache
        testCache.updateAccess(key);

        // Try to evict a non-existent key
        long bytesFreed = testCache.evictItem(nonExistentKey);

        // Verify no bytes were freed
        assertEquals(0, bytesFreed);

        // Verify doEviction was not called
        verify(testCache, never()).doEviction(nonExistentKey);
    }

    /**
     * Test that onIndexRemoval calls the doRemoveIndex method with the correct key
     */
    public void test_onIndexRemoval() {
        TestLruCache testCache = new TestLruCache();
        TestLruCacheKey key = new TestLruCacheKey("key");
        CacheKey cacheKey = key.getCacheKey();
        assertEquals(cacheKey, key.getCacheKey());

        testCache.updateAccess(key);

        testCache.onIndexRemoval(cacheKey);

        assertFalse(testCache.cache.containsKey(key));
    }

    /**
     * Test that onIndexRemoval throws NullPointerException when key is null
     */
    public void test_onIndexRemoval_withNullKey() {
        TestLruCache testCache = new TestLruCache();
        NullPointerException exception = expectThrows(NullPointerException.class, () -> testCache.onIndexRemoval(null));
        assertEquals("cacheKey is marked non-null but is null", exception.getMessage());
    }

    /**
     * A concrete implementation of AbstractLruCache for testing
     */
    private static class TestLruCache extends AbstractLruCache<TestLruCacheKey> {

        long bytesFreedPerEviction = 0;

        @Override
        protected long doEviction(TestLruCacheKey testLrucachekey) {
            return bytesFreedPerEviction;
        }
    }

    /**
     * A concrete implementation of LruCacheKey for testing
     */
    private class TestLruCacheKey implements LruCacheKey {

        private String name;
        private CacheKey cacheKey;

        public TestLruCacheKey(String name) {
            this.name = name;
        }

        @Override
        public CacheKey getCacheKey() {
            if (cacheKey == null) {
                cacheKey = prepareUniqueCacheKey(mock(SegmentInfo.class));
            }
            return cacheKey;
        }
    }
}
