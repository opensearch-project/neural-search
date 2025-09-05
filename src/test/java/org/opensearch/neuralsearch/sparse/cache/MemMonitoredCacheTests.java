/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MemMonitoredCacheTests extends AbstractSparseTestBase {

    private static final CacheKey cacheKey = prepareUniqueCacheKey(TestsPrepareUtils.prepareSegmentInfo());

    private static final long cacheItemSize = 100L;
    private static final long cacheKeySize = RamUsageEstimator.shallowSizeOf(cacheKey);
    private static final long emptyCacheMapSize = RamUsageEstimator.shallowSizeOf(new ConcurrentHashMap<>());

    private TestCache sparseCache;
    private TestAccountable mockAccountableItem;

    /**
     * Set up the test environment before each test.
     * Creates a test sparse cache and a mocked accountable item.
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        sparseCache = new TestCache();
        mockAccountableItem = new TestAccountable();
    }

    /**
     * Test that get method returns the correct value when the key exists in the cache.
     * This verifies the basic retrieval functionality of the cache.
     */
    public void test_get_whenKeyExists_returnsValue() {
        sparseCache.put(cacheKey, mockAccountableItem);

        TestAccountable result = sparseCache.get(cacheKey);

        assertEquals(mockAccountableItem, result);
    }

    /**
     * Test that get method returns null when the key does not exist in the cache.
     * This verifies the cache's behavior for cache misses.
     */
    public void test_get_whenKeyDoesNotExist_returnsNull() {
        TestAccountable result = sparseCache.get(cacheKey);

        assertNull(result);
    }

    /**
     * Test that get method throws NullPointerException when null is passed as a key.
     * This test verifies the @NonNull annotation on the method parameter.
     */
    public void test_get_whenKeyIsNull_throwsNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> sparseCache.get(null));

        assertEquals("key is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test that onIndexRemoval method correctly removes an item from the cache when the key exists.
     * This also verifies that the circuit breaker is updated to release the memory.
     */
    public void test_onIndexRemoval_whenKeyExists_removesFromCache() {
        sparseCache.put(cacheKey, mockAccountableItem);

        sparseCache.onIndexRemoval(cacheKey);

        assertFalse(sparseCache.cacheMap.containsKey(cacheKey));
        verify(mockedCircuitBreaker).addWithoutBreaking(-cacheItemSize - cacheKeySize);
    }

    /**
     * Test that onIndexRemoval method does nothing when the key does not exist in the cache.
     * This verifies that the circuit breaker is not updated when no item is removed.
     */
    public void test_onIndexRemoval_whenKeyDoesNotExist_doesNothing() {
        sparseCache.onIndexRemoval(cacheKey);

        verify(mockedCircuitBreaker, never()).addWithoutBreaking(cacheItemSize);
    }

    /**
     * Test that onIndexRemoval method throws NullPointerException when null is passed as a key.
     * This test verifies the @NonNull annotation on the method parameter.
     */
    public void test_onIndexRemoval_whenKeyIsNull_throwsNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> sparseCache.onIndexRemoval(null));

        assertEquals("key is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test that ramBytesUsed method returns the correct size when the cache is empty.
     * This verifies the memory accounting for an empty cache.
     */
    public void test_ramBytesUsed_whenCacheIsEmpty_returnsShallowSizeOfMap() {
        long result = sparseCache.ramBytesUsed();

        assertEquals(result, emptyCacheMapSize);
    }

    /**
     * Test that ramBytesUsed method returns the correct sum of sizes when the cache has entries.
     * This verifies the memory accounting for a cache with items.
     */
    public void test_ramBytesUsed_withEntries_returnsSumOfSizes() {
        sparseCache.put(cacheKey, mockAccountableItem);

        long result = sparseCache.ramBytesUsed();

        assertEquals(result, emptyCacheMapSize + cacheItemSize + cacheKeySize);
    }

    /**
     * Concrete implementation of SparseCache for testing purposes.
     * Adds a put method to allow adding items to the cache for testing.
     */
    private static class TestCache extends MemMonitoredCache<TestAccountable> {
        public void put(@NonNull CacheKey key, @NonNull TestAccountable value) {
            cacheMap.put(key, value);
        }
    }

    /**
     * Concrete implementation of Accountable for testing purposes.
     * Returns a fixed size for ramBytesUsed to simplify testing.
     */
    private static class TestAccountable implements Accountable {
        @Override
        public long ramBytesUsed() {
            return cacheItemSize;
        };
    }
}
