/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.util.List;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

public class ClusteredPostingCacheTests extends AbstractSparseTestBase {

    private static final CacheKey cacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), TestsPrepareUtils.prepareKeyFieldInfo());
    private static final long cacheKeySize = RamUsageEstimator.shallowSizeOf(cacheKey);

    private long emptyClusteredPostingCacheSize;
    private long emptyClusteredPostingCacheItemSize;
    private ClusteredPostingCache clusteredPostingCache;

    /**
     * Set up the test environment before each test.
     * Gets the singleton instance of ClusteredPostingCache.
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        clusteredPostingCache = ClusteredPostingCache.getInstance();

        emptyClusteredPostingCacheSize = clusteredPostingCache.ramBytesUsed();
        CacheKey cacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), TestsPrepareUtils.prepareKeyFieldInfo());
        emptyClusteredPostingCacheItemSize = new ClusteredPostingCacheItem(cacheKey).ramBytesUsed();
    }

    /**
     * Tear down the test environment before each test.
     * Empty the clustered posting cache.
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        clusteredPostingCache.removeIndex(cacheKey);
        super.tearDown();
    }

    /**
     * Test that getInstance returns the same instance each time.
     * This verifies the singleton pattern implementation.
     */
    public void test_getInstance_returnsSameInstance() {
        ClusteredPostingCache instance1 = ClusteredPostingCache.getInstance();
        ClusteredPostingCache instance2 = ClusteredPostingCache.getInstance();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertSame(instance1, instance2);
    }

    /**
     * Test that getOrCreate returns a new cache item when the key doesn't exist.
     * This verifies the creation functionality of the method.
     */
    public void test_getOrCreate_whenKeyDoesNotExist_createsNewItem() {
        ClusteredPostingCacheItem result = clusteredPostingCache.getOrCreate(cacheKey);

        assertNotNull(result);
        assertNotNull(clusteredPostingCache.get(cacheKey));
        long ramBytesUsed = clusteredPostingCache.ramBytesUsed();
        // The expected size would consist of three parts: empty class size, empty cache item and cache key size
        assertEquals(ramBytesUsed, emptyClusteredPostingCacheSize + emptyClusteredPostingCacheItemSize + cacheKeySize);

        // Capture the arguments passed to addWithoutBreaking
        ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockedCircuitBreaker, atLeastOnce()).addWithoutBreaking(argumentCaptor.capture());
        List<Long> capturedValues = argumentCaptor.getAllValues();
        assertTrue(capturedValues.contains(emptyClusteredPostingCacheItemSize));
        assertTrue(capturedValues.contains(cacheKeySize));
    }

    /**
     * Test that getOrCreate returns the existing cache item when the key exists.
     * This verifies the retrieval functionality of the method.
     */
    public void test_getOrCreate_whenKeyExists_returnsExistingItem() {
        ClusteredPostingCacheItem firstResult = clusteredPostingCache.getOrCreate(cacheKey);
        long ramBytesUsed1 = clusteredPostingCache.ramBytesUsed();
        ClusteredPostingCacheItem secondResult = clusteredPostingCache.getOrCreate(cacheKey);
        long ramBytesUsed2 = clusteredPostingCache.ramBytesUsed();

        // Both getOrCreate should return the same item, and the second call does not introduce new memory usage
        assertSame(firstResult, secondResult);
        assertEquals(ramBytesUsed1, ramBytesUsed2);
    }

    /**
     * Test that getOrCreate throws NullPointerException when null is passed as a key.
     * This test verifies the @NonNull annotation on the method parameter.
     */
    public void test_getOrCreate_whenKeyIsNull_throwsNullPointerException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { clusteredPostingCache.getOrCreate(null); });

        assertEquals("key is marked non-null but is null", exception.getMessage());
    }
}
