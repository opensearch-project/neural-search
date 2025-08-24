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

public class ForwardIndexCacheTests extends AbstractSparseTestBase {

    private static final int TEST_DOC_COUNT = 10;
    private static final CacheKey cacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), TestsPrepareUtils.prepareKeyFieldInfo());
    private static final long cacheKeySize = RamUsageEstimator.shallowSizeOf(cacheKey);

    private long emptyForwardIndexCacheSize;
    private long emptyForwardIndexCacheItemSize;
    private ForwardIndexCache forwardIndexCache;

    /**
     * Set up the test environment before each test.
     * Gets the singleton instance of ForwardIndexCache and mocks the CircuitBreakerManager.
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();

        forwardIndexCache = ForwardIndexCache.getInstance();
        emptyForwardIndexCacheSize = forwardIndexCache.ramBytesUsed();

        CacheKey cacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), TestsPrepareUtils.prepareKeyFieldInfo());
        emptyForwardIndexCacheItemSize = new ForwardIndexCacheItem(cacheKey, TEST_DOC_COUNT).ramBytesUsed();
    }

    /**
     * Tear down the test environment after each test.
     * Empty the forward index cache and close the mocked CircuitBreakerManager.
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        forwardIndexCache.removeIndex(cacheKey);
        super.tearDown();
    }

    /**
     * Test that getInstance returns the same instance each time.
     * This verifies the singleton pattern implementation.
     */
    public void test_getInstance_returnsSameInstance() {
        ForwardIndexCache instance1 = ForwardIndexCache.getInstance();
        ForwardIndexCache instance2 = ForwardIndexCache.getInstance();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertSame(instance1, instance2);
    }

    /**
     * Test that getOrCreate returns a new cache item when the key doesn't exist.
     * This verifies the creation functionality of the method.
     */
    public void test_getOrCreate_whenKeyDoesNotExist_createsNewItem() {
        ForwardIndexCacheItem result = forwardIndexCache.getOrCreate(cacheKey, TEST_DOC_COUNT);

        assertNotNull(result);
        assertNotNull(forwardIndexCache.get(cacheKey));
        long ramBytesUsed = forwardIndexCache.ramBytesUsed();
        // The expected size would consist of three parts: empty class size, empty cache item and cache key size
        assertEquals(ramBytesUsed, emptyForwardIndexCacheSize + emptyForwardIndexCacheItemSize + cacheKeySize);

        // Capture the arguments passed to addWithoutBreaking
        ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockedCircuitBreaker, atLeastOnce()).addWithoutBreaking(argumentCaptor.capture());
        List<Long> capturedValues = argumentCaptor.getAllValues();
        assertTrue(capturedValues.contains(emptyForwardIndexCacheItemSize));
        assertTrue(capturedValues.contains(cacheKeySize));
    }

    /**
     * Test that getOrCreate returns the existing cache item when the key exists.
     * This verifies the retrieval functionality of the method.
     */
    public void test_getOrCreate_whenKeyExists_returnsExistingItem() {
        ForwardIndexCacheItem firstResult = forwardIndexCache.getOrCreate(cacheKey, TEST_DOC_COUNT);
        long ramBytesUsed1 = forwardIndexCache.ramBytesUsed();
        ForwardIndexCacheItem secondResult = forwardIndexCache.getOrCreate(cacheKey, TEST_DOC_COUNT * 2);
        long ramBytesUsed2 = forwardIndexCache.ramBytesUsed();

        // Both getOrCreate should return the same item, and the second call does not introduce new memory usage
        assertSame(firstResult, secondResult);
        assertEquals(ramBytesUsed1, ramBytesUsed2);
    }

    /**
     * Test that getOrCreate throws NullPointerException when null is passed as a key.
     * This test verifies the @NonNull annotation on the method parameter.
     */
    public void test_getOrCreate_whenKeyIsNull_throwsNullPointerException() {
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> { forwardIndexCache.getOrCreate(null, TEST_DOC_COUNT); }
        );

        assertEquals("key is marked non-null but is null", exception.getMessage());
    }
}
