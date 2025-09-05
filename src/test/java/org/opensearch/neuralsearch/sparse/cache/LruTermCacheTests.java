/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LruTermCacheTests extends AbstractSparseTestBase {

    private BytesRef term1;
    private BytesRef term2;
    private BytesRef term3;
    private CacheKey cacheKey1;
    private CacheKey cacheKey2;
    private TestLruTermCache testCache;

    @Before
    public void setUp() {
        super.setUp();

        // Prepare cache keys
        cacheKey1 = prepareUniqueCacheKey(mock(SegmentInfo.class));
        cacheKey2 = prepareUniqueCacheKey(mock(SegmentInfo.class));

        term1 = new BytesRef("term1");
        term2 = new BytesRef("term2");
        term3 = new BytesRef("term3");

        ClusteredPostingCache.getInstance().getOrCreate(cacheKey1);

        testCache = new TestLruTermCache();
        testCache.clearAll();
    }

    public void test_getInstance_returnsSingletonInstance() {
        LruTermCache instance1 = LruTermCache.getInstance();
        LruTermCache instance2 = LruTermCache.getInstance();

        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    @SneakyThrows
    public void test_doEviction_erasesTerm() {
        LruTermCache.TermKey termKey = new LruTermCache.TermKey(cacheKey1, term1);
        List<DocumentCluster> expectedClusterList = prepareClusterList();
        ClusteredPostingCache.getInstance().get(cacheKey1).getWriter().insert(term1, expectedClusterList);

        testCache.doEviction(termKey);

        assertNull(ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term1));
    }

    @SneakyThrows
    public void test_doEviction_withNonExistentKey() {
        CacheKey nonExistentCacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "non_existent_field");
        LruTermCache.TermKey termKey = new LruTermCache.TermKey(nonExistentCacheKey, term1);

        long bytesFreed = testCache.doEviction(termKey);

        assertEquals(0, bytesFreed);
    }

    @SneakyThrows
    public void test_evict_untilEnoughMemoryFreed() {
        TestLruTermCache testCacheSpy = spy(testCache);
        LruTermCache.TermKey termKey1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey termKey2 = new LruTermCache.TermKey(cacheKey1, term2);
        LruTermCache.TermKey termKey3 = new LruTermCache.TermKey(cacheKey2, term3);

        testCacheSpy.updateAccess(termKey1);
        testCacheSpy.updateAccess(termKey2);
        testCacheSpy.updateAccess(termKey3);

        when(testCacheSpy.doEviction(termKey1)).thenReturn(10L);
        when(testCacheSpy.doEviction(termKey2)).thenReturn(20L);
        when(testCacheSpy.doEviction(termKey3)).thenReturn(30L);

        testCacheSpy.evict(30L);

        // The third term with termKey3 should still be in the cache
        LruTermCache.TermKey remainingTerm = testCacheSpy.getLeastRecentlyUsedItem();
        assertNotNull(remainingTerm);
        assertEquals(cacheKey2, remainingTerm.getCacheKey());
        assertEquals(term3, remainingTerm.getTerm());
    }

    /**
     * Test that onIndexRemoval correctly removes all terms for an index
     */
    public void test_onIndexRemoval_removesAllTermsForIndex() {
        // Add terms to the cache for different indices
        LruTermCache.TermKey termKey1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey termKey2 = new LruTermCache.TermKey(cacheKey1, term2);
        LruTermCache.TermKey termKey3 = new LruTermCache.TermKey(cacheKey2, term3);

        testCache.updateAccess(termKey1);
        testCache.updateAccess(termKey2);
        testCache.updateAccess(termKey3);

        // Remove all terms for cacheKey1
        testCache.onIndexRemoval(cacheKey1);

        // Verify only terms for cacheKey2 remain
        LruTermCache.TermKey remainingTerm = testCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingTerm);
        assertEquals(cacheKey2, remainingTerm.getCacheKey());
        assertEquals(term3, remainingTerm.getTerm());
    }

    /**
     * Test that onIndexRemoval throws NullPointerException when key is null
     */
    public void test_onIndexRemoval_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> testCache.onIndexRemoval(null));
        assertEquals("cacheKey is marked non-null but is null", exception.getMessage());
    }

    public void test_TermKey_equals_returnsTrue_whenSame() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(key1, key2);
    }

    public void test_TermKey_equals_returnsFalse_withDifferentTerm() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term2);

        assertNotEquals(key1, key2);
    }

    public void test_TermKey_equals_returnsFalse_withDifferentCacheKey() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey2, term1);

        assertNotEquals(key1, key2);
    }

    public void test_TermKey_hashCodeEquals_returnsEqualValues_whenSame() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    public void test_TermKey_hashCodeEquals_returnsDifferentValues_withDifferentTerm() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term2);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    public void test_TermKey_hashCodeEquals_returnsDifferentValues_withDifferentCacheKey() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey2, term1);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    public void test_TermKey_getCacheKey() {
        LruTermCache.TermKey key = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(cacheKey1, key.getCacheKey());
    }

    public void test_TermKey_getTerm() {
        LruTermCache.TermKey key = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(term1, key.getTerm());
    }

    @Override
    public void tearDown() throws Exception {
        testCache.clearAll();
        ClusteredPostingCache.getInstance().onIndexRemoval(cacheKey1);
        super.tearDown();
    }

    private static class TestLruTermCache extends LruTermCache {

        public TestLruTermCache() {
            super();
        }

        public void clearAll() {
            super.evict(Long.MAX_VALUE);
        }
    }
}
