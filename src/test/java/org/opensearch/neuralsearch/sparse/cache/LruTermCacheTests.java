/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.util.List;

public class LruTermCacheTests extends AbstractSparseTestBase {

    private BytesRef term1;
    private BytesRef term2;
    private BytesRef term3;
    private CacheKey cacheKey1;
    private CacheKey cacheKey2;
    private final LruTermCache lruTermCache = LruTermCache.getInstance();

    @Before
    public void setUp() {
        super.setUp();

        // Prepare cache keys
        cacheKey1 = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "test_field_1");
        cacheKey2 = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "test_field_2");

        // Prepare terms
        term1 = new BytesRef("term1");
        term2 = new BytesRef("term2");
        term3 = new BytesRef("term3");

        // Initialize clustered posting cache
        ClusteredPostingCache.getInstance().getOrCreate(cacheKey1);
        ClusteredPostingCache.getInstance().getOrCreate(cacheKey2);
    }

    /**
     * Test that getInstance returns the singleton instance
     */
    public void test_getInstance_returnsSingletonInstance() {
        LruTermCache instance1 = LruTermCache.getInstance();
        LruTermCache instance2 = LruTermCache.getInstance();

        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    /**
     * Test that updateAccess correctly updates the access order
     */
    public void test_updateAccess_updatesAccessOrder() {
        // Add terms to the cache
        lruTermCache.updateAccess(cacheKey1, term1);
        lruTermCache.updateAccess(cacheKey1, term2);

        // Verify the least recently used item is the first one added
        LruTermCache.TermKey leastRecentlyUsed = lruTermCache.getLeastRecentlyUsedItem();
        assertNotNull(leastRecentlyUsed);
        assertEquals(cacheKey1, leastRecentlyUsed.getCacheKey());
        assertEquals(term1, leastRecentlyUsed.getTerm());

        // Access the first term again
        lruTermCache.updateAccess(cacheKey1, term1);

        // Now the second term should be the least recently used
        leastRecentlyUsed = lruTermCache.getLeastRecentlyUsedItem();
        assertNotNull(leastRecentlyUsed);
        assertEquals(cacheKey1, leastRecentlyUsed.getCacheKey());
        assertEquals(term2, leastRecentlyUsed.getTerm());
    }

    /**
     * Test that updateAccess handles null cache key gracefully
     */
    public void test_updateAccess_withNullCacheKey() {
        // Try to update with null cache key
        lruTermCache.updateAccess(null, term1);

        // Add a term to the cache
        lruTermCache.updateAccess(cacheKey1, term1);

        // Verify that the least recently used term is the non-null one
        LruTermCache.TermKey leastRecentlyUsed = lruTermCache.getLeastRecentlyUsedItem();
        assertNotNull(leastRecentlyUsed);
        assertEquals(cacheKey1, leastRecentlyUsed.getCacheKey());
        assertEquals(term1, leastRecentlyUsed.getTerm());
    }

    /**
     * Test that updateAccess handles null term gracefully
     */
    public void test_updateAccess_withNullTerm() {
        // Try to update with null term
        lruTermCache.updateAccess(cacheKey1, null);

        // Add a term to the cache
        lruTermCache.updateAccess(cacheKey1, term1);

        // Verify that the least recently used term is the non-null one
        LruTermCache.TermKey leastRecentlyUsed = lruTermCache.getLeastRecentlyUsedItem();
        assertNotNull(leastRecentlyUsed);
        assertEquals(cacheKey1, leastRecentlyUsed.getCacheKey());
        assertEquals(term1, leastRecentlyUsed.getTerm());
    }

    /**
     * Test that doEviction correctly evicts a term
     */
    @SneakyThrows
    public void test_doEviction_erasesTerm() {
        LruTermCache.TermKey termKey = new LruTermCache.TermKey(cacheKey1, term1);
        List<DocumentCluster> expectedClusterList = prepareClusterList();
        PostingClusters expectedCluster = preparePostingClusters();
        ClusteredPostingCache.getInstance().get(cacheKey1).getWriter().insert(term1, expectedClusterList);

        // Verify the clustered posting cache has the term
        PostingClusters writtenCluster = ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term1);
        assertSame(expectedClusterList, writtenCluster.getClusters());

        // Call doEviction
        long bytesFreed = lruTermCache.doEviction(termKey);

        // Verify the term has been removed
        assertNull(ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term1));

        // Verify the correct number of bytes was returned
        assertEquals(expectedCluster.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(term1) + term1.bytes.length, bytesFreed);
    }

    /**
     * Test that doEviction does nothing when the key is not within the clustered posting cache
     */
    @SneakyThrows
    public void test_doEviction_withNonExistentKey() {
        CacheKey nonExistentCacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "non_existent_field");
        LruTermCache.TermKey termKey = new LruTermCache.TermKey(nonExistentCacheKey, term1);

        // Call doEviction
        long bytesFreed = lruTermCache.doEviction(termKey);

        // Verify zero bytes have been cleaned
        assertEquals(0, bytesFreed);
    }

    /**
     * Test that evict correctly evicts terms until enough memory is freed
     */
    @SneakyThrows
    public void test_evict_untilEnoughMemoryFreed() {
        // Add terms to the cache, this will update the access order
        PostingClusters posting1 = preparePostingClusters();
        PostingClusters posting2 = preparePostingClusters();
        PostingClusters posting3 = preparePostingClusters();

        ClusteredPostingCache.getInstance().get(cacheKey1).getWriter().insert(term1, posting1.getClusters());
        ClusteredPostingCache.getInstance().get(cacheKey1).getWriter().insert(term2, posting2.getClusters());
        ClusteredPostingCache.getInstance().get(cacheKey2).getWriter().insert(term3, posting3.getClusters());

        // Update access order
        lruTermCache.updateAccess(cacheKey1, term1);
        lruTermCache.updateAccess(cacheKey1, term2);
        lruTermCache.updateAccess(cacheKey2, term3);

        // Evict 2 terms
        long posting1Size = posting1.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(term1) + term1.bytes.length;
        long posting2Size = posting2.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(term2) + term2.bytes.length;
        lruTermCache.evict(posting1Size + posting2Size);

        // The third term should still be in the cache
        LruTermCache.TermKey remainingTerm = lruTermCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingTerm);
        assertEquals(cacheKey2, remainingTerm.getCacheKey());
        assertEquals(term3, remainingTerm.getTerm());

        // The first 2 terms have been removed and the third term is still in cache
        assertNull(ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term1));
        assertNull(ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term2));
        PostingClusters remainingPosting = ClusteredPostingCache.getInstance().get(cacheKey2).getReader().read(term3);
        assertEquals(posting3.getClusters(), remainingPosting.getClusters());
    }

    /**
     * Test that removeIndex correctly removes all terms for an index
     */
    public void test_removeIndex_removesAllTermsForIndex() {
        // Add terms to the cache for different indices
        lruTermCache.updateAccess(cacheKey1, term1);
        lruTermCache.updateAccess(cacheKey1, term2);
        lruTermCache.updateAccess(cacheKey2, term3);

        // Remove all terms for cacheKey1
        lruTermCache.removeIndex(cacheKey1);

        // Verify only terms for cacheKey2 remain
        LruTermCache.TermKey remainingTerm = lruTermCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingTerm);
        assertEquals(cacheKey2, remainingTerm.getCacheKey());
        assertEquals(term3, remainingTerm.getTerm());
    }

    /**
     * Test that removeIndex throws NullPointerException when key is null
     */
    public void test_removeIndex_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> lruTermCache.removeIndex(null));
        assertEquals("cacheKey is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test TermKey equals
     */
    public void test_TermKey_equals_returnsTrue_whenSame() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(key1, key2);
    }

    /**
     * Test TermKey equals
     */
    public void test_TermKey_equals_returnsFalse_withDifferentTerm() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term2);

        assertNotEquals(key1, key2);
    }

    /**
     * Test TermKey equals
     */
    public void test_TermKey_equals_returnsFalse_withDifferentCacheKey() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey2, term1);

        assertNotEquals(key1, key2);
    }

    /**
     * Test TermKey hashCode
     */
    public void test_TermKey_hashCodeEquals_returnsEqualValues_whenSame() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test TermKey hashCode
     */
    public void test_TermKey_hashCodeEquals_returnsDifferentValues_withDifferentTerm() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term2);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test TermKey hashCode
     */
    public void test_TermKey_hashCodeEquals_returnsDifferentValues_withDifferentCacheKey() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey2, term1);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test TermKey get CacheKey
     */
    public void test_TermKey_getCacheKey() {
        LruTermCache.TermKey key = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(cacheKey1, key.getCacheKey());
    }

    /**
     * Test TermKey get Term
     */
    public void test_TermKey_getTerm() {
        LruTermCache.TermKey key = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(term1, key.getTerm());
    }

    /**
     * Clear the LRU Term Cache and Clustered Posting Cache to avoid impact on other tests
     */
    @Override
    public void tearDown() throws Exception {
        lruTermCache.evict(Long.MAX_VALUE);
        ClusteredPostingCache.getInstance().removeIndex(cacheKey1);
        ClusteredPostingCache.getInstance().removeIndex(cacheKey2);
        super.tearDown();
    }
}
