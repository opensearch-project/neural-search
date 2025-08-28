/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

public class LruDocumentCacheTests extends AbstractSparseTestBase {

    private CacheKey cacheKey1;
    private CacheKey cacheKey2;
    private final LruDocumentCache lruDocumentCache = LruDocumentCache.getInstance();

    @Before
    public void setUp() {
        super.setUp();

        // Prepare forward index and cache key
        cacheKey1 = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "test_field_1");
        cacheKey2 = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "test_field_2");
        ForwardIndexCache.getInstance().getOrCreate(cacheKey1, 10);
        ForwardIndexCache.getInstance().getOrCreate(cacheKey2, 10);
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

        // Verify the forward index cache has the document
        SparseVector writtenVector = ForwardIndexCache.getInstance().get(cacheKey1).getReader().read(1);
        assertSame(expectedVector, writtenVector);

        // Call doEviction
        long bytesFreed = lruDocumentCache.doEviction(documentKey);

        // Verify the document has been removed
        assertNull(ForwardIndexCache.getInstance().get(cacheKey1).getReader().read(1));

        // Verify the correct number of bytes was returned
        assertEquals(expectedVector.ramBytesUsed(), bytesFreed);
    }

    /**
     * Test that doEviction doest nothing when the key is not within the forward index cache
     */
    @SneakyThrows
    public void test_doEviction_withNonExistentKey() {
        CacheKey nonExistentCacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "non_existent_field");
        LruDocumentCache.DocumentKey documentKey = new LruDocumentCache.DocumentKey(nonExistentCacheKey, 1);

        // Call doEviction
        long bytesFreed = lruDocumentCache.doEviction(documentKey);

        // Verify zero bytes have been cleaned
        assertEquals(0, bytesFreed);
    }

    /**
     * Test that evict correctly evicts documents until enough memory is freed
     */
    @SneakyThrows
    public void test_evict_untilEnoughMemoryFreed() {
        // Add documents to the cache, this will update the access order
        SparseVector vector1 = createVector(1, 1, 2, 2);
        SparseVector vector2 = createVector(1, 10, 2, 20);
        SparseVector vector3 = createVector(1, 100, 2, 200);
        ForwardIndexCache.getInstance().get(cacheKey1).getWriter().insert(1, vector1);
        ForwardIndexCache.getInstance().get(cacheKey1).getWriter().insert(2, vector2);
        ForwardIndexCache.getInstance().get(cacheKey2).getWriter().insert(2, vector3);

        // Evict 2 documents
        lruDocumentCache.evict(vector1.ramBytesUsed() + vector2.ramBytesUsed());

        // The third document should still be in the cache
        LruDocumentCache.DocumentKey remainingDoc = lruDocumentCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingDoc);
        assertEquals(cacheKey2, remainingDoc.getCacheKey());
        assertEquals(2, remainingDoc.getDocId());

        // The first 2 documents has been removed and the third document is still in cache
        assertNull(ForwardIndexCache.getInstance().get(cacheKey1).getReader().read(1));
        assertNull(ForwardIndexCache.getInstance().get(cacheKey1).getReader().read(2));
        assertSame(vector3, ForwardIndexCache.getInstance().get(cacheKey2).getReader().read(2));
    }

    /**
     * Test that removeIndex correctly removes all documents for an index
     */
    public void test_removeIndex_removesAllDocumentsForIndex() {
        // Add documents to the cache for different indices
        LruDocumentCache.DocumentKey documentKey1 = new LruDocumentCache.DocumentKey(cacheKey1, 1);
        LruDocumentCache.DocumentKey documentKey2 = new LruDocumentCache.DocumentKey(cacheKey1, 2);
        LruDocumentCache.DocumentKey documentKey3 = new LruDocumentCache.DocumentKey(cacheKey2, 1);

        lruDocumentCache.updateAccess(documentKey1);
        lruDocumentCache.updateAccess(documentKey2);
        lruDocumentCache.updateAccess(documentKey3);

        // Remove all documents for mockCacheKey1
        lruDocumentCache.removeIndex(cacheKey1);

        // Verify only documents for mockCacheKey2 remain
        LruDocumentCache.DocumentKey remainingDoc = lruDocumentCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingDoc);
        assertEquals(cacheKey2, remainingDoc.getCacheKey());
        assertEquals(1, remainingDoc.getDocId());
    }

    /**
     * Test that removeIndex throws NullPointerException when key is null
     */
    public void test_removeIndex_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> lruDocumentCache.removeIndex(null));
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
        lruDocumentCache.evict(Long.MAX_VALUE);
        ForwardIndexCache.getInstance().removeIndex(cacheKey1);
        ForwardIndexCache.getInstance().removeIndex(cacheKey2);
        super.tearDown();
    }
}
