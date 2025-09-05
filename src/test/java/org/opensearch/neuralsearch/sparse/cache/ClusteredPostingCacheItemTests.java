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
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class ClusteredPostingCacheItemTests extends AbstractSparseTestBase {

    private static final BytesRef testTerm = new BytesRef("test_term");
    private List<DocumentCluster> testClusters;
    private ClusteredPostingCacheItem cacheItem;
    private RamBytesRecorder globalRecorder;

    /**
     * Set up the test environment before each test.
     * Creates a list of clusters and a cache item.
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        testClusters = prepareClusterList();
        globalRecorder = mock(RamBytesRecorder.class);
        when(globalRecorder.record(anyLong())).thenReturn(true);
        CacheKey cacheKey = prepareUniqueCacheKey(TestsPrepareUtils.prepareSegmentInfo());
        cacheItem = new ClusteredPostingCacheItem(cacheKey, globalRecorder);
    }

    public void test_constructor() {
        verify(globalRecorder, times(1)).recordWithoutValidation(anyLong(), any());
    }

    /**
     * Tests that with different getWriter function calling, correct writer will be returned.
     * This verifies the basic functionality of the ClusteredPostingWriter.
     */
    @SneakyThrows
    public void test_writer_gettingMethods() {
        ClusteredPostingWriter originalWriter = cacheItem.getWriter();
        ClusteredPostingWriter consumerWriter = cacheItem.getWriter((lambdaPlaceHolder) -> {});

        // Test CacheClusteredPostingWriter and EarlyStopCacheClusteredPostingWriter can be correctly created
        assertNotEquals("Two writers should be different", originalWriter, consumerWriter);
    }

    /**
     * Tests that a term with clusters can be successfully inserted and read back.
     * This verifies the basic functionality of the writer and reader.
     */
    @SneakyThrows
    public void test_writerInsert_withValidClusters() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        // Initial state - no terms should exist
        assertEquals("Initial cache should be empty", 0, reader.size());
        assertNull("Term should not exist initially", reader.read(testTerm));

        writer.insert(testTerm, testClusters);

        assertEquals("Cache should have one entry", 1, reader.size());
        PostingClusters readClusters = reader.read(testTerm);
        assertNotNull("Read cluster should not be null", readClusters);
    }

    /**
     * Tests that inserting null clusters is ignored.
     * This verifies the null handling in the ClusteredPostingWriter.
     */
    @SneakyThrows
    public void test_writerInsert_withNullClusters() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        long initialRam = cacheItem.ramBytesUsed();
        writer.insert(testTerm, null);

        assertEquals("Cache should be empty", 0, reader.size());
        assertNull("Term should not exist", reader.read(testTerm));
        assertEquals("RAM usage should not change", initialRam, cacheItem.ramBytesUsed());
    }

    /**
     * Tests that inserting empty clusters is ignored.
     * This verifies the empty list handling in the ClusteredPostingWriter.
     */
    @SneakyThrows
    public void test_writerInsert_withEmptyClusters() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        long initialRam = cacheItem.ramBytesUsed();
        writer.insert(testTerm, new ArrayList<>());

        assertEquals("Cache should be empty", 0, reader.size());
        assertNull("Term should not exist", reader.read(testTerm));
        assertEquals("RAM usage should not change", initialRam, cacheItem.ramBytesUsed());
    }

    /**
     * Tests that inserting with a null term is ignored.
     * This verifies the null handling in the ClusteredPostingWriter.
     */
    @SneakyThrows
    public void test_writerInsert_withNullTerm() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        long initialRam = cacheItem.ramBytesUsed();
        writer.insert(null, testClusters);

        assertEquals("Cache should be empty", 0, reader.size());
        assertEquals("RAM usage should not change", initialRam, cacheItem.ramBytesUsed());
    }

    /**
     * Tests that ramBytesUsed correctly reports the memory usage after clusters are inserted.
     * This verifies the memory tracking functionality of the ClusteredPostingCacheItem.
     */
    @SneakyThrows
    public void test_ramBytesUsed_withInsertedClusters() {
        ClusteredPostingWriter writer = cacheItem.getWriter();

        long initialRam = cacheItem.ramBytesUsed();
        writer.insert(testTerm, testClusters);
        long ramWithClusters = cacheItem.ramBytesUsed();

        PostingClusters postingClusters = new PostingClusters(testClusters);
        long expectedRamBytesIncreased = postingClusters.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(testTerm)
            + (testTerm.bytes != null ? testTerm.bytes.length : 0);

        assertEquals("RAM usage should increase as expected after insertion", ramWithClusters - initialRam, expectedRamBytesIncreased);
    }

    /**
     * Tests that inserting the same term twice doesn't overwrite the existing entry.
     * This verifies the putIfAbsent behavior in the ClusteredPostingWriter.
     */
    @SneakyThrows
    public void test_writerInsert_skipsDuplicates() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        writer.insert(testTerm, testClusters);
        PostingClusters firstClusters = reader.read(testTerm);

        // Insert with the same term with different cluster
        List<DocumentCluster> newClusters = new ArrayList<>();

        SparseVector documentSummary1 = createVector(1, 20, 2, 40);
        SparseVector documentSummary2 = createVector(1, 2, 2, 4);
        List<DocWeight> docWeights1 = new ArrayList<>();
        docWeights1.add(new DocWeight(0, (byte) 1));

        List<DocWeight> docWeights2 = new ArrayList<>();
        docWeights1.add(new DocWeight(1, (byte) 2));

        newClusters.add(new DocumentCluster(documentSummary1, docWeights1, false));
        newClusters.add(new DocumentCluster(documentSummary2, docWeights2, false));

        writer.insert(testTerm, newClusters);

        // Verify the original clusters are still there
        PostingClusters readClusters = reader.read(testTerm);
        assertSame("Should be the same clusters object", firstClusters, readClusters);
        assertEquals("Should have the original number of clusters", testClusters.size(), readClusters.getClusters().size());
    }

    /**
     * Tests that getTerms returns the correct set of terms.
     * This verifies the term set functionality of the ClusteredPostingReader.
     */
    @SneakyThrows
    public void test_readerGetTerms() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        Set<BytesRef> initialTerms = reader.getTerms();
        assertEquals("Initial term set should be empty", 0, initialTerms.size());

        BytesRef term1 = new BytesRef("term1");
        BytesRef term2 = new BytesRef("term2");
        writer.insert(term1, testClusters);
        writer.insert(term2, testClusters);

        Set<BytesRef> terms = reader.getTerms();
        assertEquals("Should have two terms", 2, terms.size());
        assertTrue("Should contain term1", terms.contains(term1));
        assertTrue("Should contain term2", terms.contains(term2));
    }

    /**
     * Tests that size returns the correct number of terms.
     * This verifies the size functionality of the ClusteredPostingReader.
     */
    @SneakyThrows
    public void test_readerSize() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        assertEquals("Initial size should be 0", 0, reader.size());

        writer.insert(new BytesRef("term1"), testClusters);
        assertEquals("Size should be 1 after first insertion", 1, reader.size());

        writer.insert(new BytesRef("term2"), testClusters);
        assertEquals("Size should be 2 after second insertion", 2, reader.size());
    }

    /**
     * Tests that the BytesRef terms are properly cloned when inserted.
     * This verifies that the cache doesn't depend on the original BytesRef instances.
     */
    @SneakyThrows
    public void test_writerInsert_withMutableTerm() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        // Create a BytesRef that we'll modify after insertion
        byte[] bytes = new byte[] { 1, 2, 3 };
        BytesRef mutableTerm = new BytesRef(bytes);

        writer.insert(mutableTerm, testClusters);

        byte[] newBytes = new byte[] { 1, 2, 3, 4 };
        mutableTerm = new BytesRef(newBytes);

        // The cached term should still be retrievable with the original values
        BytesRef originalValueTerm = new BytesRef(bytes);
        PostingClusters clusters = reader.read(originalValueTerm);
        assertNotNull("Should be able to retrieve with original value", clusters);

        // And we shouldn't be able to retrieve with the modified value
        clusters = reader.read(mutableTerm);
        assertNull("Should not retrieve with modified value", clusters);
    }

    /**
     * Tests that multiple terms can be inserted and retrieved correctly.
     * This verifies the multi-term handling capabilities of the cache.
     */
    @SneakyThrows
    public void test_writerInsert_withMultipleTerms() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        BytesRef term1 = new BytesRef("term1");
        List<DocumentCluster> clusters1 = prepareClusterList();

        BytesRef term2 = new BytesRef("term2");
        List<DocumentCluster> clusters2 = prepareClusterList();

        BytesRef term3 = new BytesRef("term3");
        List<DocumentCluster> clusters3 = prepareClusterList();

        writer.insert(term1, clusters1);
        writer.insert(term2, clusters2);
        writer.insert(term3, clusters3);

        assertEquals("Term1 should have 2 cluster", 2, reader.read(term1).getClusters().size());
        assertEquals("Term2 should have 2 clusters", 2, reader.read(term2).getClusters().size());
        assertEquals("Term3 should have 2 clusters", 2, reader.read(term3).getClusters().size());
    }

    /**
     * Tests that the cache correctly handles empty BytesRef terms.
     * This verifies the edge case handling for empty terms.
     */
    @SneakyThrows
    public void test_writerInsert_withEmptyTerm() {
        ClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        // Create an empty BytesRef
        BytesRef emptyTerm = new BytesRef("");

        // Insert with clusters
        writer.insert(emptyTerm, testClusters);

        // Verify it can be retrieved
        PostingClusters clusters = reader.read(emptyTerm);
        assertNotNull("Should be able to retrieve empty term", clusters);
        assertEquals("Should have correct number of clusters", testClusters.size(), clusters.getClusters().size());
    }

    /**
     * Tests that getWriter with circuitBreakerHandler returns a writer with the handler.
     * This verifies the new getWriter method functionality.
     */
    @SneakyThrows
    public void test_getWriter_withCircuitBreakerHandler() {
        Consumer<Long> mockHandler = mock(Consumer.class);
        ClusteredPostingWriter writer = cacheItem.getWriter(mockHandler);
        ClusteredPostingReader reader = cacheItem.getReader();

        assertNotNull("Writer should not be null", writer);

        // Insert should work normally when circuit breaker doesn't trip
        writer.insert(testTerm, testClusters);

        PostingClusters clusters = reader.read(testTerm);
        assertNotNull("Should be able to retrieve clusters", clusters);
        verify(mockHandler, never()).accept(anyLong());
    }

    @SneakyThrows
    public void test_writerInsert_whenRecordReturnFalse() {
        Consumer<Long> mockHandler = mock(Consumer.class);
        ClusteredPostingWriter writer = cacheItem.getWriter(mockHandler);
        ClusteredPostingReader reader = cacheItem.getReader();
        when(globalRecorder.record(anyLong())).thenReturn(false);

        writer.insert(testTerm, testClusters);

        assertEquals("Cache should be empty when record fails", 0, reader.size());
        assertNull("Term should not exist when record fails", reader.read(testTerm));
        verify(mockHandler).accept(anyLong());
        verify(globalRecorder, times(2)).record(anyLong());
    }

    /**
     * Tests that circuitBreakerHandler is not called when circuit breaker doesn't trip.
     * This verifies the conditional calling of the handler.
     */
    @SneakyThrows
    public void test_writerInsert_withCircuitBreakerHandler_whenCircuitBreakerDoesNotTrip() {
        Consumer<Long> mockHandler = mock(Consumer.class);
        ClusteredPostingWriter writer = cacheItem.getWriter(mockHandler);
        ClusteredPostingReader reader = cacheItem.getReader();

        writer.insert(testTerm, testClusters);

        assertEquals("Cache should have one entry", 1, reader.size());
        assertNotNull("Term should exist", reader.read(testTerm));
        verify(mockHandler, never()).accept(anyLong());
    }

    /**
     * Tests that erasing a term with null value returns 0 bytes freed.
     * This verifies the null handling in the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withNullTerm() {
        CacheableClusteredPostingWriter writer = cacheItem.getWriter();

        long bytesFreed = writer.erase(null);

        assertEquals("Erasing null term should free 0 bytes", 0, bytesFreed);
    }

    /**
     * Tests that erasing a non-existent term returns 0 bytes freed.
     * This verifies the behavior when trying to erase a term that doesn't exist.
     */
    @SneakyThrows
    public void test_writerErase_withNonExistentTerm() {
        ClusteredPostingReader reader = cacheItem.getReader();
        CacheableClusteredPostingWriter writer = cacheItem.getWriter();

        BytesRef nonExistentTerm = new BytesRef("non_existent_term");
        long bytesFreed = writer.erase(nonExistentTerm);

        assertEquals("Erasing non-existent term should free 0 bytes", 0, bytesFreed);
        assertEquals("Cache size should remain empty", 0, reader.size());
    }

    /**
     * Tests that erasing an existing term returns the correct number of bytes freed
     * and removes the term from the cache.
     * This verifies the basic functionality of the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withExistingTerm() {
        ClusteredPostingReader reader = cacheItem.getReader();
        CacheableClusteredPostingWriter writer = cacheItem.getWriter();

        // First insert a term
        writer.insert(testTerm, testClusters);

        // Verify it exists
        assertNotNull("Term should exist after insertion", reader.read(testTerm));
        assertEquals("Cache should have one entry", 1, reader.size());

        // Calculate expected bytes to be freed
        PostingClusters postingClusters = new PostingClusters(testClusters);
        long expectedBytesFreed = postingClusters.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(testTerm) + (testTerm.bytes != null
            ? testTerm.bytes.length
            : 0);

        // Erase the term
        long actualBytesFreed = writer.erase(testTerm);

        // Verify results
        assertEquals("Should free the expected number of bytes", expectedBytesFreed, actualBytesFreed);
        assertNull("Term should no longer exist", reader.read(testTerm));
        assertEquals("Cache should be be empty", 0, reader.size());
    }

    /**
     * Tests that erasing multiple terms works correctly.
     * This verifies the erase method works with multiple terms in the cache.
     */
    @SneakyThrows
    public void test_writerErase_withMultipleTerms() {
        CacheableClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        // Insert multiple terms
        BytesRef term1 = new BytesRef("term1");
        BytesRef term2 = new BytesRef("term2");
        BytesRef term3 = new BytesRef("term3");

        writer.insert(term1, testClusters);
        writer.insert(term2, testClusters);
        writer.insert(term3, testClusters);

        assertEquals("Cache should have three entries", 3, reader.size());

        // Erase term2
        long bytesFreed = writer.erase(term2);

        // Verify term2 is gone but others remain
        assertTrue("Should free some bytes", bytesFreed > 0);
        assertNotNull("Term1 should still exist", reader.read(term1));
        assertNull("Term2 should be gone", reader.read(term2));
        assertNotNull("Term3 should still exist", reader.read(term3));
        assertEquals("Cache should have two entries", 2, reader.size());
    }

    @SneakyThrows
    public void test_writerErase_updatesRecord() {
        CacheableClusteredPostingWriter writer = cacheItem.getWriter();
        // Insert a term
        writer.insert(testTerm, testClusters);

        verify(globalRecorder, times(1)).record(anyLong());

        // Erase the term
        writer.erase(testTerm);
        // one from constructor, one from erase
        verify(globalRecorder, times(2)).recordWithoutValidation(anyLong(), any());
    }

    /**
     * Tests that erasing an empty BytesRef term works correctly.
     * This verifies the edge case handling for empty terms in the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withEmptyTerm() {
        CacheableClusteredPostingWriter writer = cacheItem.getWriter();
        ClusteredPostingReader reader = cacheItem.getReader();

        // Insert an empty term
        BytesRef emptyTerm = new BytesRef("");
        writer.insert(emptyTerm, testClusters);

        // Verify it exists
        assertNotNull("Empty term should exist after insertion", reader.read(emptyTerm));

        // Erase the empty term
        long bytesFreed = writer.erase(emptyTerm);

        // Verify results
        assertTrue("Should free some bytes", bytesFreed > 0);
        assertNull("Empty term should no longer exist", reader.read(emptyTerm));
        assertEquals("Cache should be empty", 0, reader.size());
    }
}
