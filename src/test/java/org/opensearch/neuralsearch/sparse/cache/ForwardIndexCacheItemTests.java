/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.index.SegmentInfo;
import org.junit.After;
import org.junit.Before;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

public class ForwardIndexCacheItemTests extends AbstractSparseTestBase {

    private static final int testDocCount = 10;
    private static final String testFieldName = "test_field";

    private CacheKey cacheKey;
    private SegmentInfo segmentInfo;

    /**
     * Set up the test environment before each test.
     * Creates a segment info and cache key for testing.
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();

        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        cacheKey = new CacheKey(segmentInfo, testFieldName);
    }

    /**
     * Tear down the test environment after each test.
     * Removes the test index from the cache.
     */
    @After
    @Override
    public void tearDown() throws Exception {
        ForwardIndexCache.getInstance().removeIndex(cacheKey);
        super.tearDown();
    }

    /**
     * Tests that reading a vector with an out-of-bounds index returns null.
     * This verifies the bounds checking in the SparseVectorReader.
     */
    @SneakyThrows
    public void test_readerRead_withOutOfBoundVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVector readVector = reader.read(testDocCount + 11);

        assertNull("Read out of bound vector should return null", readVector);
    }

    /**
     * Tests that a vector can be successfully inserted and read back.
     * This verifies the basic functionality of the writer and reader.
     */
    @SneakyThrows
    public void test_writerInsert_withValidVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertEquals("Read vector should match inserted vector", vector, readVector);
    }

    /**
     * Tests that inserting a vector with an out-of-bounds index is ignored.
     * This verifies the bounds checking in the SparseVectorWriter.
     */
    @SneakyThrows
    public void test_writerInsert_withOutOfBoundVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        long ramBytesUsed1 = cacheItem.ramBytesUsed();
        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(testDocCount + 1, vector1);
        long ramBytesUsed2 = cacheItem.ramBytesUsed();
        assertEquals("Inserting vector out of bound will not increase memory usage", ramBytesUsed1, ramBytesUsed2);
    }

    /**
     * Tests that inserting a vector at an index that already has a vector is ignored.
     * This verifies that the writer doesn't overwrite existing vectors.
     */
    @SneakyThrows
    public void test_writerInsert_withNullVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        // Test inserting null vector should be ignored and does not throw exception
        writer.insert(2, null);
        assertNull("Vector should still be null", reader.read(2));
    }

    /**
     * Tests that inserting a vector at an index that already has a vector is ignored.
     * This verifies that the writer doesn't overwrite existing vectors.
     */
    @SneakyThrows
    public void test_writerInsert_skipsDuplicates() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(0, vector1);
        assertEquals("First vector should be inserted", vector1, reader.read(0));

        SparseVector vector2 = createVector(5, 6, 7, 8);
        writer.insert(0, vector2);
        assertEquals("Original vector should remain unchanged", vector1, reader.read(0));
    }

    /**
     * Tests that vector insertion fails gracefully when the circuit breaker throws an exception.
     * This verifies the error handling in the SparseVectorWriter.
     */
    @SneakyThrows
    public void test_writerInsert_whenCircuitBreakerThrowException() {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertNull("Read vector should be null", readVector);
    }

    /**
     * Tests that ramBytesUsed correctly records the memory on the sparse vector array
     */
    @SneakyThrows
    public void test_ramBytesUsed_withDifferentVectorSize() {
        int docCount1 = 10, docCount2 = 20;
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, docCount1);
        long ramBytesUsed1 = cacheItem.ramBytesUsed();

        ForwardIndexCache.getInstance().removeIndex(cacheKey);
        cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, docCount2);
        long ramBytesUsed2 = cacheItem.ramBytesUsed();

        assertTrue("Initial RAM usage should increase when the size of forward index increases", ramBytesUsed2 > ramBytesUsed1);
    }

    /**
     * Tests that erasing a vector with an out-of-bounds index returns 0 bytes freed.
     * This verifies the bounds checking in the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withOutOfBoundVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        long bytesFreed = writer.erase(testDocCount + 1);
        assertEquals("Erasing out of bounds vector should free 0 bytes", 0, bytesFreed);
    }

    /**
     * Tests that erasing a null vector (one that doesn't exist) returns 0 bytes freed.
     * This verifies the null checking in the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withNullVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        long bytesFreed = writer.erase(0);
        assertEquals("Erasing null vector should free 0 bytes", 0, bytesFreed);
    }

    /**
     * Tests that a vector can be successfully erased and memory is properly released.
     * This verifies the basic functionality of the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withValidVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        SparseVectorReader reader = cacheItem.getReader();
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        // Insert a vector
        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);
        assertEquals("Vector should be inserted", vector, reader.read(0));

        long initialRam = cacheItem.ramBytesUsed();
        long vectorSize = vector.ramBytesUsed();

        // Erase the vector
        long bytesFreed = writer.erase(0);

        // Verify the vector was erased
        assertNull("Vector should be erased", reader.read(0));
        assertEquals("Bytes freed should match vector size", vectorSize, bytesFreed);
        assertEquals("RAM usage should decrease by vector size", initialRam - vectorSize, cacheItem.ramBytesUsed());

        // Verify CircuitBreakerManager was called to release bytes
        verify(mockedCircuitBreaker).addWithoutBreaking(-vectorSize);
    }

    /**
     * Tests that erasing a vector that was already erased returns 0 bytes freed.
     * This verifies the compareAndSet logic in the erase method.
     */
    @SneakyThrows
    public void test_writerErase_withAlreadyErasedVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        SparseVectorReader reader = cacheItem.getReader();
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        // Insert and then erase a vector
        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);
        long firstErase = writer.erase(0);

        // Try to erase it again
        long secondErase = writer.erase(0);

        assertEquals("First erase should return vector size", vector.ramBytesUsed(), firstErase);
        assertEquals("Second erase should return 0 bytes", 0, secondErase);
    }

    /**
     * Tests that multiple vectors can be inserted and erased correctly.
     * This verifies the erase method works with multiple vectors.
     */
    @SneakyThrows
    public void test_writerErase_withMultipleVectors() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        SparseVectorReader reader = cacheItem.getReader();
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        // Insert multiple vectors
        SparseVector vector1 = createVector(1, 2, 3, 4);
        SparseVector vector2 = createVector(5, 6, 7, 8);
        writer.insert(0, vector1);
        writer.insert(1, vector2);

        long initialRam = cacheItem.ramBytesUsed();

        // Erase one vector
        long bytesFreed = writer.erase(0);

        // Verify only the specified vector was erased
        assertNull("Vector 0 should be erased", reader.read(0));
        assertEquals("Vector 1 should still exist", vector2, reader.read(1));
        assertEquals("Bytes freed should match vector1 size", vector1.ramBytesUsed(), bytesFreed);
        assertEquals("RAM usage should decrease by vector1 size", initialRam - vector1.ramBytesUsed(), cacheItem.ramBytesUsed());
    }

    /**
     * Tests that ramBytesUsed correctly reports the memory usage after vectors are inserted.
     * This verifies the memory tracking functionality of the ForwardIndexCacheItem.
     */
    @SneakyThrows
    public void test_ramBytesUsed_withInsertedVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        long initialRam = cacheItem.ramBytesUsed();
        SparseVectorWriter writer = cacheItem.getWriter();
        SparseVector vector1 = createVector(1, 2, 3, 4);
        SparseVector vector2 = createVector(5, 6, 7, 8, 9, 10);
        writer.insert(0, vector1);
        writer.insert(1, vector2);
        long ramWithVectors = cacheItem.ramBytesUsed();

        assertEquals(
            "RAM usage should increase by the size of inserted vectors",
            ramWithVectors - initialRam,
            vector1.ramBytesUsed() + vector2.ramBytesUsed()
        );
    }

    /**
     * Tests that creating multiple indices with different keys returns different instances.
     * This verifies that the ForwardIndexCache correctly differentiates between different keys.
     */
    public void test_create_withMultipleIndices() {
        ForwardIndexCacheItem cacheItem1 = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        CacheKey cacheKey2 = new CacheKey(segmentInfo, "another_field");
        ForwardIndexCacheItem cacheItem2 = ForwardIndexCache.getInstance().getOrCreate(cacheKey2, testDocCount);

        assertNotSame("Should be different index instances", cacheItem1, cacheItem2);
    }

    /**
     * Tests that getWriter with circuitBreakerHandler returns a writer with the handler.
     * This verifies the new getWriter method functionality.
     */
    @SneakyThrows
    public void test_getWriter_withCircuitBreakerHandler() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        Consumer<Long> mockHandler = mock(Consumer.class);
        SparseVectorWriter writer = cacheItem.getWriter(mockHandler);
        SparseVectorReader reader = cacheItem.getReader();

        assertNotNull("Writer should not be null", writer);

        // Insert should work normally when circuit breaker doesn't trip
        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertEquals("Should be able to retrieve vector", vector, readVector);
        verify(mockHandler, never()).accept(anyLong());
    }

    /**
     * Tests that circuitBreakerHandler is called when circuit breaker trips.
     * This verifies the circuit breaker handler functionality.
     */
    @SneakyThrows
    public void test_writerInsert_withCircuitBreakerHandler_whenCircuitBreakerTrips() {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        Consumer<Long> mockHandler = mock(Consumer.class);
        SparseVectorWriter writer = cacheItem.getWriter(mockHandler);
        SparseVectorReader reader = cacheItem.getReader();

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertNull("Vector should not be inserted when circuit breaker trips", readVector);
        verify(mockHandler).accept(anyLong());
    }

    /**
     * Tests that circuitBreakerHandler is not called when circuit breaker doesn't trip.
     * This verifies the conditional calling of the handler.
     */
    @SneakyThrows
    public void test_writerInsert_withCircuitBreakerHandler_whenCircuitBreakerDoesNotTrip() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        Consumer<Long> mockHandler = mock(Consumer.class);
        SparseVectorWriter writer = cacheItem.getWriter(mockHandler);
        SparseVectorReader reader = cacheItem.getReader();

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertEquals("Vector should be inserted successfully", vector, readVector);
        verify(mockHandler, never()).accept(anyLong());
    }

    /**
     * Tests that default writer (without handler) works correctly when circuit breaker trips.
     * This verifies backward compatibility.
     */
    @SneakyThrows
    public void test_defaultWriter_whenCircuitBreakerTrips() {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        SparseVectorWriter writer = cacheItem.getWriter();
        SparseVectorReader reader = cacheItem.getReader();

        // Should not throw exception even without handler
        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertNull("Vector should not be inserted when circuit breaker trips", readVector);
    }
}
