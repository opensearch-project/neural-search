/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

public class ForwardIndexCacheItemTests extends AbstractSparseTestBase {

    private static final int testDocCount = 10;

    private CacheKey cacheKey;
    @Mock
    private SegmentInfo segmentInfo;
    private RamBytesRecorder mockGlobalRamBytesRecorder;
    private ForwardIndexCacheItem cacheItem;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        cacheKey = prepareUniqueCacheKey(segmentInfo);
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        mockGlobalRamBytesRecorder = mock(RamBytesRecorder.class);
        when(mockGlobalRamBytesRecorder.record(anyLong())).thenReturn(true);
        cacheItem = new ForwardIndexCacheItem(cacheKey, testDocCount, mockGlobalRamBytesRecorder);
    }

    @SneakyThrows
    public void test_readerRead_withOutOfBoundVector() {
        SparseVectorReader reader = cacheItem.getReader();
        SparseVector readVector = reader.read(testDocCount + 11);

        assertNull("Read out of bound vector should return null", readVector);
    }

    @SneakyThrows
    public void test_writerInsert_withValidVector() {
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

    @SneakyThrows
    public void test_writerInsert_withOutOfBoundVector() {
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

    @SneakyThrows
    public void test_writerInsert_withNullVector() {
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

    @SneakyThrows
    public void test_writerInsert_skipsDuplicates() {
        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(0, vector1);
        assertEquals("First vector should be inserted", vector1, reader.read(0));

        SparseVector vector2 = createVector(5, 6, 7, 8);
        writer.insert(0, vector2);
        assertEquals("Original vector should remain unchanged", vector1, reader.read(0));
    }

    @SneakyThrows
    public void test_writerInsert_whenRecordIsFalse() {
        when(mockGlobalRamBytesRecorder.record(anyLong())).thenReturn(false);
        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertNull("Read vector should be null", readVector);
        verify(mockGlobalRamBytesRecorder, times(2)).record(anyLong());
    }

    @SneakyThrows
    public void test_ramBytesUsed_withDifferentVectorSize() {
        int docCount1 = 10, docCount2 = 20;
        ForwardIndexCacheItem cacheItem = new ForwardIndexCacheItem(cacheKey, docCount1, mockGlobalRamBytesRecorder);
        long ramBytesUsed1 = cacheItem.ramBytesUsed();

        ForwardIndexCache.getInstance().removeIndex(cacheKey);
        cacheItem = new ForwardIndexCacheItem(cacheKey, docCount2, mockGlobalRamBytesRecorder);
        long ramBytesUsed2 = cacheItem.ramBytesUsed();

        assertTrue("Initial RAM usage should increase when the size of forward index increases", ramBytesUsed2 > ramBytesUsed1);
    }

    @SneakyThrows
    public void test_writerErase_withOutOfBoundVector() {
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        long bytesFreed = writer.erase(testDocCount + 1);
        assertEquals("Erasing out of bounds vector should free 0 bytes", 0, bytesFreed);
    }

    @SneakyThrows
    public void test_writerErase_withNullVector() {
        CacheableSparseVectorWriter writer = cacheItem.getWriter();

        long bytesFreed = writer.erase(0);
        assertEquals("Erasing null vector should free 0 bytes", 0, bytesFreed);
    }

    @SneakyThrows
    public void test_writerErase_withValidVector() {
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
        verify(mockGlobalRamBytesRecorder).safeRecord(eq(-vectorSize), any());
    }

    @SneakyThrows
    public void test_writerErase_withAlreadyErasedVector() {
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

    @SneakyThrows
    public void test_writerErase_withMultipleVectors() {
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

    @SneakyThrows
    public void test_ramBytesUsed_withInsertedVector() {
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

    public void test_create_withMultipleIndices() {
        ForwardIndexCacheItem cacheItem1 = new ForwardIndexCacheItem(cacheKey, testDocCount, mockGlobalRamBytesRecorder);

        CacheKey cacheKey2 = prepareUniqueCacheKey(segmentInfo);
        ForwardIndexCacheItem cacheItem2 = new ForwardIndexCacheItem(cacheKey2, testDocCount, mockGlobalRamBytesRecorder);

        assertNotSame("Should be different index instances", cacheItem1, cacheItem2);
        ForwardIndexCache.getInstance().removeIndex(cacheKey2);
    }

    @SneakyThrows
    public void test_getWriter_withCircuitBreakerHandler() {
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

    @SneakyThrows
    public void test_writerInsert_withCircuitBreakerHandler_whenCircuitBreakerDoesNotTrip() {
        Consumer<Long> mockHandler = mock(Consumer.class);
        SparseVectorWriter writer = cacheItem.getWriter(mockHandler);
        SparseVectorReader reader = cacheItem.getReader();

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertEquals("Vector should be inserted successfully", vector, readVector);
        verify(mockHandler, never()).accept(anyLong());
    }

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
