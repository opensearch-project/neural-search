/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import java.io.IOException;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CacheGatedForwardIndexReaderTests extends AbstractSparseTestBase {

    private final int testDocId = 1;
    private final SparseVector testSparseVector = createVector(1, 5, 2, 3);
    private final SparseVectorReader cacheReader = mock(SparseVectorReader.class);
    private final SparseVectorWriter cacheWriter = mock(SparseVectorWriter.class);
    private final SparseVectorReader luceneReader = mock(SparseVectorReader.class);

    /**
     * Tests the constructor of CacheGatedForwardIndexReader.
     * Verifies that the constructor successfully creates an instance
     * when provided with null parameters.
     */
    public void test_constructor_withNullParameters() {
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(null, null, null);
        assertNotNull("CacheGatedForwardIndexReader should be created successfully", reader);
    }

    /**
     * Test case for the CacheGatedForwardIndexReader constructor.
     * Verifies that the constructor successfully creates an instance
     * when provided with valid non-null parameters.
     */
    public void test_constructor_withValidParameters() {
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(cacheReader, cacheWriter, luceneReader);
        assertNotNull("CacheGatedForwardIndexReader should be created successfully", reader);
    }

    /**
     * Test case for the read method when the vector is found in the cache.
     * This test verifies that the method returns the vector from the cache without accessing Lucene storage.
     */
    public void test_read_whenVectorInCache() throws IOException {
        when(cacheReader.read(anyInt())).thenReturn(testSparseVector);

        // Create the CacheGatedForwardIndexReader instance
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(cacheReader, cacheWriter, luceneReader);

        // Call the method under test
        SparseVector result = reader.read(testDocId);

        // Verify the result
        assertEquals(testSparseVector, result);

        // Verify that luceneReader was not called
        verify(luceneReader, never()).read(anyInt());
    }

    /**
     * Tests the read method when both cache and Lucene storage return null.
     * This scenario verifies that the method correctly handles the case where the
     * requested vector does not exist in either storage.
     */
    public void test_read_whenVectorNotInCacheAndLucene() throws IOException {
        when(cacheReader.read(anyInt())).thenReturn(null);
        when(luceneReader.read(anyInt())).thenReturn(null);

        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(cacheReader, cacheWriter, luceneReader);

        SparseVector result = reader.read(testDocId);

        assertNull(result);
        verify(cacheReader).read(testDocId);
        verify(luceneReader).read(testDocId);
        verify(cacheWriter, never()).insert(anyInt(), any(SparseVector.class));
    }

    /**
     * Test case for read method when the vector is not in cache but exists in Lucene storage.
     * This test verifies that:
     * 1. The method attempts to read from cache first (which returns null)
     * 2. Then reads from Lucene storage successfully
     * 3. The retrieved vector is inserted into the cache
     * 4. The method returns the vector retrieved from Lucene storage
     *
     * @throws IOException if an I/O error occurs during the test
     */
    public void test_read_whenVectorNotInCacheButInLucene() throws IOException {
        when(cacheReader.read(anyInt())).thenReturn(null);
        when(luceneReader.read(anyInt())).thenReturn(testSparseVector);

        // Create the CacheGatedForwardIndexReader instance
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(cacheReader, cacheWriter, luceneReader);

        // Execute the method under test
        SparseVector result = reader.read(testDocId);

        // Verify the result
        assertEquals(testSparseVector, result);

        // Verify that the vector was inserted into the cache
        verify(cacheWriter).insert(testDocId, testSparseVector);
    }
}
