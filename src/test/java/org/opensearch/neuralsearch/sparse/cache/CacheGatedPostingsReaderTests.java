/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.codec.SparseTermsLuceneReader;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CacheGatedPostingsReaderTests extends AbstractSparseTestBase {

    private final String testFieldName = "test_field";
    private final BytesRef testTerm = new BytesRef("test_term");
    private final ClusteredPostingReader cacheReader = mock(ClusteredPostingReader.class);
    private final ClusteredPostingWriter cacheWriter = mock(ClusteredPostingWriter.class);
    private final SparseTermsLuceneReader luceneReader = mock(SparseTermsLuceneReader.class);
    private final PostingClusters testPostingClusters = preparePostingClusters();

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for fieldName.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void test_constructor_withNullFieldName() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(null, cacheReader, cacheWriter, luceneReader);
        });
        assertEquals("fieldName is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for the cache reader.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void test_constructor_withNullCacheReader() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(testFieldName, null, cacheWriter, luceneReader);
        });
        assertEquals("cacheReader is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for the cache writer.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void test_constructor_withNullCacheWriter() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(testFieldName, cacheReader, null, luceneReader);
        });
        assertEquals("cacheWriter is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for luceneReader.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void test_constructor_withNullLuceneReader() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, null);
        });
        assertEquals("luceneReader is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test case for the CacheGatedPostingsReader constructor.
     * Verifies that the constructor successfully creates an instance
     * when provided with valid non-null parameters.
     */
    public void test_constructor_withValidParameters() {
        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        assertNotNull("CacheGatedPostingsReader should be created successfully", reader);
    }

    /**
     * Test case for the read method when the posting clusters are found in the cache.
     * This test verifies that the method returns the clusters from the cache without accessing Lucene storage.
     */
    public void test_read_whenClusterInCache() throws IOException {
        when(cacheReader.read(any(BytesRef.class))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertEquals(testPostingClusters, result);
        verify(cacheReader).read(testTerm);
        verify(luceneReader, never()).read(anyString(), any(BytesRef.class));
        verify(cacheWriter, never()).insert(any(BytesRef.class), any());
    }

    /**
     * Tests the read method when both cache and Lucene storage return null.
     * This scenario verifies that the method correctly handles the case where the
     * requested posting clusters do not exist in either storage.
     */
    public void test_read_whenClustersNotInCacheFirstTime() throws IOException {
        when(cacheReader.read(any(BytesRef.class))).thenReturn(null).thenReturn(testPostingClusters);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenReturn(null);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertNotNull(result);
        verify(cacheReader, times(2)).read(testTerm);
        verify(luceneReader, never()).read(anyString(), any(BytesRef.class));
        verify(cacheWriter).insert(eq(testTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the read method when both cache and Lucene storage return null.
     * This scenario verifies that the method correctly handles the case where the
     * requested posting clusters do not exist in either storage.
     */
    public void test_read_whenClustersNotInCacheAndLucene() throws IOException {
        when(cacheReader.read(any(BytesRef.class))).thenReturn(null);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenReturn(null);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertNull(result);
        verify(cacheReader, times(2)).read(testTerm);
        verify(luceneReader).read(testFieldName, testTerm);
        verify(cacheWriter, never()).insert(any(BytesRef.class), any());
    }

    /**
     * Test case for read method when the posting clusters are not in cache but exist in Lucene storage.
     * This test verifies that:
     * 1. The method attempts to read from cache first (which returns null)
     * 2. Then reads from Lucene storage successfully
     * 3. The retrieved posting clusters are inserted into the cache
     * 4. The method returns the posting clusters retrieved from Lucene storage
     */
    public void test_read_whenClusterNotInCacheButInLucene() throws IOException {
        when(cacheReader.read(any(BytesRef.class))).thenReturn(null);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertEquals(testPostingClusters, result);
        verify(cacheReader, times(2)).read(testTerm);
        verify(luceneReader).read(testFieldName, testTerm);
        verify(cacheWriter).insert(eq(testTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the getTerms method to verify it returns terms from the Lucene reader.
     * This test ensures that the method correctly delegates to the Lucene reader
     * rather than using the cache, which may be incomplete.
     */
    public void test_getTerms() {
        Set<BytesRef> expectedTerms = new HashSet<>();
        expectedTerms.add(new BytesRef("term1"));
        expectedTerms.add(new BytesRef("term2"));

        when(luceneReader.getTerms(anyString())).thenReturn(expectedTerms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        Set<BytesRef> result = reader.getTerms();

        assertEquals(expectedTerms, result);
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the size method to verify it returns the correct number of terms.
     * This test ensures that the method correctly calculates the size based on
     * the number of terms in the Lucene reader.
     */
    public void test_size() {
        Set<BytesRef> terms = new HashSet<>();
        terms.add(new BytesRef("term1"));
        terms.add(new BytesRef("term2"));
        terms.add(new BytesRef("term3"));

        when(luceneReader.getTerms(anyString())).thenReturn(terms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        long size = reader.size();

        assertEquals(3, size);
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the read method with an empty term.
     * This test verifies that the method handles empty terms correctly.
     */
    public void test_read_withEmptyTerm() throws IOException {
        BytesRef emptyTerm = new BytesRef("");
        when(cacheReader.read(emptyTerm)).thenReturn(null);
        when(luceneReader.read(anyString(), eq(emptyTerm))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        PostingClusters result = reader.read(emptyTerm);

        assertEquals(testPostingClusters, result);
        verify(cacheReader, times(2)).read(emptyTerm);
        verify(luceneReader).read(testFieldName, emptyTerm);
        verify(cacheWriter).insert(eq(emptyTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the getTerms method when the Lucene reader returns an empty set.
     * This test ensures that the method correctly handles the case where there are no terms.
     */
    public void test_getTerms_withEmptySet() {
        Set<BytesRef> emptyTerms = new HashSet<>();
        when(luceneReader.getTerms(anyString())).thenReturn(emptyTerms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        Set<BytesRef> result = reader.getTerms();

        assertTrue(result.isEmpty());
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the size method when there are no terms.
     * This test ensures that the method correctly returns zero when there are no terms.
     */
    public void test_size_withNoTerms() {
        Set<BytesRef> emptyTerms = new HashSet<>();
        when(luceneReader.getTerms(anyString())).thenReturn(emptyTerms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        long size = reader.size();

        assertEquals(0, size);
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the read method with a term that has special characters.
     * This test verifies that the method handles terms with special characters correctly.
     */
    public void test_read_withSpecialCharacterTerm() throws IOException {
        BytesRef specialTerm = new BytesRef("special!@#$%^&*()_+");
        when(cacheReader.read(specialTerm)).thenReturn(null);
        when(luceneReader.read(anyString(), eq(specialTerm))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);
        PostingClusters result = reader.read(specialTerm);

        assertEquals(testPostingClusters, result);
        verify(cacheReader, times(2)).read(specialTerm);
        verify(luceneReader).read(testFieldName, specialTerm);
        verify(cacheWriter).insert(eq(specialTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the read method when an IOException occurs while reading from Lucene.
     * This test verifies that the method properly propagates the exception.
     */
    public void test_read_withIOException() throws IOException {
        when(cacheReader.read(any(BytesRef.class))).thenReturn(null);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenThrow(new IOException("Test IO Exception"));

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, cacheReader, cacheWriter, luceneReader);

        IOException exception = expectThrows(IOException.class, () -> {
            reader.read(testTerm);
        });

        assertEquals("Test IO Exception", exception.getMessage());
        verify(cacheReader, times(2)).read(testTerm);
        verify(luceneReader).read(testFieldName, testTerm);
        verify(cacheWriter, never()).insert(any(BytesRef.class), any());
    }
}
