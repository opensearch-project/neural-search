/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseTermsTests extends AbstractSparseTestBase {

    private static final String TEST_FIELD = "test_field";

    @Mock
    private CacheKey mockCacheKey;
    @Mock
    private SparseTermsLuceneReader mockReader;

    private Set<BytesRef> terms;
    private SparseTerms sparseTerms;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        terms = new HashSet<>();
        terms.add(new BytesRef("term"));
        when(mockReader.getTerms(TEST_FIELD)).thenReturn(terms);
        ClusteredPostingCache.getInstance().getOrCreate(mockCacheKey);
        sparseTerms = new SparseTerms(mockCacheKey, mockReader, TEST_FIELD);
    }

    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        ClusteredPostingCache.getInstance().removeIndex(mockCacheKey);
        super.tearDown();
    }

    @SneakyThrows
    public void testConstructor() {
        SparseTerms sparseTerms = new SparseTerms(mockCacheKey, mockReader, TEST_FIELD);
        assertNotNull(sparseTerms);
        assertEquals(mockCacheKey, sparseTerms.getCacheKey());
        // verify that sparseTerms.reader creates successfully by calling size
        assertEquals(1, sparseTerms.size());
    }

    public void testIterator() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        assertNotNull(termsEnum);
        assertTrue(termsEnum instanceof SparseTerms.SparseTermsEnum);
    }

    public void testSize() throws IOException {
        int expectedSize = terms.size();

        assertEquals(expectedSize, sparseTerms.size());
        verify(mockReader, times(1)).getTerms(TEST_FIELD);
    }

    public void testGetSumTotalTermFreq() throws IOException {
        assertEquals(0, sparseTerms.getSumTotalTermFreq());
    }

    public void testGetSumDocFreq() throws IOException {
        assertEquals(0, sparseTerms.getSumDocFreq());
    }

    public void testGetDocCount() throws IOException {
        assertEquals(0, sparseTerms.getDocCount());
    }

    public void testHasFreqs() {
        assertFalse(sparseTerms.hasFreqs());
    }

    public void testHasOffsets() {
        assertFalse(sparseTerms.hasOffsets());
    }

    public void testHasPositions() {
        assertFalse(sparseTerms.hasPositions());
    }

    public void testHasPayloads() {
        assertFalse(sparseTerms.hasPayloads());
    }

    public void testSparseTermsEnum_constructor() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        assertNotNull(termsEnum);
        verify(mockReader, times(1)).getTerms(TEST_FIELD);
        assertNotNull(termsEnum.next());
    }

    public void testSparseTermsEnum_constructor_NullTerms() throws IOException {
        when(mockReader.getTerms(TEST_FIELD)).thenReturn(null);

        TermsEnum termsEnum = sparseTerms.iterator();

        assertNotNull(termsEnum);
        verify(mockReader, times(1)).getTerms(TEST_FIELD);
        assertNull(termsEnum.next());
    }

    public void testSparseTermsEnum_seekCeil_notFound() throws IOException {
        BytesRef term = new BytesRef("term");
        when(mockReader.read(TEST_FIELD, term)).thenReturn(null);

        TermsEnum termsEnum = sparseTerms.iterator();
        TermsEnum.SeekStatus status = termsEnum.seekCeil(term);

        assertEquals(TermsEnum.SeekStatus.NOT_FOUND, status);
        assertNull(termsEnum.term());
        verify(mockReader, times(1)).read(TEST_FIELD, term);
    }

    public void testSparseTermsEnum_seekCeil_found() throws IOException {
        BytesRef term = new BytesRef("term");
        PostingClusters mockClusters = mock(PostingClusters.class);
        when(mockReader.read(TEST_FIELD, term)).thenReturn(mockClusters);

        TermsEnum termsEnum = sparseTerms.iterator();
        TermsEnum.SeekStatus status = termsEnum.seekCeil(term);

        assertEquals(TermsEnum.SeekStatus.FOUND, status);
        assertEquals(term, termsEnum.term());
        verify(mockReader, times(1)).read(TEST_FIELD, term);
    }

    public void testSparseTermsEnum_seekExact() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        Exception exception = expectThrows(UnsupportedOperationException.class, () -> termsEnum.seekExact(5L));
        assertNull(exception.getMessage());
    }

    public void testSparseTermsEnum_term() throws IOException {
        BytesRef term = new BytesRef("term1");
        PostingClusters mockClusters = mock(PostingClusters.class);
        when(mockReader.read(TEST_FIELD, term)).thenReturn(mockClusters);

        TermsEnum termsEnum = sparseTerms.iterator();
        termsEnum.seekCeil(term);

        assertEquals(term, termsEnum.term());
    }

    public void testSparseTermsEnum_ord() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        Exception exception = expectThrows(UnsupportedOperationException.class, termsEnum::ord);
        assertNull(exception.getMessage());
    }

    public void testSparseTermsEnum_docFreq() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        Exception exception = expectThrows(UnsupportedOperationException.class, termsEnum::docFreq);
        assertNull(exception.getMessage());
    }

    public void testSparseTermsEnum_totalTermFreq() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        Exception exception = expectThrows(UnsupportedOperationException.class, termsEnum::totalTermFreq);
        assertNull(exception.getMessage());
    }

    public void testSparseTermsEnum_postings_nullCurrentTerm() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();
        PostingsEnum postingsEnum = termsEnum.postings(null, 0);

        assertNull(postingsEnum);
        verify(mockReader, never()).read(anyString(), any(BytesRef.class));
    }

    public void testSparseTermsEnum_postings_nullClusters() throws IOException {
        BytesRef term = new BytesRef("term");
        when(mockReader.read(TEST_FIELD, term)).thenReturn(null);

        TermsEnum termsEnum = sparseTerms.iterator();
        termsEnum.next();

        PostingsEnum postingsEnum = termsEnum.postings(null, 0);

        assertNull(postingsEnum);
        verify(mockReader, times(1)).read(TEST_FIELD, term);
    }

    public void testSparseTermsEnum_postings_withClusters() throws IOException {
        BytesRef term = new BytesRef("term");
        PostingClusters mockClusters = preparePostingClusters();
        when(mockReader.read(TEST_FIELD, term)).thenReturn(mockClusters);

        TermsEnum termsEnum = sparseTerms.iterator();
        termsEnum.next();

        PostingsEnum postingsEnum = termsEnum.postings(null, 0);

        assertNotNull(postingsEnum);
        verify(mockReader, times(1)).read(TEST_FIELD, term);
    }

    public void testSparseTermsEnum_impacts() throws IOException {
        TermsEnum termsEnum = sparseTerms.iterator();

        Exception exception = expectThrows(UnsupportedOperationException.class, () -> termsEnum.impacts(0));
        assertNull(exception.getMessage());
    }

    public void testSparseTermsEnum_next_withTerms() throws IOException {
        Set<BytesRef> terms = new HashSet<>();
        BytesRef term1 = new BytesRef("term1");
        terms.add(term1);
        when(mockReader.getTerms(TEST_FIELD)).thenReturn(terms);

        TermsEnum termsEnum = sparseTerms.iterator();
        BytesRef nextTerm = termsEnum.next();

        assertEquals(term1, nextTerm);
        assertEquals(term1, termsEnum.term());
    }

    public void testSparseTermsEnum_next_noMoreTerms() throws IOException {
        Set<BytesRef> terms = new HashSet<>();
        when(mockReader.getTerms(TEST_FIELD)).thenReturn(terms);

        TermsEnum termsEnum = sparseTerms.iterator();
        BytesRef nextTerm = termsEnum.next();

        assertNull(nextTerm);
    }

    public void testSparseTermsEnum_next_nullIterator() throws IOException {
        when(mockReader.getTerms(TEST_FIELD)).thenReturn(null);

        TermsEnum termsEnum = sparseTerms.iterator();
        BytesRef nextTerm = termsEnum.next();

        assertNull(nextTerm);
    }
}
