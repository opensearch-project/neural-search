/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SeismicBaseScorerTests extends AbstractSparseTestBase {

    // Mocks configuration
    @Mock
    private LeafReader leafReader;

    @Mock
    private SparseQueryContext sparseQueryContext;

    @Mock
    private SparseVectorReader vectorReader;

    @Mock
    private Terms terms;

    @Mock
    private TermsEnum termsEnum;

    @Mock
    private SparsePostingsEnum postingsEnum;

    @Mock
    private Bits acceptedDocs;

    private static final String FIELD_NAME = "test_field";
    private static final int MAX_DOC_COUNT = 10;
    private static final List<String> TEST_TOKENS = Arrays.asList("token1", "token2");
    private static final float HEAP_FACTOR = 2.0f;

    private TestSeismicScorer testScorer;
    private SparseVector queryVector;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        // Initialize mocks
        MockitoAnnotations.openMocks(this);

        // Setup query vector
        queryVector = createVector(1, 5, 2, 3, 3, 7);

        // Setup sparse query context
        when(sparseQueryContext.getTokens()).thenReturn(TEST_TOKENS);
        when(sparseQueryContext.getHeapFactor()).thenReturn(HEAP_FACTOR);

        // Setup terms and termsEnum
        when(leafReader.getBinaryDocValues(eq(FIELD_NAME))).thenReturn(null);
        preparePostings(leafReader, FIELD_NAME, terms, termsEnum, postingsEnum, Map.of("token1", true, "token2", false));
        prepareCluster(postingsEnum);

        // Setup vector reader
        SparseVector docVector = createVector(1, 5, 2, 3);
        when(vectorReader.read(anyInt())).thenReturn(docVector);

        // Create test scorer
        testScorer = new TestSeismicScorer(
            leafReader,
            FIELD_NAME,
            sparseQueryContext,
            MAX_DOC_COUNT,
            queryVector,
            vectorReader,
            acceptedDocs
        );
    }

    public void testInitialize_happyCase() throws IOException {
        // Verify that initialize was called and processed the tokens correctly
        assertEquals(1, testScorer.subScorers.size());
        verify(terms, times(2)).iterator();
        verify(termsEnum).postings(isNull(), eq(8));
        verify(sparseQueryContext).getTokens();
    }

    public void testInitialize_nullTerms() throws IOException {
        when(leafReader.terms(anyString())).thenReturn(null);
        assertEquals(0, testScorer.subScorers.size());
        verify(sparseQueryContext, never()).getTokens();
    }

    public void testInitialize_unexpectedType() throws IOException {
        PostingsEnum postingsEnum1 = mock(PostingsEnum.class);
        when(termsEnum.postings(any())).thenReturn(postingsEnum1);
        testScorer.initialize(leafReader);

    }

    public void testSearchUpfront() throws IOException {
        // Setup acceptedDocs to accept all docs
        when(acceptedDocs.get(anyInt())).thenReturn(true);

        // Call searchUpfront
        List<Pair<Integer, Integer>> results = testScorer.searchUpfront(5);

        // Verify results
        assertNotNull(results);
        assertTrue(results.size() > 0);

        // Verify that vector reader was called
        verify(vectorReader, times(3)).read(anyInt());
    }

    public void testHeapWrapper() {
        // Create a heap wrapper
        SeismicBaseScorer.HeapWrapper heapWrapper = new SeismicBaseScorer.HeapWrapper(3);

        // Add some pairs
        heapWrapper.add(Pair.of(1, 10));
        heapWrapper.add(Pair.of(2, 20));
        heapWrapper.add(Pair.of(3, 30));

        // Verify heap is full
        assertTrue(heapWrapper.isFull());

        // Add a pair with lower score, should not be added
        heapWrapper.add(Pair.of(4, 5));
        assertEquals(3, heapWrapper.size());

        // Add a pair with higher score, should replace lowest score
        heapWrapper.add(Pair.of(5, 40));
        assertEquals(3, heapWrapper.size());

        // Get ordered list
        List<Pair<Integer, Integer>> orderedList = heapWrapper.toOrderedList();
        assertEquals(3, orderedList.size());
        assertEquals(Integer.valueOf(2), orderedList.get(0).getLeft());
        assertEquals(Integer.valueOf(3), orderedList.get(1).getLeft());
        assertEquals(Integer.valueOf(5), orderedList.get(2).getLeft());
    }

    public void testResultsDocValueIterator() throws IOException {
        // Create test results
        List<Pair<Integer, Integer>> results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));

        // Create iterator
        SeismicBaseScorer.ResultsDocValueIterator iterator = new SeismicBaseScorer.ResultsDocValueIterator(results);

        // Test nextDoc
        assertEquals(-1, iterator.docID());
        assertEquals(1, iterator.nextDoc());
        assertEquals(1, iterator.docID());
        assertEquals(1, iterator.docID());
        assertEquals(10, iterator.cost());
        assertEquals(3, iterator.nextDoc());
        assertEquals(3, iterator.docID());
        assertEquals(30, iterator.cost());
        assertEquals(5, iterator.nextDoc());
        assertEquals(5, iterator.docID());
        assertEquals(50, iterator.cost());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());

        // Create new iterator for advance test
        results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));
        results.add(Pair.of(7, 70));

        iterator = new SeismicBaseScorer.ResultsDocValueIterator(results);

        // Test advance
        assertEquals(3, iterator.advance(3));
        assertEquals(3, iterator.docID());
        assertEquals(7, iterator.advance(6));
        assertEquals(7, iterator.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(10));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }

    public void testSingleScorer() throws IOException {
        // Get the SingleScorer from testScorer
        DocIdSetIterator iterator = testScorer.subScorers.get(0).iterator();

        // Test nextDoc
        assertEquals(1, iterator.nextDoc());
        assertEquals(2, iterator.nextDoc());
        assertEquals(3, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    // Test implementation of SeismicBaseScorer for testing
    private static class TestSeismicScorer extends SeismicBaseScorer {

        public TestSeismicScorer(
            LeafReader leafReader,
            String fieldName,
            SparseQueryContext sparseQueryContext,
            int maxDocCount,
            SparseVector queryVector,
            SparseVectorReader reader,
            Bits acceptedDocs
        ) throws IOException {
            super(leafReader, fieldName, sparseQueryContext, maxDocCount, queryVector, reader, acceptedDocs);
        }

        @Override
        public float getMaxScore(int upTo) {
            return 0;
        }

        @Override
        public float score() {
            return 0;
        }

        @Override
        public DocIdSetIterator iterator() {
            return null;
        }

        @Override
        public int docID() {
            return 0;
        }
    }
}
