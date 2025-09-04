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
import org.opensearch.neuralsearch.sparse.common.DocWeightIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SeismicBaseScorerTests extends AbstractSparseTestBase {

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

    private DocumentCluster cluster;

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
        cluster = prepareCluster(postingsEnum);

        // Setup vector reader
        SparseVector docVector = createVector(1, 5, 2, 3);
        when(vectorReader.read(anyInt())).thenReturn(docVector);
        when(acceptedDocs.get(anyInt())).thenReturn(true);
    }

    public void testInitialize_happyCase() throws IOException {
        init();
        // Verify that initialize was called and processed the tokens correctly
        assertEquals(1, testScorer.subScorers.size());
        verify(terms, times(2)).iterator();
        verify(termsEnum).postings(isNull(), eq(8));
        verify(sparseQueryContext).getTokens();
    }

    public void testInitialize_nullTerms() throws IOException {
        when(leafReader.terms(anyString())).thenReturn(null);
        init();
        assertEquals(0, testScorer.subScorers.size());
    }

    public void testInitialize_unexpectedType() throws IOException {
        PostingsEnum postingsEnum1 = mock(PostingsEnum.class);
        when(termsEnum.postings(any(), anyInt())).thenReturn(postingsEnum1);
        expectThrows(IllegalStateException.class, () -> init());
    }

    public void testInitialize_seekExactFalse() throws IOException {
        when(termsEnum.seekExact(any())).thenReturn(false);
        init();
        assertEquals(0, testScorer.subScorers.size());
    }

    public void testSearchUpfront_happyCase() throws IOException {
        init();
        // Setup acceptedDocs to accept all docs
        when(acceptedDocs.get(anyInt())).thenReturn(true);

        // Call searchUpfront
        List<Pair<Integer, Integer>> results = testScorer.searchUpfront(5);

        // Verify results
        assertEquals(3, results.size());

        // Verify that vector reader was called
        verify(vectorReader, times(3)).read(anyInt());
    }

    public void testSearchUpfront_acceptedDocsIsNull() throws IOException {
        testScorer = new TestSeismicScorer(leafReader, FIELD_NAME, sparseQueryContext, MAX_DOC_COUNT, queryVector, vectorReader, null);
        // Call searchUpfront
        List<Pair<Integer, Integer>> results = testScorer.searchUpfront(5);

        // Verify results
        assertEquals(3, results.size());

        // Verify that vector reader was called
        verify(vectorReader, times(3)).read(anyInt());
    }

    public void testSearchUpfront_someDocsAreNotAccepted() throws IOException {
        init();
        int expectedDocsCount = 2;
        when(acceptedDocs.get(eq(1))).thenReturn(false);
        when(acceptedDocs.get(eq(2))).thenReturn(true);
        when(acceptedDocs.get(eq(3))).thenReturn(true);
        List<Pair<Integer, Integer>> results = testScorer.searchUpfront(5);

        // Verify results
        assertEquals(expectedDocsCount, results.size());

        // Verify that vector reader was called
        verify(vectorReader, times(expectedDocsCount)).read(anyInt());
    }

    public void testSearchUpfront_visitedDocs() throws IOException {
        init();
        int expectedDocsCount = 3;
        when(acceptedDocs.get(anyInt())).thenReturn(true);
        DocWeightIterator docWeightIterator = constructDocWeightIterator(1, 2, 3, 2);
        when(cluster.getDisi()).thenReturn(docWeightIterator);
        List<Pair<Integer, Integer>> results = testScorer.searchUpfront(5);

        // Verify results
        assertEquals(expectedDocsCount, results.size());

        // Verify that vector reader was called
        verify(vectorReader, times(expectedDocsCount)).read(anyInt());
    }

    public void testSearchUpfront_readerReturnNull() throws IOException {
        init();
        int expectedDocsCount = 2;
        when(vectorReader.read(eq(1))).thenReturn(null);
        List<Pair<Integer, Integer>> results = testScorer.searchUpfront(5);

        // Verify results
        assertEquals(expectedDocsCount, results.size());

        // Verify that vector reader was called
        verify(vectorReader, times(expectedDocsCount + 1)).read(anyInt());
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
        assertEquals(2, orderedList.get(0).getLeft().intValue());
        assertEquals(3, orderedList.get(1).getLeft().intValue());
        assertEquals(5, orderedList.get(2).getLeft().intValue());

        assertEquals(20, heapWrapper.peek().getRight().intValue());
    }

    public void testResultsDocValueIterator() throws IOException {
        init();
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
    }

    public void testResultsDocValueIterator_advance() throws IOException {
        init();
        // Create new iterator for advance test
        List<Pair<Integer, Integer>> results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));
        results.add(Pair.of(7, 70));

        SeismicBaseScorer.ResultsDocValueIterator iterator = new SeismicBaseScorer.ResultsDocValueIterator(results);
        // Test advance
        assertEquals(1, iterator.nextDoc());
        assertEquals(1, iterator.advance(0));
        assertEquals(3, iterator.advance(3));
        assertEquals(3, iterator.docID());
        assertEquals(7, iterator.advance(6));
        assertEquals(7, iterator.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(10));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }

    public void testResultsDocValueIterator_cost() throws IOException {
        init();
        // Create new iterator for advance test
        List<Pair<Integer, Integer>> results = new ArrayList<>();
        results.add(Pair.of(1, 10));
        results.add(Pair.of(3, 30));
        results.add(Pair.of(5, 50));
        results.add(Pair.of(7, 70));

        SeismicBaseScorer.ResultsDocValueIterator iterator = new SeismicBaseScorer.ResultsDocValueIterator(results);
        assertEquals(0, iterator.cost());
        assertEquals(1, iterator.nextDoc());
        assertEquals(10, iterator.cost());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(100));
        assertEquals(0, iterator.cost());
    }

    public void testSingleScorer_basic() throws IOException {
        init();
        // Get the SingleScorer from testScorer
        DocIdSetIterator iterator = testScorer.subScorers.getFirst().iterator();

        // Test nextDoc
        assertEquals(-1, testScorer.subScorers.getFirst().docID());
        assertEquals(1, testScorer.subScorers.getFirst().iterator().nextDoc());
        assertEquals(1, testScorer.subScorers.getFirst().docID());
        assertEquals(0, testScorer.subScorers.getFirst().getMaxScore(0), DELTA_FOR_ASSERTION);
        assertEquals(0, testScorer.subScorers.getFirst().score(), DELTA_FOR_ASSERTION);
    }

    public void testSingleScorer_iterator() throws IOException {
        init();
        // Get the SingleScorer from testScorer
        DocIdSetIterator iterator = testScorer.subScorers.get(0).iterator();

        // Test nextDoc
        assertEquals(-1, iterator.docID());
        assertEquals(1, iterator.nextDoc());
        assertEquals(0, iterator.advance(3));
        assertEquals(0, iterator.cost());
        assertEquals(2, iterator.nextDoc());
        assertEquals(3, iterator.nextDoc());
        assertEquals(3, iterator.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testSingleScorer_multipleClusters() throws IOException {
        DocumentCluster cluster2 = mock(DocumentCluster.class);
        DocWeightIterator docWeightIterator = constructDocWeightIterator(4, 5, 6);
        when(cluster2.getDisi()).thenReturn(docWeightIterator);
        when(cluster2.isShouldNotSkip()).thenReturn(false);

        DocumentCluster cluster3 = mock(DocumentCluster.class);
        docWeightIterator = constructDocWeightIterator(7, 8, 9, 10, 11, 12);
        when(cluster3.getDisi()).thenReturn(docWeightIterator);
        when(cluster3.isShouldNotSkip()).thenReturn(true);

        DocumentCluster cluster4 = mock(DocumentCluster.class);
        docWeightIterator = constructDocWeightIterator(13, 14);
        when(cluster4.getDisi()).thenReturn(docWeightIterator);
        when(cluster4.isShouldNotSkip()).thenReturn(false);

        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(clusterIterator.next()).thenReturn(cluster, cluster2, cluster3, cluster4, null);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);

        when(cluster2.getSummary()).thenReturn(createVector(1, 4, 2, 5));
        when(cluster3.getSummary()).thenReturn(createVector(3, 6, 2, 8));
        SparseVector mockSummary = mock(SparseVector.class);
        when(mockSummary.dotProduct(any())).thenReturn(0);
        when(cluster4.getSummary()).thenReturn(mockSummary);
        init();

        // Get the SingleScorer from testScorer
        DocIdSetIterator iterator = testScorer.subScorers.get(0).iterator();

        // Test nextDoc
        for (int i = 1; i < 13; ++i) {
            assertEquals(i, iterator.nextDoc());
            testScorer.scoreHeap.add(Pair.of(i, i));
        }
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

    private void init() throws IOException {
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
}
