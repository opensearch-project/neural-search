/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class OrderedPostingWithClustersScorerTests extends AbstractSparseTestBase {

    @Mock
    private LeafReader leafReader;
    @Mock
    private Terms terms;
    @Mock
    private TermsEnum termsEnum;
    @Mock
    private SparseVectorReader vectorReader;
    @Mock
    private SparsePostingsEnum postingsEnum1;
    @Mock
    private SparsePostingsEnum postingsEnum2;
    @Mock
    private SparseBinaryDocValuesPassThrough sparseBinaryDocValues;

    private static final String FIELD_NAME = "test_field";
    private static final int MAX_DOC_COUNT = 100;
    private static final float score = 34.0f;
    private static final int K_VALUE = 5;
    private static final List<String> TEST_TOKENS = Arrays.asList("token1", "token2");

    private SparseVector queryVector;
    private List<Pair<Integer, Integer>> searchResults;
    private Similarity.SimScorer simScorer;
    private byte[] queryDenseVector;
    private SparseQueryContext sparseQueryContext;
    private SegmentInfo segmentInfo;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        // Initialize mocks
        MockitoAnnotations.openMocks(this);

        // Setup query vector
        queryVector = createVector(1, 5, 2, 3, 3, 7);
        queryDenseVector = queryVector.toDenseVector();
        searchResults = Arrays.asList(Pair.of(3, 1), Pair.of(2, 2), Pair.of(3, 3));
        // Setup sparse query context
        sparseQueryContext = constructSparseQueryContext(K_VALUE, 1.0f, TEST_TOKENS);

        // Setup leaf reader
        when(leafReader.maxDoc()).thenReturn(MAX_DOC_COUNT);
        preparePostings(leafReader, FIELD_NAME, terms, termsEnum, postingsEnum1, Map.of("token1", true, "token2", false));
        prepareCluster(postingsEnum1);
        // Setup vector reader
        SparseVector docVector = createVector(1, 5, 2, 3);
        when(vectorReader.read(anyInt())).thenReturn(docVector);

        // Setup simScorer
        simScorer = new Similarity.SimScorer() {
            @Override
            public float score(float freq, long norm) {
                return freq;
            }
        };
        when(leafReader.getBinaryDocValues(anyString())).thenReturn(sparseBinaryDocValues);
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        when(sparseBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
    }

    public void testConstructorWithoutFilter() throws IOException {
        // Create a spy of OrderedPostingWithClustersScorer to mock searchUpfront
        OrderedPostingWithClustersScorer scorerSpy = spy(
            new OrderedPostingWithClustersScorer(
                FIELD_NAME,
                sparseQueryContext,
                queryVector,
                leafReader,
                null,
                vectorReader,
                simScorer,
                null
            )
        );

        // Mock searchUpfront to return our predefined results
        doReturn(searchResults).when(scorerSpy).searchUpfront(eq(K_VALUE));

        // Test iterator
        DocIdSetIterator iterator = scorerSpy.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);
        assertEquals(2, iterator.nextDoc());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);
        assertEquals(3, iterator.nextDoc());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testConstructorWithFilter() throws IOException {
        // Create a BitSetIterator for filtering
        FixedBitSet bitSet = new FixedBitSet(MAX_DOC_COUNT);
        bitSet.set(1);
        bitSet.set(5);
        BitSetIterator filterBitSetIterator = new BitSetIterator(bitSet, 2);

        // Create a spy of OrderedPostingWithClustersScorer to mock searchUpfront
        OrderedPostingWithClustersScorer scorerSpy = spy(
            new OrderedPostingWithClustersScorer(
                FIELD_NAME,
                sparseQueryContext,
                queryVector,
                leafReader,
                null,
                vectorReader,
                simScorer,
                filterBitSetIterator
            )
        );

        // Test iterator - should only return doc 1 and 5 (intersection of results and filter)
        DocIdSetIterator iterator = scorerSpy.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testDocID() throws IOException {
        // Create a spy of OrderedPostingWithClustersScorer to mock searchUpfront
        OrderedPostingWithClustersScorer scorerSpy = spy(
            new OrderedPostingWithClustersScorer(
                FIELD_NAME,
                sparseQueryContext,
                queryVector,
                leafReader,
                null,
                vectorReader,
                simScorer,
                null
            )
        );

        // Mock searchUpfront to return our predefined results
        doReturn(searchResults).when(scorerSpy).searchUpfront(eq(K_VALUE));

        // Test docID
        DocIdSetIterator iterator = scorerSpy.iterator();
        assertEquals(-1, scorerSpy.docID());

        iterator.nextDoc();
        assertEquals(1, scorerSpy.docID());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);

        iterator.nextDoc();
        assertEquals(2, scorerSpy.docID());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);

        iterator.nextDoc();
        assertEquals(3, scorerSpy.docID());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);

        iterator.nextDoc();
        assertEquals(NO_MORE_DOCS, scorerSpy.docID());
    }

    public void testComplexHappyCase() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.seekExact(new BytesRef("token2"))).thenReturn(true);

        // Mock SparsePostingsEnum
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1).thenReturn(postingsEnum2);

        // Mock clusters in term1
        DocumentCluster cluster1_1 = prepareCluster(10, false, queryDenseVector);
        DocumentCluster cluster1_2 = prepareCluster(0, true, queryDenseVector);
        DocumentCluster cluster1_3 = prepareCluster(0, false, queryDenseVector);
        DocumentCluster cluster2_1 = prepareCluster(3, false, queryDenseVector);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator1 = mock(IteratorWrapper.class);
        IteratorWrapper<DocumentCluster> clusterIterator2 = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator1);
        when(postingsEnum2.clusterIterator()).thenReturn(clusterIterator2);
        when(clusterIterator1.next()).thenReturn(cluster1_1).thenReturn(cluster1_2).thenReturn(cluster1_3).thenReturn(null);
        when(clusterIterator2.next()).thenReturn(cluster2_1).thenReturn(null);
        when(leafReader.maxDoc()).thenReturn(15);
        prepareClusterAndItsDocs(vectorReader, queryDenseVector, cluster1_1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6); // first cluster will be examined
        prepareClusterAndItsDocs(vectorReader, queryDenseVector,cluster1_2, 7, 0, 8, 1, 9, 2, 10, 5, 11, 2); // second cluster should not be skipped
        prepareClusterAndItsDocs(vectorReader, queryDenseVector,cluster1_3, 12, 100); // third cluster will be skipped
        prepareClusterAndItsDocs(vectorReader, queryDenseVector,cluster2_1, 13, 10); // fourth cluster will be examined

        // Create scorer
        OrderedPostingWithClustersScorer scorer = new OrderedPostingWithClustersScorer(
            FIELD_NAME,
            sparseQueryContext,
            queryVector,
            leafReader,
            null,
            vectorReader,
            simScorer,
            null
        );

        // Test iterator - should skip the low score cluster and process the high score one
        verifyDocIDs(Arrays.asList(4, 5, 6, 10, 13), scorer);
    }

    public void testBasicScoring() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.seekExact(new BytesRef("token2"))).thenReturn(false);

        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);

        // Mock document clusters
        DocumentCluster cluster = mock(DocumentCluster.class);
        SparseVector clusterSummary = mock(SparseVector.class);
        when(cluster.getSummary()).thenReturn(clusterSummary);
        when(clusterSummary.dotProduct(queryDenseVector)).thenReturn(10);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        // Mock DocWeightIterator
        DocWeightIterator docIterator = mock(DocWeightIterator.class);
        when(cluster.getDisi()).thenReturn(docIterator);
        when(docIterator.nextDoc()).thenReturn(1).thenReturn(NO_MORE_DOCS);
        when(docIterator.docID()).thenReturn(1).thenReturn(NO_MORE_DOCS);

        // Mock SparseVectorReader
        SparseVectorReader reader = mock(SparseVectorReader.class);
        SparseVector docVector = mock(SparseVector.class);
        when(reader.read(1)).thenReturn(docVector);
        when(docVector.dotProduct(queryDenseVector)).thenReturn(5);

        // Create scorer
        OrderedPostingWithClustersScorer scorer = new OrderedPostingWithClustersScorer(
            FIELD_NAME,
            sparseQueryContext,
            queryVector,
            leafReader,
            null,
            reader,
            simScorer,
            null
        );

        // Test iterator
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(5, scorer.score(), DELTA_FOR_ASSERTION);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testDocumentFiltering() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);

        // Mock SparsePostingsEnum
        SparsePostingsEnum postingsEnum = mock(SparsePostingsEnum.class);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false, queryDenseVector);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        // Mock DocWeightIterator - returns 3 docs: deleted, already visited, and valid
        DocWeightIterator docIterator = constructDocWeightIterator(Arrays.asList(10, 20, 30, 20), Arrays.asList(1, 2, 3, 4));
        when(cluster.getDisi()).thenReturn(docIterator);

        // Mock acceptedDocs (live docs) - doc 10 is deleted
        Bits acceptedDocs = mock(Bits.class);
        when(acceptedDocs.get(10)).thenReturn(false);
        when(acceptedDocs.get(20)).thenReturn(true);
        when(acceptedDocs.get(30)).thenReturn(true);

        // Mock SparseVectorReader
        SparseVectorReader reader = mock(SparseVectorReader.class);
        SparseVector docVector = mock(SparseVector.class);
        when(reader.read(30)).thenReturn(docVector);
        // make first access to doc 20 return due to null doc, but second time due to visited.
        when(reader.read(20)).thenReturn(null).thenReturn(docVector);
        when(docVector.dotProduct(queryDenseVector)).thenReturn(15);

        // Create scorer
        OrderedPostingWithClustersScorer scorer = new OrderedPostingWithClustersScorer(
            FIELD_NAME,
            sparseQueryContext,
            queryVector,
            leafReader,
            acceptedDocs,
            reader,
            simScorer,
            null
        );

        // First call to nextDoc() - should skip doc 10 (deleted) and doc 20 (already visited)
        // and return doc 30
        DocIdSetIterator iterator = scorer.iterator();

        // Mark doc 20 as already visited (simulate previous iteration)
        iterator.nextDoc(); // This will mark doc 10 as visited (though it's deleted)
        assertEquals(30, iterator.docID());
        assertEquals(15, scorer.score(), DELTA_FOR_ASSERTION);

        // No more docs
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testPriorityQueue() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);

        // Mock SparsePostingsEnum
        SparsePostingsEnum postingsEnum = mock(SparsePostingsEnum.class);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum);
        // cluster 1 will always be examined, cluster2 will be not as its dp score won't surpass heap's lowest
        DocumentCluster cluster1 = prepareCluster(10, false, queryDenseVector);
        DocumentCluster cluster2 = prepareCluster(9, false, queryDenseVector);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster1).thenReturn(cluster2).thenReturn(null);

        prepareClusterAndItsDocs(vectorReader, queryDenseVector, cluster1, 1, 5, 2, 10, 3, 15);
        prepareClusterAndItsDocs(vectorReader, queryDenseVector, cluster2, 2, 10, 3, 15);

        // Create scorer
        OrderedPostingWithClustersScorer scorer = new OrderedPostingWithClustersScorer(FIELD_NAME, sparseQueryContext, queryVector,
            leafReader, null, vectorReader, simScorer, null
        );

        // Process all documents
        DocIdSetIterator iterator = scorer.iterator();

        // First doc (doc 1)
        assertEquals(1, iterator.nextDoc());
        assertEquals(5, scorer.score(), DELTA_FOR_ASSERTION);

        // Second doc (doc 2)
        assertEquals(2, iterator.nextDoc());
        assertEquals(10, scorer.score(), DELTA_FOR_ASSERTION);

        // Third doc (doc 3)
        assertEquals(3, iterator.nextDoc());
        assertEquals(15, scorer.score(), DELTA_FOR_ASSERTION);

        // No more docs
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testHeapFactor() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);

        // Mock SparsePostingsEnum
        SparsePostingsEnum postingsEnum = mock(SparsePostingsEnum.class);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum);
        // cluster 1 will always be examined, cluster2 will be not as its dp score won't surpass heap's lowest
        DocumentCluster cluster1 = prepareCluster(10, false, queryDenseVector);
        DocumentCluster cluster2 = prepareCluster(1, false, queryDenseVector);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster1).thenReturn(cluster2).thenReturn(null);

        prepareClusterAndItsDocs(vectorReader, queryDenseVector, cluster1, 1, 12, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10);
        prepareClusterAndItsDocs(vectorReader, queryDenseVector, cluster2, 11, 11);

        // Create scorer
        SparseQueryContext context = SparseQueryContext.builder()
            .heapFactor(2.0f)
            .k(2)
            .tokens(TEST_TOKENS)
            .build();
        OrderedPostingWithClustersScorer scorer = new OrderedPostingWithClustersScorer(FIELD_NAME, context, queryVector,
            leafReader, null, vectorReader, simScorer, null
        );

        // Process all documents
        verifyDocIDs(Arrays.asList(1, 11), scorer);
    }

    public void testNullSparseVectorReaderThenThrowException() {
        // Test behavior with null merge sparse vector reader - should throw NullPointerException within constructor
        NullPointerException nullPointerException = assertThrows(
            NullPointerException.class,
            () -> new OrderedPostingWithClustersScorer(FIELD_NAME, sparseQueryContext, queryVector, leafReader, null, null, simScorer, null)
        );

        assertEquals("reader is marked non-null but is null", nullPointerException.getMessage());
    }

    public void testMissingVector() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false, queryDenseVector);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        // Mock DocWeightIterator with two docs - one with vector and one without
        DocWeightIterator docIterator = constructDocWeightIterator(1, 2);
        when(cluster.getDisi()).thenReturn(docIterator);
        // Doc 1 has no vector
        when(vectorReader.read(1)).thenReturn(null);
        // Doc 2 has a vector
        SparseVector docVector = prepareVector(2, 5, vectorReader, queryDenseVector);

        // Create scorer
        OrderedPostingWithClustersScorer scorer = new OrderedPostingWithClustersScorer(
            FIELD_NAME,
            sparseQueryContext,
            queryVector,
            leafReader,
            null,
            vectorReader,
            simScorer,
            null
        );

        // Test iterator - should skip doc 1 (no vector) and return doc 2
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(2, iterator.nextDoc());
        assertEquals(5, scorer.score(), DELTA_FOR_ASSERTION);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    private void verifyDocIDs(List<Integer> expectedDocIds, Scorer scorer) throws IOException {
        DocIdSetIterator iterator = scorer.iterator();
        List<Integer> actualDocIds = new ArrayList<>();
        int doc = iterator.nextDoc();
        while (iterator.docID() != NO_MORE_DOCS) {
            actualDocIds.add(doc);
            doc = iterator.nextDoc();
        }
        assertEquals(expectedDocIds, actualDocIds);
    }
}
