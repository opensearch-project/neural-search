/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.codec.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

public class PostingWithClustersScorerTests extends AbstractSparseTestBase {

    private static final String FIELD_NAME = "test_field";

    // Test constants
    private static final int K = 2;
    private static final float HEAP_FACTOR = 1.0f;
    private static final List<String> TOKENS = Arrays.asList("token1", "token2");
    private static final Similarity.SimScorer simScorer = new Similarity.SimScorer() {
        @Override
        public float score(float freq, long norm) {
            return freq;
        }
    };
    // Test variables
    private SparseQueryContext queryContext;
    private byte[] queryDenseVector;
    @Mock
    private SparseVector queryVector;
    @Mock
    private LeafReader leafReader;
    @Mock
    private Terms terms;
    @Mock
    private TermsEnum termsEnum;
    @Mock
    private SparseVectorReader reader;
    @Mock
    private SparsePostingsEnum postingsEnum1;
    @Mock
    private SparsePostingsEnum postingsEnum2;
    @Mock
    private SparseBinaryDocValuesPassThrough sparseBinaryDocValues;
    private SegmentInfo segmentInfo;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        // Initialize common test objects
        queryContext = constructSparseQueryContext(K, HEAP_FACTOR, TOKENS);
        queryDenseVector = new byte[] { 1, 2, 3, 4 };
        when(queryVector.toDenseVector()).thenReturn(queryDenseVector);
        // Mock LeafReaderContext
        when(leafReader.maxDoc()).thenReturn(100);
        // Mock Terms
        when(Terms.getTerms(leafReader, FIELD_NAME)).thenReturn(terms);
        // Mock TermsEnum
        when(terms.iterator()).thenReturn(termsEnum);
        when(leafReader.getBinaryDocValues(anyString())).thenReturn(sparseBinaryDocValues);
        segmentInfo = new SegmentInfo(
            new ByteBuffersDirectory(),
            Version.LATEST,
            Version.LATEST,
            "name",
            1,
            true,
            true,
            null,
            Map.of(),
            "1234567890123456".getBytes(StandardCharsets.UTF_8),
            Map.of(),
            null
        );
        when(sparseBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
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

        // Mock DocFreqIterator
        DocFreqIterator docIterator = mock(DocFreqIterator.class);
        when(cluster.getDisi()).thenReturn(docIterator);
        when(docIterator.nextDoc()).thenReturn(1).thenReturn(NO_MORE_DOCS);
        when(docIterator.docID()).thenReturn(1).thenReturn(NO_MORE_DOCS);

        // Mock SparseVectorReader
        SparseVectorReader reader = mock(SparseVectorReader.class);
        SparseVector docVector = mock(SparseVector.class);
        when(reader.read(1)).thenReturn(docVector);
        when(docVector.dotProduct(queryDenseVector)).thenReturn(5);

        // Create scorer
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            null,
            reader,
            simScorer
        );

        // Test iterator
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(5, scorer.score(), DELTA_FOR_ASSERTION);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testComplexHappyCase() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.seekExact(new BytesRef("token2"))).thenReturn(true);

        // Mock SparsePostingsEnum
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1).thenReturn(postingsEnum2);

        // Mock clusters in term1
        DocumentCluster cluster1_1 = prepareCluster(10, false);
        DocumentCluster cluster1_2 = prepareCluster(0, true);
        DocumentCluster cluster1_3 = prepareCluster(1, false);
        DocumentCluster cluster2_1 = prepareCluster(3, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator1 = mock(IteratorWrapper.class);
        IteratorWrapper<DocumentCluster> clusterIterator2 = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator1);
        when(postingsEnum2.clusterIterator()).thenReturn(clusterIterator2);
        when(clusterIterator1.next()).thenReturn(cluster1_1).thenReturn(cluster1_2).thenReturn(cluster1_3).thenReturn(null);
        when(clusterIterator2.next()).thenReturn(cluster2_1).thenReturn(null);

        prepareClusterAndItsDocs(cluster1_1, 1, 2, 2, 2); // first cluster will be examined
        prepareClusterAndItsDocs(cluster1_2, 3, 0); // second cluster should not be skipped
        prepareClusterAndItsDocs(cluster1_3, 4, 1); // third cluster will be skipped
        prepareClusterAndItsDocs(cluster2_1, 5, 1, 1, 10); // fourth cluster will be examined

        // Create scorer
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            null,
            reader,
            simScorer
        );

        // Test iterator - should skip the low score cluster and process the high score one
        DocIdSetIterator iterator = scorer.iterator();
        List<Integer> expectedDocIds = Arrays.asList(1, 2, 3, 5);
        List<Integer> actualDocIds = new ArrayList<>();
        int doc = iterator.nextDoc();
        while (iterator.docID() != NO_MORE_DOCS) {
            actualDocIds.add(doc);
            doc = iterator.nextDoc();
        }
        assertEquals(expectedDocIds, actualDocIds);
    }

    public void testDocumentFiltering() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);

        // Mock SparsePostingsEnum
        SparsePostingsEnum postingsEnum = mock(SparsePostingsEnum.class);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        // Mock DocFreqIterator - returns 3 docs: deleted, already visited, and valid
        DocFreqIterator docIterator = constructDocFreqIterator(Arrays.asList(10, 20, 30, 20), Arrays.asList(1, 2, 3, 4));
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
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            acceptedDocs,
            reader,
            simScorer
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
        DocumentCluster cluster1 = prepareCluster(10, false);
        DocumentCluster cluster2 = prepareCluster(9, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster1).thenReturn(cluster2).thenReturn(null);

        prepareClusterAndItsDocs(cluster1, 1, 5, 2, 10, 3, 15);
        prepareClusterAndItsDocs(cluster2, 2, 10, 3, 15);

        // Create scorer
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(FIELD_NAME, queryContext, queryVector,
            leafReader, null, reader, simScorer
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
        DocumentCluster cluster1 = prepareCluster(10, false);
        DocumentCluster cluster2 = prepareCluster(6, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster1).thenReturn(cluster2).thenReturn(null);

        prepareClusterAndItsDocs(cluster1, 1, 5, 2, 10, 3, 15);
        prepareClusterAndItsDocs(cluster2, 4, 1);

        // Create scorer
        SparseQueryContext context = SparseQueryContext.builder()
            .heapFactor(2.0f)
            .k(2)
            .tokens(TOKENS)
            .build();
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(FIELD_NAME, context, queryVector,
            leafReader, null, reader, simScorer
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

        // fourth doc (doc 4)
        assertEquals(4, iterator.nextDoc());
        assertEquals(1, scorer.score(), DELTA_FOR_ASSERTION);

        // No more docs
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testLargerK() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);

        // Mock SparsePostingsEnum
        SparsePostingsEnum postingsEnum = mock(SparsePostingsEnum.class);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum);
        // cluster 1 will always be examined, cluster2 will be not as its dp score won't surpass heap's lowest
        DocumentCluster cluster1 = prepareCluster(10, false);
        DocumentCluster cluster2 = prepareCluster(6, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster1).thenReturn(cluster2).thenReturn(null);

        prepareClusterAndItsDocs(cluster1, 1, 5, 2, 10, 3, 15);
        prepareClusterAndItsDocs(cluster2, 4, 1);

        // Create scorer
        SparseQueryContext context = SparseQueryContext.builder()
            .heapFactor(0.1f)
            .k(5)
            .tokens(TOKENS)
            .build();
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(FIELD_NAME, context, queryVector,
            leafReader, null, reader, simScorer
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

        // fourth doc (doc 4)
        assertEquals(4, iterator.nextDoc());
        assertEquals(1, scorer.score(), DELTA_FOR_ASSERTION);

        // No more docs
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testClusterShouldNotSkip() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);

        // Mock SparsePostingsEnum
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);
        // cluster 1 will always be examined, cluster2 will be not as its dp score won't surpass heap's lowest
        DocumentCluster cluster1 = prepareCluster(10, false);
        DocumentCluster cluster2 = prepareCluster(9, true);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster1).thenReturn(cluster2).thenReturn(null);

        prepareClusterAndItsDocs(cluster1, 1, 5, 2, 10);
        prepareClusterAndItsDocs(cluster2, 3, 15, 4, 9);

        // Create scorer
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(FIELD_NAME, queryContext, queryVector,
            leafReader, null, reader, simScorer
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

        // Fourth doc (doc 4)
        assertEquals(4, iterator.nextDoc());
        assertEquals(9, scorer.score(), DELTA_FOR_ASSERTION);

        // No more docs
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testNullSparseVectorReaderWithoutInMemoryReader() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);
        InMemoryKey.IndexKey indexKey = mock(InMemoryKey.IndexKey.class);
        when(postingsEnum1.getIndexKey()).thenReturn(indexKey);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        // mock vector & reader
        SparseVector vector = mock(SparseVector.class);
        when(sparseBinaryDocValues.read(1)).thenReturn(null);
        when(sparseBinaryDocValues.read(2)).thenReturn(vector);

        prepareClusterAndItsDocs(cluster, 1, 2, 2, 3);

        // Create scorer
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            null,
            null,
            simScorer
        );

        // Test iterator - should skip doc 1 (no vector) and return doc 2
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(2, iterator.nextDoc());
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testNullSparseVectorReaderWithInMemoryReader() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, FIELD_NAME);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        prepareClusterAndItsDocs(cluster, 1, 2, 2, 3);

        SparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.getOrCreate(indexKey, 10);
        SparseVector vector = mock(SparseVector.class);
        when(vector.dotProduct(queryDenseVector)).thenReturn(5);
        index.getWriter().write(1, vector);

        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            null,
            null,
            simScorer
        );

        // Test iterator - should skip doc 1 (no vector) and return doc 2
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testNullSparseVectorReaderWithBinaryDocValuesTypeMismatch() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);
        InMemoryKey.IndexKey indexKey = mock(InMemoryKey.IndexKey.class);
        when(postingsEnum1.getIndexKey()).thenReturn(indexKey);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        prepareClusterAndItsDocs(cluster, 1, 2, 2, 3);

        SparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.getOrCreate(indexKey, 10);
        SparseVector vector = mock(SparseVector.class);
        when(vector.dotProduct(queryDenseVector)).thenReturn(5);
        index.getWriter().write(1, vector);

        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(leafReader.getBinaryDocValues(anyString())).thenReturn(binaryDocValues);

        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            null,
            null,
            simScorer
        );

        // Test iterator - should skip doc 1 (no vector) and return doc 2
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testMissingVector() throws IOException {
        when(termsEnum.seekExact(new BytesRef("token1"))).thenReturn(true);
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum1);

        // Mock document cluster
        DocumentCluster cluster = prepareCluster(10, false);

        // Mock cluster iterator
        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(postingsEnum1.clusterIterator()).thenReturn(clusterIterator);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);

        // Mock DocFreqIterator with two docs - one with vector and one without
        DocFreqIterator docIterator = constructDocFreqIterator(1, 2);
        when(cluster.getDisi()).thenReturn(docIterator);
        // Doc 1 has no vector
        when(reader.read(1)).thenReturn(null);
        // Doc 2 has a vector
        SparseVector docVector = prepareVector(2, 5);

        // Create scorer
        PostingWithClustersScorer scorer = new PostingWithClustersScorer(
            FIELD_NAME,
            queryContext,
            queryVector,
            leafReader,
            null,
            reader,
            simScorer
        );

        // Test iterator - should skip doc 1 (no vector) and return doc 2
        DocIdSetIterator iterator = scorer.iterator();
        assertEquals(2, iterator.nextDoc());
        assertEquals(5, scorer.score(), DELTA_FOR_ASSERTION);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    private SparseQueryContext constructSparseQueryContext(int k, float hf, List<String> tokens) {
        return SparseQueryContext.builder().k(k).heapFactor(hf).tokens(tokens).build();
    }

    private DocumentCluster prepareCluster(int summaryDP, boolean shouldNotSkip) {
        // Mock document cluster
        DocumentCluster cluster = mock(DocumentCluster.class);
        SparseVector clusterSummary = mock(SparseVector.class);
        when(cluster.getSummary()).thenReturn(clusterSummary);
        when(clusterSummary.dotProduct(queryDenseVector)).thenReturn(summaryDP);
        when(cluster.isShouldNotSkip()).thenReturn(shouldNotSkip);
        return cluster;
    }

    private void prepareVectors(int... arguments) throws IOException {
        for (int i = 0; i < arguments.length; i += 2) {
            prepareVector(arguments[i], arguments[i + 1]);
        }
    }

    private SparseVector prepareVector(int docId, int dpScore) throws IOException {
        SparseVector docVector = mock(SparseVector.class);
        when(reader.read(docId)).thenReturn(docVector);
        when(docVector.dotProduct(queryDenseVector)).thenReturn(dpScore);
        return docVector;
    }

    private void prepareClusterAndItsDocs(DocumentCluster cluster, int... docScores) throws IOException {
        prepareVectors(docScores);
        List<Integer> docs = new ArrayList<>();
        for (int i = 0; i < docScores.length; i += 2) {
            docs.add(docScores[i]);
        }
        // Mock DocFreqIterator with two docs - one with vector and one without
        DocFreqIterator docIterator = constructDocFreqIterator(docs, docs);
        when(cluster.getDisi()).thenReturn(docIterator);
    }
}
