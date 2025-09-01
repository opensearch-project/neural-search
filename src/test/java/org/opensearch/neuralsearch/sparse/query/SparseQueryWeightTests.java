/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;

public class SparseQueryWeightTests extends AbstractSparseTestBase {

    private static final String FIELD_NAME = "test_field";

    @Mock
    private IndexSearcher mockSearcher;

    @Mock
    private SparseQueryContext mockQueryContext;

    @Mock
    private Weight mockBooleanQueryWeight;

    @Mock
    private Scorer mockScorer;

    @Mock
    private LeafCollector mockLeafCollector;

    private SparseVector queryVector;
    private SparseVectorQuery sparseVectorQuery;
    private LeafReaderContext leafReaderContext;
    private LeafReaderContext fallbackContext;
    private FieldInfo sparseFieldInfo;
    private FieldInfo nonSparseFieldInfo;
    private FieldInfos sparseFieldInfos;
    private FieldInfos nonSparseFieldInfos;
    private SegmentInfo segmentInfo;
    private SegmentReader sparseSegmentReader;
    private SegmentReader nonSparseSegmentReader;
    private SegmentCommitInfo mockSegmentCommitInfo;
    private IndexReader indexReader;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Create query vector
        queryVector = createVector(1, 1, 3, 2, 5, 3);

        // Create a mock original query that returns our mock weight
        Query mockOriginalQuery = mock(Query.class);
        when(mockOriginalQuery.createWeight(any(IndexSearcher.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        sparseVectorQuery = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(mockOriginalQuery)
            .build();

        // Set up mock searcher
        indexReader = TestsPrepareUtils.prepareTestIndexReader();
        when(mockSearcher.getIndexReader()).thenReturn(indexReader);

        // Mock LeafReaderContext and LeafReader
        leafReaderContext = mock(LeafReaderContext.class);
        sparseSegmentReader = mock(SegmentReader.class);
        mockSegmentCommitInfo = TestsPrepareUtils.prepareSegmentCommitInfo();
        when(Lucene.segmentReader(sparseSegmentReader).getSegmentInfo()).thenReturn(mockSegmentCommitInfo);
        when(leafReaderContext.reader()).thenReturn(sparseSegmentReader);

        fallbackContext = mock(LeafReaderContext.class);
        nonSparseSegmentReader = mock(SegmentReader.class);
        mockSegmentCommitInfo = TestsPrepareUtils.prepareSegmentCommitInfo();
        when(Lucene.segmentReader(nonSparseSegmentReader).getSegmentInfo()).thenReturn(mockSegmentCommitInfo);
        when(fallbackContext.reader()).thenReturn(nonSparseSegmentReader);

        // Create FieldInfo with sparse attributes that can trigger SEISMIC
        sparseFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        sparseFieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "5");
        sparseFieldInfos = new FieldInfos(new FieldInfo[] { sparseFieldInfo });
        when(sparseSegmentReader.getFieldInfos()).thenReturn(sparseFieldInfos);

        nonSparseFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        nonSparseFieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "100");
        nonSparseFieldInfos = new FieldInfos(new FieldInfo[] { nonSparseFieldInfo });
        when(nonSparseSegmentReader.getFieldInfos()).thenReturn(nonSparseFieldInfos);

        // Set up mock boolean query weight
        ScorerSupplier realScorerSupplier = new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
                return mockScorer;
            }

            @Override
            public long cost() {
                return 1;
            }
        };
        when(mockBooleanQueryWeight.scorerSupplier(any(LeafReaderContext.class))).thenReturn(realScorerSupplier);
        when(mockScorer.iterator()).thenReturn(DocIdSetIterator.all(1));

        // Mock the query context
        when(mockQueryContext.getK()).thenReturn(10);

        // Prepare SegmentInfo
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
    }

    public void testCreateWeight() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        assertNotNull("Weight should not be null", weight);
        assertEquals("Weight should have the correct query", sparseVectorQuery, weight.getQuery());
    }

    public void testScorerSupplierWithSparseAlgorithm() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);

        assertEquals("Cost should be 0", 0, scorerSupplier.cost());
    }

    public void testScorerSupplierFallbackToBooleanQuery() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        ScorerSupplier scorerSupplier = weight.scorerSupplier(fallbackContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        verify(mockBooleanQueryWeight).scorerSupplier(fallbackContext);

        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);

        assertEquals("Cost should be 1", 1, scorerSupplier.cost());
    }

    public void testScorerSupplierWithFilterResults() throws Exception {
        Map<Object, BitSet> filterResults = new HashMap<>();
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(0);
        bitSet.set(1);
        filterResults.put(leafReaderContext.id(), bitSet);

        when(mockQueryContext.getK()).thenReturn(1);

        Query mockOriginalQueryForFilter = mock(Query.class);
        when(mockOriginalQueryForFilter.createWeight(any(IndexSearcher.class), any(ScoreMode.class), anyFloat())).thenReturn(
            mockBooleanQueryWeight
        );

        sparseVectorQuery = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(mockOriginalQueryForFilter)
            .filterResults(filterResults)
            .build();

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);
    }

    public void testIsCacheable() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        assertFalse("Weight should not be cacheable", weight.isCacheable(leafReaderContext));
    }

    public void testExplain() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        assertNull("Explain should return null", weight.explain(leafReaderContext, 0));
    }

    public void testBulkScorerFunctionality() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        BulkScorer bulkScorer = scorerSupplier.bulkScorer();
        assertNotNull("BulkScorer should not be null", bulkScorer);

        assertEquals("ScorerSupplier cost should be 0", 0, scorerSupplier.cost());
        assertEquals("BulkScorer cost should be 0", 0, bulkScorer.cost());
    }

    public void testBulkScorerScoreMethod() throws Exception {
        // Mock terms to return empty iterator so OrderedPostingWithClustersScorer returns no docs
        when(sparseSegmentReader.terms(FIELD_NAME)).thenReturn(null);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        BulkScorer bulkScorer = scorerSupplier.bulkScorer();

        // Test the score method - should call setScorer but no collect since no terms
        int result = bulkScorer.score(mockLeafCollector, null, 0, 10);

        // Verify collector interactions
        verify(mockLeafCollector).setScorer(any(Scorer.class));
        assertEquals("Should return NO_MORE_DOCS", DocIdSetIterator.NO_MORE_DOCS, result);
    }

    public void testExactMatchScorerPath() throws Exception {
        // Create filter results with small cardinality to trigger ExactMatchScorer
        Map<Object, BitSet> filterResults = new HashMap<>();
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(0);
        filterResults.put(leafReaderContext.id(), bitSet);

        // Set K higher than cardinality to trigger exact match path
        when(mockQueryContext.getK()).thenReturn(5);

        Query mockOriginalQuery = mock(Query.class);
        when(mockOriginalQuery.createWeight(any(IndexSearcher.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseVectorQuery queryWithFilter = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(mockOriginalQuery)
            .filterResults(filterResults)
            .build();

        SparseQueryWeight weight = new SparseQueryWeight(queryWithFilter, mockSearcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);

        assertNotNull("ScorerSupplier should not be null", scorerSupplier);
        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);
    }

    public void testSparseBinaryDocValuesPassThroughPath() throws Exception {
        // Mock SparseBinaryDocValuesPassThrough
        SparseBinaryDocValuesPassThrough mockDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(sparseSegmentReader.getBinaryDocValues(FIELD_NAME)).thenReturn(mockDocValues);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);

        assertNotNull("ScorerSupplier should not be null", scorerSupplier);
        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);
    }
}
