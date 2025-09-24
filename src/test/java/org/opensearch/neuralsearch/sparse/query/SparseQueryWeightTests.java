/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
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
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    @Mock
    private SparseVectorQuery sparseVectorQuery;
    @Mock
    private Query mockOriginalQuery;
    @Mock
    private LeafReaderContext leafReaderContext;
    @Mock
    private LeafReaderContext fallbackContext;
    @Mock
    private SegmentReader sparseSegmentReader;
    @Mock
    private SegmentReader nonSparseSegmentReader;
    @Mock
    private FieldInfo sparseFieldInfo;
    @Mock
    private FieldInfo nonSparseFieldInfo;
    @Mock
    private FieldInfos nonSparseFieldInfos;
    @Mock
    private FieldInfos sparseFieldInfos;
    @Mock
    private ForwardIndexCache mockForwardIndexCache;
    @Mock
    private ForwardIndexCacheItem mockForwardIndexCacheItem;

    private SparseVector queryVector;
    private SegmentInfo segmentInfo;
    private SegmentCommitInfo mockSegmentCommitInfo;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Create query vector
        queryVector = createVector(1, 1, 3, 2, 5, 3);
        when(sparseVectorQuery.getQueryVector()).thenReturn(queryVector);
        when(sparseVectorQuery.getQueryContext()).thenReturn(new SparseQueryContext(List.of("token1", "token2"), 1.0f, 5));

        // Create a mock original query that returns our mock weight
        when(mockOriginalQuery.createWeight(any(IndexSearcher.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);
        when(sparseVectorQuery.getFallbackQuery()).thenReturn(mockOriginalQuery);
        when(sparseVectorQuery.getFieldName()).thenReturn("name");

        // Mock LeafReaderContext and LeafReader
        mockSegmentCommitInfo = TestsPrepareUtils.prepareSegmentCommitInfo();
        when(sparseSegmentReader.getSegmentInfo()).thenReturn(mockSegmentCommitInfo);
        when(leafReaderContext.reader()).thenReturn(sparseSegmentReader);

        mockSegmentCommitInfo = TestsPrepareUtils.prepareSegmentCommitInfo();
        when(nonSparseSegmentReader.getSegmentInfo()).thenReturn(mockSegmentCommitInfo);
        when(fallbackContext.reader()).thenReturn(nonSparseSegmentReader);

        // Create FieldInfo with sparse attributes that can trigger SEISMIC
        when(sparseFieldInfo.getIndexOptions()).thenReturn(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        when(sparseFieldInfo.attributes()).thenReturn(prepareAttributes(true, 5, 1.0f, 10, 1.0f));
        when(sparseFieldInfos.fieldInfo(anyString())).thenReturn(sparseFieldInfo);
        when(sparseSegmentReader.getFieldInfos()).thenReturn(sparseFieldInfos);

        when(nonSparseFieldInfo.attributes()).thenReturn(prepareAttributes(false, 100, 1.0f, 10, 1.0f));
        when(nonSparseFieldInfo.getIndexOptions()).thenReturn(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        when(nonSparseFieldInfos.fieldInfo(anyString())).thenReturn(nonSparseFieldInfo);
        when(nonSparseSegmentReader.getFieldInfos()).thenReturn(nonSparseFieldInfos);

        when(mockForwardIndexCache.getOrCreate(any(), anyInt())).thenReturn(mockForwardIndexCacheItem);

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

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, 3.0f, 3.0f, mockForwardIndexCache);

        assertNotNull("Weight should not be null", weight);
        assertEquals("Weight should have the correct query", sparseVectorQuery, weight.getQuery());
    }

    public void testScorerSupplierWithSparseAlgorithm() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);

        assertEquals("Cost should be 0", 0, scorerSupplier.cost());
    }

    public void testScorerSupplierFallbackToBooleanQuery() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );

        ScorerSupplier scorerSupplier = weight.scorerSupplier(fallbackContext);
        assertNotNull(scorerSupplier);

        verify(mockBooleanQueryWeight).scorerSupplier(fallbackContext);

        Scorer scorer = scorerSupplier.get(0);
        assertNotNull(scorer);
        assertEquals(1, scorerSupplier.cost());
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
            .fallbackQuery(mockOriginalQueryForFilter)
            .filterResults(filterResults)
            .build();

        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);
    }

    public void testIsCacheable() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, 3.0f, 3.0f, mockForwardIndexCache);

        assertFalse("Weight should not be cacheable", weight.isCacheable(leafReaderContext));
    }

    public void testExplain() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, 3.0f, 3.0f, mockForwardIndexCache);

        assertNull("Explain should return null", weight.explain(leafReaderContext, 0));
    }

    public void testBulkScorerFunctionality() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull(scorerSupplier);

        BulkScorer bulkScorer = scorerSupplier.bulkScorer();
        assertNotNull(bulkScorer);

        assertEquals(0, scorerSupplier.cost());
        assertEquals(0, bulkScorer.cost());
    }

    public void testBulkScorerScoreMethod() throws Exception {
        // Mock terms to return empty iterator so OrderedPostingWithClustersScorer returns no docs
        when(sparseSegmentReader.terms(FIELD_NAME)).thenReturn(null);
        Scorer scorer = mock(Scorer.class);
        DocIdSetIterator iter = mock(DocIdSetIterator.class);
        when(scorer.iterator()).thenReturn(iter);
        when(iter.nextDoc()).thenReturn(1, 2, DocIdSetIterator.NO_MORE_DOCS);
        SparseQueryWeight weight = spy(new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, 3.0f, 3.0f, mockForwardIndexCache));
        doReturn(scorer).when(weight).selectScorer(any(), any(), any());

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        BulkScorer bulkScorer = scorerSupplier.bulkScorer();

        // Test the score method - should call setScorer but no collect since no terms
        int result = bulkScorer.score(mockLeafCollector, null, 0, 10);

        // Verify collector interactions
        verify(mockLeafCollector).setScorer(any(Scorer.class));
        verify(mockLeafCollector).collect(1);
        verify(mockLeafCollector).collect(2);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, result);
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
            .fallbackQuery(mockOriginalQuery)
            .filterResults(filterResults)
            .build();

        SparseQueryWeight weight = new SparseQueryWeight(
            queryWithFilter,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);

        assertNotNull("ScorerSupplier should not be null", scorerSupplier);
        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);
    }

    public void testSparseBinaryDocValuesPassThroughPath() throws Exception {
        // Mock SparseBinaryDocValuesPassThrough
        SparseBinaryDocValuesPassThrough mockDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseSegmentReader.getBinaryDocValues(FIELD_NAME)).thenReturn(mockDocValues);

        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);

        assertNotNull("ScorerSupplier should not be null", scorerSupplier);
        Scorer scorer = scorerSupplier.get(0);
        assertNotNull("Scorer should not be null", scorer);
    }

    public void test_selectScorer() throws IOException {
        SparseBinaryDocValuesPassThrough mockDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseSegmentReader.getBinaryDocValues(anyString())).thenReturn(mockDocValues);
        when(sparseVectorQuery.getFilterResults()).thenReturn(null);

        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );
        Scorer scorer = weight.selectScorer(sparseVectorQuery, leafReaderContext, segmentInfo);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
    }

    public void test_selectScorerWithFilter() throws IOException {
        SparseBinaryDocValuesPassThrough mockDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseSegmentReader.getBinaryDocValues(anyString())).thenReturn(mockDocValues);
        String id = "1";
        when(leafReaderContext.id()).thenReturn(id);
        BitSet bitSet = mock(BitSet.class);
        when(sparseVectorQuery.getFilterResults()).thenReturn(Map.of(id, bitSet));
        when(bitSet.cardinality()).thenReturn(2);

        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );
        Scorer scorer = weight.selectScorer(sparseVectorQuery, leafReaderContext, segmentInfo);
        assertTrue(scorer instanceof ExactMatchScorer);
    }

    public void test_selectScorer_IOException() throws IOException {
        doThrow(IOException.class).when(sparseSegmentReader).getBinaryDocValues(anyString());
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );
        expectThrows(IOException.class, () -> weight.selectScorer(sparseVectorQuery, leafReaderContext, segmentInfo));
    }

    public void test_selectScorer_segmentIsNull() throws IOException {
        SparseBinaryDocValuesPassThrough mockDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseSegmentReader.getBinaryDocValues(anyString())).thenReturn(mockDocValues);
        when(sparseVectorQuery.getFilterResults()).thenReturn(null);

        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            3.0f,
            3.0f,
            mockForwardIndexCache
        );
        Scorer scorer = weight.selectScorer(sparseVectorQuery, leafReaderContext, null);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
        verify(mockForwardIndexCache, never()).getOrCreate(any(), anyInt());
        assertSame(SparseVectorReader.NOOP_READER, ((OrderedPostingWithClustersScorer) scorer).getReader());
    }
}
