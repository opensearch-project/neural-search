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
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScorerSelectorTests extends AbstractSparseTestBase {
    @Mock
    private SparseQueryContext mockQueryContext;
    @Mock
    private Weight mockBooleanQueryWeight;
    @Mock
    private Scorer mockScorer;
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
    @Mock
    private SparseQueryWeight weight;

    private SparseVector queryVector;
    private SegmentInfo segmentInfo;
    private SegmentCommitInfo mockSegmentCommitInfo;
    private ScorerSelector scorerSelector;

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
        when(sparseVectorQuery.getSparseQueryTwoPhaseInfo()).thenReturn(null);

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
        when(weight.getFallbackQueryWeight()).thenReturn(mockBooleanQueryWeight);
        when(weight.getBoost()).thenReturn(1.0f);
        when(weight.getForwardIndexCache()).thenReturn(mockForwardIndexCache);
        when(weight.getRankFeaturePhaseOneWeight()).thenReturn(null);
        when(weight.getRankFeaturePhaseTwoWeight()).thenReturn(null);
        scorerSelector = new ScorerSelector(weight);
    }

    @SneakyThrows
    public void testSelectFallbackWhenNotSparseField() {
        ScorerSupplier result = scorerSelector.select(fallbackContext, sparseVectorQuery, null);
        assertNotNull(result);
        assertEquals(mockScorer, result.get(0));
        verify(mockBooleanQueryWeight).scorerSupplier(eq(fallbackContext));
    }

    @SneakyThrows
    public void testSelectTwoPhaseWhenTwoPhaseInfoPresent() {
        SparseQueryTwoPhaseInfo twoPhaseInfo = SparseQueryTwoPhaseInfo.builder().expansionRatio(5.0f).build();
        when(sparseVectorQuery.getSparseQueryTwoPhaseInfo()).thenReturn(twoPhaseInfo);

        Weight phaseOneWeight = mock(Weight.class);
        Weight phaseTwoWeight = mock(Weight.class);
        when(weight.getRankFeaturePhaseOneWeight()).thenReturn(phaseOneWeight);
        when(weight.getRankFeaturePhaseTwoWeight()).thenReturn(phaseTwoWeight);

        ScorerSupplier phaseOneScorerSupplier = createMockScorerSupplier();
        ScorerSupplier phaseTwoScorerSupplier = createMockScorerSupplier();
        when(phaseOneWeight.scorerSupplier(any())).thenReturn(phaseOneScorerSupplier);
        when(phaseTwoWeight.scorerSupplier(any())).thenReturn(phaseTwoScorerSupplier);

        ScorerSupplier result = scorerSelector.select(fallbackContext, sparseVectorQuery, null);
        assertNotNull(result);
        assertTrue(result instanceof TwoPhaseScorerSupplier);
    }

    @SneakyThrows
    public void testSelectTwoPhaseWhenPhaseOneScorerSupplierNull() {
        SparseQueryTwoPhaseInfo twoPhaseInfo = SparseQueryTwoPhaseInfo.builder().expansionRatio(5.0f).build();
        when(sparseVectorQuery.getSparseQueryTwoPhaseInfo()).thenReturn(twoPhaseInfo);

        Weight phaseOneWeight = mock(Weight.class);
        Weight phaseTwoWeight = mock(Weight.class);
        when(weight.getRankFeaturePhaseOneWeight()).thenReturn(phaseOneWeight);
        when(weight.getRankFeaturePhaseTwoWeight()).thenReturn(phaseTwoWeight);

        when(phaseOneWeight.scorerSupplier(any())).thenReturn(null);
        when(phaseTwoWeight.scorerSupplier(any())).thenReturn(createMockScorerSupplier());

        ScorerSupplier result = scorerSelector.select(fallbackContext, sparseVectorQuery, null);
        assertNotNull(result);
        assertTrue(result instanceof TwoPhaseScorerSupplier);
    }

    @SneakyThrows
    public void testSelectTwoPhaseWhenPhaseTwoScorerSupplierNull() {
        SparseQueryTwoPhaseInfo twoPhaseInfo = SparseQueryTwoPhaseInfo.builder().expansionRatio(5.0f).build();
        when(sparseVectorQuery.getSparseQueryTwoPhaseInfo()).thenReturn(twoPhaseInfo);

        Weight phaseOneWeight = mock(Weight.class);
        Weight phaseTwoWeight = mock(Weight.class);
        when(weight.getRankFeaturePhaseOneWeight()).thenReturn(phaseOneWeight);
        when(weight.getRankFeaturePhaseTwoWeight()).thenReturn(phaseTwoWeight);

        when(phaseOneWeight.scorerSupplier(any())).thenReturn(createMockScorerSupplier());
        when(phaseTwoWeight.scorerSupplier(any())).thenReturn(null);

        ScorerSupplier result = scorerSelector.select(fallbackContext, sparseVectorQuery, null);
        assertNotNull(result);
        assertTrue(result instanceof TwoPhaseScorerSupplier);
    }

    @SneakyThrows
    public void testSelectFallbackWhenRankFeaturePhaseOneWeightNull() {
        SparseQueryTwoPhaseInfo twoPhaseInfo = SparseQueryTwoPhaseInfo.builder().expansionRatio(5.0f).build();
        when(sparseVectorQuery.getSparseQueryTwoPhaseInfo()).thenReturn(twoPhaseInfo);

        when(weight.getRankFeaturePhaseOneWeight()).thenReturn(null);
        when(weight.getRankFeaturePhaseTwoWeight()).thenReturn(mock(Weight.class));

        ScorerSupplier result = scorerSelector.select(fallbackContext, sparseVectorQuery, null);
        assertNotNull(result);
        assertEquals(mockScorer, result.get(0));
    }

    @SneakyThrows
    public void testSelectFallbackWhenRankFeaturePhaseTwoWeightNull() {
        SparseQueryTwoPhaseInfo twoPhaseInfo = SparseQueryTwoPhaseInfo.builder().expansionRatio(5.0f).build();
        when(sparseVectorQuery.getSparseQueryTwoPhaseInfo()).thenReturn(twoPhaseInfo);

        when(weight.getRankFeaturePhaseOneWeight()).thenReturn(mock(Weight.class));
        when(weight.getRankFeaturePhaseTwoWeight()).thenReturn(null);

        ScorerSupplier result = scorerSelector.select(fallbackContext, sparseVectorQuery, null);
        assertNotNull(result);
        assertEquals(mockScorer, result.get(0));
    }

    @SneakyThrows
    public void testSelectSeismicScorerWhenSparseField() {
        ScorerSupplier result = scorerSelector.select(leafReaderContext, sparseVectorQuery, null);
        assertNotNull(result);
        Scorer scorer = result.get(0);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
    }

    @SneakyThrows
    public void testSelectSeismicScorerReturnsOrderedPostingScorer() {
        Scorer scorer = scorerSelector.selectSeismicScorer(sparseVectorQuery, leafReaderContext, segmentInfo, null);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
    }

    @SneakyThrows
    public void testSelectSeismicScorerReturnsExactMatchScorerWhenFilterSmall() {
        when(sparseVectorQuery.getQueryContext()).thenReturn(new SparseQueryContext(List.of("token1"), 1.0f, 10));
        BitSetIterator filterIterator = createBitSetIterator(3);
        Scorer scorer = scorerSelector.selectSeismicScorer(sparseVectorQuery, leafReaderContext, segmentInfo, filterIterator);
        assertTrue(scorer instanceof ExactMatchScorer);
    }

    @SneakyThrows
    public void testSelectSeismicScorerReturnsOrderedPostingScorerWhenFilterLarge() {
        when(sparseVectorQuery.getQueryContext()).thenReturn(new SparseQueryContext(List.of("token1"), 1.0f, 5));
        BitSetIterator filterIterator = createBitSetIterator(10);
        Scorer scorer = scorerSelector.selectSeismicScorer(sparseVectorQuery, leafReaderContext, segmentInfo, filterIterator);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
    }

    @SneakyThrows
    public void testSelectSeismicScorerWhenSegmentInfoNull() {
        Scorer scorer = scorerSelector.selectSeismicScorer(sparseVectorQuery, leafReaderContext, null, null);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
    }

    @SneakyThrows
    public void testSelectSeismicScorerWithSparseBinaryDocValues() {
        SparseBinaryDocValuesPassThrough mockDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseSegmentReader.getBinaryDocValues(anyString())).thenReturn(mockDocValues);

        Scorer scorer = scorerSelector.selectSeismicScorer(sparseVectorQuery, leafReaderContext, segmentInfo, null);
        assertTrue(scorer instanceof OrderedPostingWithClustersScorer);
    }

    private BitSetIterator createBitSetIterator(int cardinality) {
        FixedBitSet bitSet = new FixedBitSet(100);
        for (int i = 0; i < cardinality; i++) {
            bitSet.set(i);
        }
        return new BitSetIterator(bitSet, cardinality);
    }

    private ScorerSupplier createMockScorerSupplier() {
        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
                return mockScorer;
            }

            @Override
            public long cost() {
                return 1;
            }
        };
    }

    @SneakyThrows
    public void testGeneralScorerSupplierGet() {
        ScorerSelector.GeneralScorerSupplier supplier = new ScorerSelector.GeneralScorerSupplier(mockScorer);
        assertEquals(mockScorer, supplier.get(0));
        assertEquals(0, supplier.cost());
    }

    @SneakyThrows
    public void testGeneralScorerSupplierBulkScorer() {
        when(mockScorer.iterator()).thenReturn(DocIdSetIterator.all(3));
        ScorerSelector.GeneralScorerSupplier supplier = new ScorerSelector.GeneralScorerSupplier(mockScorer);
        BulkScorer bulkScorer = supplier.bulkScorer();

        assertNotNull(bulkScorer);
        assertEquals(0, bulkScorer.cost());

        java.util.List<Integer> collected = new java.util.ArrayList<>();
        LeafCollector collector = new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {
                collected.add(doc);
            }
        };
        int result = bulkScorer.score(collector, null, 0, Integer.MAX_VALUE);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, result);
        assertEquals(3, collected.size());
    }

    @SneakyThrows
    public void testEmptyScorer() {
        ScorerSelector.EmptyScorer emptyScorer = new ScorerSelector.EmptyScorer();

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, emptyScorer.docID());
        assertEquals(0, emptyScorer.score(), 0.0f);
        assertEquals(0, emptyScorer.getMaxScore(Integer.MAX_VALUE), 0.0f);

        DocIdSetIterator iterator = emptyScorer.iterator();
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(10));
        assertEquals(0, iterator.cost());
    }
}
