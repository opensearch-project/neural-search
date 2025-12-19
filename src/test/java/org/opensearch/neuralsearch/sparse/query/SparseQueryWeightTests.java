/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.Mockito.when;

public class SparseQueryWeightTests extends AbstractSparseTestBase {

    @Mock
    private IndexSearcher mockSearcher;
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
    private ForwardIndexCache mockForwardIndexCache;
    @Mock
    private ScorerSelector scorerSelector;
    @Mock
    private Query phaseOneQuery;
    @Mock
    private Query phaseTwoQuery;
    @Mock
    private Weight phaseOneWeight;
    @Mock
    private Weight phaseTwoWeight;

    private SparseVector queryVector;

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
        when(sparseVectorQuery.getRankFeaturesPhaseOneQuery()).thenReturn(phaseOneQuery);
        when(sparseVectorQuery.getRankFeaturesPhaseTwoQuery()).thenReturn(phaseTwoQuery);
        when(phaseOneQuery.createWeight(any(), any(), anyFloat())).thenReturn(phaseOneWeight);
        when(phaseTwoQuery.createWeight(any(), any(), anyFloat())).thenReturn(phaseTwoWeight);

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
        when(scorerSelector.select(any(), any(), any())).thenReturn(realScorerSupplier);
        when(mockBooleanQueryWeight.scorerSupplier(any(LeafReaderContext.class))).thenReturn(realScorerSupplier);
        when(mockScorer.iterator()).thenReturn(DocIdSetIterator.all(1));
    }

    public void testCreateWeight_happyCase() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, mockForwardIndexCache, scorerSelector);

        assertNotNull(weight);
        assertEquals(sparseVectorQuery, weight.getQuery());
        assertEquals(phaseOneWeight, weight.getRankFeaturePhaseOneWeight());
        assertEquals(phaseTwoWeight, weight.getRankFeaturePhaseTwoWeight());
    }

    public void testCreateWeight_nullTwoPhaseQuery() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);
        when(sparseVectorQuery.getRankFeaturesPhaseOneQuery()).thenReturn(null);
        when(sparseVectorQuery.getRankFeaturesPhaseTwoQuery()).thenReturn(null);
        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, mockForwardIndexCache, scorerSelector);

        assertNotNull(weight);
        assertEquals(sparseVectorQuery, weight.getQuery());
        assertEquals(null, weight.getRankFeaturePhaseOneWeight());
        assertEquals(null, weight.getRankFeaturePhaseTwoWeight());
    }

    public void testCreateWeight_nullScorerSelector() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);
        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, mockForwardIndexCache, null);

        assertNotNull(weight);
        assertNotNull(weight.getSelector());
        assertEquals(weight, weight.getSelector().getSparseQueryWeight());
    }

    public void testScorerSupplier() throws Exception {
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            mockForwardIndexCache,
            scorerSelector
        );

        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        assertNotNull("ScorerSupplier should not be null", scorerSupplier);

        Scorer scorer = scorerSupplier.get(0);
        assertEquals(mockScorer, scorer);
    }

    public void testIsCacheable() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, mockForwardIndexCache, scorerSelector);

        assertFalse("Weight should not be cacheable", weight.isCacheable(leafReaderContext));
    }

    public void testExplain() throws IOException {
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockBooleanQueryWeight);

        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, mockForwardIndexCache, scorerSelector);

        assertNull("Explain should return null", weight.explain(leafReaderContext, 0));
    }

    public void testGetFilterBitIterator_nullFilterResults() throws IOException {
        when(sparseVectorQuery.getFilterResults()).thenReturn(null);
        SparseQueryWeight weight = new SparseQueryWeight(sparseVectorQuery, mockSearcher, ScoreMode.COMPLETE, 1.0f, mockForwardIndexCache, scorerSelector);

        BitSetIterator result = weight.getFilterBitIterator(sparseVectorQuery, leafReaderContext);

        assertNull(result);
    }

    public void testGetFilterBitIterator_nullFilterForContext() throws IOException {
        Map<Object, BitSet> filterResults = new HashMap<>();
        when(sparseVectorQuery.getFilterResults()).thenReturn(filterResults);
        when(leafReaderContext.id()).thenReturn(0);
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            mockForwardIndexCache,
            scorerSelector
        );

        BitSetIterator result = weight.getFilterBitIterator(sparseVectorQuery, leafReaderContext);

        assertNull(result);
    }

    public void testGetFilterBitIterator_validFilter() throws IOException {
        FixedBitSet bitSet = new FixedBitSet(10);
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(5);
        Map<Object, BitSet> filterResults = new HashMap<>();
        filterResults.put(0, bitSet);
        when(sparseVectorQuery.getFilterResults()).thenReturn(filterResults);
        when(leafReaderContext.id()).thenReturn(0);
        SparseQueryWeight weight = new SparseQueryWeight(
            sparseVectorQuery,
            mockSearcher,
            ScoreMode.COMPLETE,
            1.0f,
            mockForwardIndexCache,
            scorerSelector
        );

        BitSetIterator result = weight.getFilterBitIterator(sparseVectorQuery, leafReaderContext);

        assertNotNull(result);
        assertEquals(3, result.cost());
    }
}
