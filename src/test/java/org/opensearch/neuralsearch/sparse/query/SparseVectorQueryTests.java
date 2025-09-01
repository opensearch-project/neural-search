/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

public class SparseVectorQueryTests extends AbstractSparseTestBase {

    private static final String FIELD_NAME = "test_field";
    private static final String FILTER_FIELD = "filter_field";

    @Mock
    private IndexSearcher mockSearcher;

    @Mock
    private SparseQueryContext mockQueryContext;

    @Mock
    private Weight mockRewrittenFilterWeight;

    @Mock
    private Scorer mockFilterWeightScorer;

    @Mock
    private ScorerSupplier mockScorerSupplier;

    @Mock
    private BitSet mockBitSet;

    private SparseVector queryVector;
    private List<LeafReaderContext> leaves;

    /**
     * Set up test environment
     * - Initialize mock objects
     * - Create query vector for testing
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        // Create query vector
        queryVector = createVector(1, 1, 3, 2, 5, 3);
        IndexReader indexReader = createTestIndexReader();
        leaves = indexReader.leaves();

        Executor executor = Executors.newSingleThreadExecutor();
        TaskExecutor taskExecutor = new TaskExecutor(executor);
        when(mockSearcher.getTaskExecutor()).thenReturn(taskExecutor);
        when(mockSearcher.getIndexReader()).thenReturn(indexReader);
        when(mockSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenReturn(mockRewrittenFilterWeight);

        // Create a BitSet and DocIdSetIterator for the filter
        BitSet bitSet = new FixedBitSet(13);// 0,2,3
        DocIdSetIterator iter = new BitSetIterator(bitSet, 0);
        when(mockFilterWeightScorer.iterator()).thenReturn(iter);

        // Use ScorerSupplier instead of Scorer directly
        when(mockRewrittenFilterWeight.scorer(any(LeafReaderContext.class))).thenReturn(mockFilterWeightScorer);
        when(mockRewrittenFilterWeight.scorerSupplier(any(LeafReaderContext.class))).thenReturn(mockScorerSupplier);
        when(mockScorerSupplier.get(anyLong())).thenReturn(mockFilterWeightScorer);
    }

    public void testCreateWeight() throws IOException {
        Query originalQuery = new MatchAllDocsQuery();

        SparseVectorQuery query = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(originalQuery)
            .build();

        Weight weight = query.createWeight(mockSearcher, ScoreMode.COMPLETE, 1.0f);

        assertNotNull("Weight should not be null", weight);
        assertTrue("Weight should be instance of SparseQueryWeight", weight instanceof SparseQueryWeight);

        SparseQueryWeight sparseWeight = (SparseQueryWeight) weight;
        assertEquals("SparseQueryWeight should have the correct query", query, sparseWeight.getQuery());
    }

    public void testRewriteWithoutFilter() throws IOException {
        Query originalQuery = new MatchAllDocsQuery();

        SparseVectorQuery query = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(originalQuery)
            .build();

        Query rewritten = query.rewrite(mockSearcher);

        // Without filter, the query should return itself
        assertSame("Rewritten query should be the same instance", query, rewritten);
        assertNull("Filter results should be null", query.getFilterResults());
    }

    public void testRewriteWithFilter() throws IOException {
        Query originalQuery = new MatchAllDocsQuery();
        Query filter = new TermQuery(new Term(FILTER_FIELD, "even"));
        when(mockSearcher.rewrite(any(Query.class))).thenReturn(filter);
        SparseVectorQuery query = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(originalQuery)
            .filter(filter)
            .build();

        Query rewritten = query.rewrite(mockSearcher);

        // With filter, the query should return itself but with filterResults populated
        assertSame("Rewritten query should be the same instance", query, rewritten);
        assertNotNull("Filter results should not be null", query.getFilterResults());

        // Check that filter results contain entries for each segment
        Map<Object, BitSet> filterResults = query.getFilterResults();
        assertEquals("Filter results should have entries for all segments", leaves.size(), filterResults.size());
    }

    public void testRewriteWithFilterWeightScorerNull() throws IOException {
        Query originalQuery = new MatchAllDocsQuery();
        Query filter = new TermQuery(new Term(FILTER_FIELD, "even"));
        when(mockSearcher.rewrite(any(Query.class))).thenReturn(filter);
        when(mockScorerSupplier.get(anyLong())).thenReturn(null);
        when(mockRewrittenFilterWeight.scorer(any(LeafReaderContext.class))).thenReturn(null);
        SparseVectorQuery query = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(originalQuery)
            .filter(filter)
            .build();

        Query rewritten = query.rewrite(mockSearcher);

        // With filter, the query should return itself but with filterResults populated
        assertSame("Rewritten query should be the same instance", query, rewritten);
        assertNotNull("Filter results should not be null", query.getFilterResults());

        // Check that filter results contain entries for each segment
        Map<Object, BitSet> filterResults = query.getFilterResults();
        assertEquals("Filter results should have be empty", 0, filterResults.size());
    }

    public void testRewriteWithComplexFilter() throws IOException {
        Query originalQuery = new MatchAllDocsQuery();

        // Create a boolean query as filter
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term(FILTER_FIELD, "even")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term(FIELD_NAME, "value0")), BooleanClause.Occur.SHOULD);
        Query filter = builder.build();
        when(mockSearcher.rewrite(any(Query.class))).thenReturn(filter);
        SparseVectorQuery query = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(originalQuery)
            .filter(filter)
            .build();

        Query rewritten = query.rewrite(mockSearcher);

        // With filter, the query should return itself but with filterResults populated
        assertSame("Rewritten query should be the same instance", query, rewritten);
        assertNotNull("Filter results should not be null", query.getFilterResults());
    }

    public void testGetters() {
        Query originalQuery = new MatchAllDocsQuery();
        Query filter = new TermQuery(new Term(FILTER_FIELD, "even"));

        SparseVectorQuery query = SparseVectorQuery.builder()
            .queryVector(queryVector)
            .queryContext(mockQueryContext)
            .fieldName(FIELD_NAME)
            .originalQuery(originalQuery)
            .filter(filter)
            .build();

        assertEquals("Query vector should match", queryVector, query.getQueryVector());
        assertEquals("Query context should match", mockQueryContext, query.getQueryContext());
        assertEquals("Field name should match", FIELD_NAME, query.getFieldName());
        assertEquals("Original query should match", originalQuery, query.getOriginalQuery());
        assertEquals("Filter should match", filter, query.getFilter());
    }

    private IndexReader createTestIndexReader() throws IOException {
        ByteBuffersDirectory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new MockAnalyzer(random())));
        writer.addDocument(new Document());
        writer.close();
        return DirectoryReader.open(directory);
    }
}
