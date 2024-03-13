/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import lombok.SneakyThrows;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.common.lucene.search.FilteredCollector;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryWeight;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.HybridTopScoreDocCollector;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;

public class HybridCollectorManagerTests extends OpenSearchQueryTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String QUERY1 = "hello";
    private static final float DELTA_FOR_ASSERTION = 0.001f;

    @SneakyThrows
    public void testNewCollector_whenNotConcurrentSearch_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)));

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager.HybridCollectorNonConcurrentManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof HybridTopScoreDocCollector);

        Collector secondCollector = hybridCollectorManager.newCollector();
        assertSame(collector, secondCollector);
    }

    @SneakyThrows
    public void testNewCollector_whenConcurrentSearch_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)));

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager.HybridCollectorConcurrentSearchManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof HybridTopScoreDocCollector);

        Collector secondCollector = hybridCollectorManager.newCollector();
        assertNotSame(collector, secondCollector);
    }

    @SneakyThrows
    public void testPostFilter_whenNotConcurrentSearch_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)));

        QueryBuilder postFilterQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, "world");
        ParsedQuery parsedQuery = new ParsedQuery(postFilterQuery.toQuery(mockQueryShardContext));
        searchContext.parsedQuery(parsedQuery);

        Query pfQuery = postFilterQuery.toQuery(mockQueryShardContext);
        when(searchContext.parsedPostFilter()).thenReturn(parsedQuery);

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        when(indexSearcher.rewrite(pfQuery)).thenReturn(pfQuery);
        Weight weight = mock(Weight.class);
        when(indexSearcher.createWeight(pfQuery, ScoreMode.COMPLETE_NO_SCORES, 1f)).thenReturn(weight);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager.HybridCollectorNonConcurrentManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof FilteredCollector);

        Collector secondCollector = hybridCollectorManager.newCollector();
        assertSame(collector, secondCollector);
    }

    @SneakyThrows
    public void testPostFilter_whenConcurrentSearch_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)));

        QueryBuilder postFilterQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, "world");
        ParsedQuery parsedQuery = new ParsedQuery(postFilterQuery.toQuery(mockQueryShardContext));
        searchContext.parsedQuery(parsedQuery);

        Query pfQuery = postFilterQuery.toQuery(mockQueryShardContext);
        when(searchContext.parsedPostFilter()).thenReturn(parsedQuery);

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        when(indexSearcher.rewrite(pfQuery)).thenReturn(pfQuery);
        Weight weight = mock(Weight.class);
        when(indexSearcher.createWeight(pfQuery, ScoreMode.COMPLETE_NO_SCORES, 1f)).thenReturn(weight);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager.HybridCollectorConcurrentSearchManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof FilteredCollector);

        Collector secondCollector = hybridCollectorManager.newCollector();
        assertNotSame(collector, secondCollector);
    }

    @SneakyThrows
    public void testReduce_whenMatchedDocs_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1).toQuery(mockQueryShardContext))
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
        when(indexReader.numDocs()).thenReturn(3);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        when(searchContext.size()).thenReturn(1);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, TEST_DOC_TEXT3, ft));
        w.flush();
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext);
        HybridTopScoreDocCollector collector = (HybridTopScoreDocCollector) hybridCollectorManager.newCollector();

        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        collector.setWeight(weight);
        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);
        BulkScorer scorer = weight.bulkScorer(leafReaderContext);
        scorer.score(leafCollector, leafReaderContext.reader().getLiveDocs());
        leafCollector.finish();

        Object results = hybridCollectorManager.reduce(List.of());

        assertNotNull(results);
        ReduceableSearchResult reduceableSearchResult = ((ReduceableSearchResult) results);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        reduceableSearchResult.reduce(querySearchResult);
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();

        assertNotNull(topDocsAndMaxScore);
        assertEquals(1, topDocsAndMaxScore.topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation);
        float maxScore = topDocsAndMaxScore.maxScore;
        assertTrue(maxScore > 0);
        ScoreDoc[] scoreDocs = topDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(4, scoreDocs.length);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, DELTA_FOR_ASSERTION);
        assertEquals(maxScore, scoreDocs[2].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[3].score, DELTA_FOR_ASSERTION);

        w.close();
        reader.close();
        directory.close();
    }
}
