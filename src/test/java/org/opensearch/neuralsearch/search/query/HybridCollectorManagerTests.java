/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import lombok.SneakyThrows;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryContext;
import org.opensearch.neuralsearch.query.HybridQueryWeight;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopDocsCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopScoreDocCollector;
import org.opensearch.neuralsearch.search.collector.PagingFieldCollector;
import org.opensearch.neuralsearch.search.collector.SimpleFieldCollector;
import org.opensearch.neuralsearch.search.query.exception.HybridSearchRescoreQueryException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_DELIMITER;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAGIC_NUMBER_START_STOP;

import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.Rescorer;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.sort.SortAndFormats;

public class HybridCollectorManagerTests extends OpenSearchQueryTestCase {

    private static final String TEXT_FIELD_NAME = "field";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String QUERY1 = "hello";
    private static final String SCORE_ASCENDING_NOT_SUPPORTED_MESSAGE =
        "Sorting a hybrid query by _score in ascending order is not supported; hybrid results are ranked by descending "
            + "relevance. Use \"order\": \"desc\" for _score, or sort by a field.";
    private static final String SCORE_MORE_THAN_ONCE_NOT_SUPPORTED_MESSAGE =
        "Sorting a hybrid query by _score more than once is not supported; use a single \"_score\" sort criterion.";
    private static final String QUERY2 = "hi";
    protected static final String QUERY3 = "everyone";

    private Directory directory;
    private IndexWriter writer;
    private IndexReader indexReader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexObjects indexObjects = createIndexObjects(3);
        directory = indexObjects.directory();
        writer = indexObjects.writer();
        indexReader = indexObjects.indexReader();
    }

    @SneakyThrows
    public void testNewCollector_whenNotConcurrentSearch_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof HybridTopScoreDocCollector);
    }

    @SneakyThrows
    public void testNewCollector_whenConcurrentSearch_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof HybridTopScoreDocCollector);

        Collector secondCollector = hybridCollectorManager.newCollector();
        assertNotSame(collector, secondCollector);
    }

    @SneakyThrows
    public void testReduce_whenMatchedDocs_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1).toQuery(mockQueryShardContext)),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        when(searchContext.size()).thenReturn(1);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
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
        IndexSearcher searcher = new IndexSearcher(reader);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);
        HybridTopScoreDocCollector collector = (HybridTopScoreDocCollector) hybridCollectorManager.newCollector();

        QueryBuilder postFilterQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);

        Query pfQuery = postFilterQuery.toQuery(mockQueryShardContext);
        ParsedQuery parsedQuery = new ParsedQuery(pfQuery);
        searchContext.parsedQuery(parsedQuery);
        when(searchContext.parsedPostFilter()).thenReturn(parsedQuery);
        when(indexSearcher.rewrite(pfQuery)).thenReturn(pfQuery);
        Weight postFilterWeight = mock(Weight.class);
        when(indexSearcher.createWeight(pfQuery, ScoreMode.COMPLETE_NO_SCORES, 1f)).thenReturn(postFilterWeight);

        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        collector.setWeight(weight);
        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);
        BulkScorer scorer = weight.bulkScorer(leafReaderContext);
        scorer.score(leafCollector, leafReaderContext.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        leafCollector.finish();

        Object results = hybridCollectorManager.reduce(List.of(collector));

        assertNotNull(results);
        ReduceableSearchResult reduceableSearchResult = ((ReduceableSearchResult) results);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        reduceableSearchResult.reduce(querySearchResult);
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();

        assertNotNull(topDocsAndMaxScore);
        assertEquals(1, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation());
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

    @SneakyThrows
    public void testNewCollector_whenNotConcurrentSearchAndSortingIsApplied_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        SortField sortField = new SortField("_doc", SortField.Type.DOC);
        Sort sort = new Sort(sortField);
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        when(searchContext.sort()).thenReturn(new SortAndFormats(sort, docValueFormat));
        when(searchContext.minimumScore()).thenReturn(null);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof SimpleFieldCollector);
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenScoreAscendingWithFieldAndCollapse_thenFail() {
        // [_score asc, <field>] + collapse must be rejected. The collapse relaxation allows _score to be mixed
        // with a field only when _score is the DESCENDING primary key; ascending _score with a tiebreaker is out
        // of scope. _score asc maps to SortField reverse=true.
        SearchContext searchContext = mock(SearchContext.class);
        SortField scoreAscending = new SortField(null, SortField.Type.SCORE, true); // _score asc == reverse=true
        SortField fieldSort = new SortField("price", SortField.Type.INT);
        Sort sort = new Sort(scoreAscending, fieldSort);
        DocValueFormat[] docValueFormat = new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW };
        when(searchContext.sort()).thenReturn(new SortAndFormats(sort, docValueFormat));
        when(searchContext.minimumScore()).thenReturn(null);

        // Collapse present + score primary would otherwise pass the relaxation; the ascending order is what fails it.
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldName()).thenReturn(TEXT_FIELD_NAME);
        when(searchContext.collapse()).thenReturn(collapseContext);

        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(TEXT_FIELD_NAME)).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery)
        );
        assertEquals(
            "_score must be sorted descending when combined with a field in a hybrid query with collapse",
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenScoreDescendingWithFieldAndCollapse_thenCollapseCollectorCreated() {
        SearchContext searchContext = mockSearchContextForSortValidation(
            new Sort(SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT)),
            true
        );

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query());
        Collector collector = hybridCollectorManager.newCollector();
        assertTrue(collector instanceof HybridCollapsingTopDocsCollector);
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenScoreDescendingWithFieldWithoutCollapse_thenFail() {
        SearchContext searchContext = mockSearchContextForSortValidation(
            new Sort(SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT)),
            false
        );

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query())
        );
        assertEquals(
            "_score sort criteria cannot be applied with any other criteria unless the query uses collapse with _score as the "
                + "primary, descending key. Please select one sort criteria out of them.",
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenFieldThenScoreWithCollapse_thenFail() {
        SearchContext searchContext = mockSearchContextForSortValidation(
            new Sort(new SortField("price", SortField.Type.INT), SortField.FIELD_SCORE),
            true
        );

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query())
        );
        assertEquals(
            "_score must be the primary sort key when combined with a field in a hybrid query with collapse",
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenSortByScoreAscending_thenFail() {
        // "_score": {"order": "asc"} parses to a SCORE SortField with reverse == true
        SearchContext searchContext = mockSearchContextForSortValidation(new Sort(new SortField(null, SortField.Type.SCORE, true)), false);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query())
        );
        assertEquals(SCORE_ASCENDING_NOT_SUPPORTED_MESSAGE, illegalArgumentException.getMessage());
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenSortByScoreAscendingWithTrackScores_thenFailWithAscendingMessage() {
        // the ascending check runs before the track_scores check so the user gets the actionable message
        SearchContext searchContext = mockSearchContextForSortValidation(new Sort(new SortField(null, SortField.Type.SCORE, true)), false);
        when(searchContext.trackScores()).thenReturn(true);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query())
        );
        assertEquals(SCORE_ASCENDING_NOT_SUPPORTED_MESSAGE, illegalArgumentException.getMessage());
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenSortByScoreDescending_thenSuccessful() {
        // an explicit non-reversed _score sort keeps working (core normally drops it before the collector manager runs)
        SearchContext searchContext = mockSearchContextForSortValidation(new Sort(SortField.FIELD_SCORE), false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query());
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenSortByScoreAscendingWithCollapse_thenCollapseCollectorCreated() {
        // with collapse, ascending _score is a supported shape: the collapse collector keeps each sub-query's lowest-scoring window
        SearchContext searchContext = mockSearchContextForSortValidation(new Sort(new SortField(null, SortField.Type.SCORE, true)), true);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query());
        Collector collector = hybridCollectorManager.newCollector();
        assertTrue(collector instanceof HybridCollapsingTopDocsCollector);
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenSortByScoreMoreThanOnce_thenFail() {
        // core keeps every sort field, so sort: [{"_score": "asc"}, {"_score": "desc"}] reaches the collector manager as two SCORE fields
        List<Sort> sorts = List.of(
            new Sort(new SortField(null, SortField.Type.SCORE, true), SortField.FIELD_SCORE),
            new Sort(SortField.FIELD_SCORE, new SortField(null, SortField.Type.SCORE, true)),
            new Sort(SortField.FIELD_SCORE, SortField.FIELD_SCORE)
        );
        for (Sort sort : sorts) {
            for (boolean withCollapse : new boolean[] { false, true }) {
                SearchContext searchContext = mockSearchContextForSortValidation(sort, withCollapse);

                IllegalArgumentException illegalArgumentException = assertThrows(
                    IllegalArgumentException.class,
                    () -> HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query())
                );
                assertEquals(SCORE_MORE_THAN_ONCE_NOT_SUPPORTED_MESSAGE, illegalArgumentException.getMessage());
            }
        }
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenScoreFieldAndScoreAgainWithCollapse_thenFail() {
        // [_score desc, price, _score asc] passes the collapse rules for mixing _score with a field, but lists _score twice
        SearchContext searchContext = mockSearchContextForSortValidation(
            new Sort(SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT), new SortField(null, SortField.Type.SCORE, true)),
            true
        );

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, searchContext.query())
        );
        assertEquals(SCORE_MORE_THAN_ONCE_NOT_SUPPORTED_MESSAGE, illegalArgumentException.getMessage());
    }

    /**
     * Mocks a non-concurrent SearchContext over a single-term hybrid query with the given sort, and a
     * keyword collapse on {@link #TEXT_FIELD_NAME} when {@code withCollapse} is true.
     */
    private SearchContext mockSearchContextForSortValidation(Sort sort, boolean withCollapse) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        DocValueFormat[] docValueFormat = new DocValueFormat[sort.getSort().length];
        Arrays.fill(docValueFormat, DocValueFormat.RAW);
        when(searchContext.sort()).thenReturn(new SortAndFormats(sort, docValueFormat));
        when(searchContext.minimumScore()).thenReturn(null);

        if (withCollapse) {
            CollapseContext collapseContext = mock(CollapseContext.class);
            when(collapseContext.getFieldName()).thenReturn(TEXT_FIELD_NAME);
            when(collapseContext.getFieldType()).thenReturn(new KeywordFieldMapper.KeywordFieldType(TEXT_FIELD_NAME));
            when(searchContext.collapse()).thenReturn(collapseContext);

            IndexShard indexShard = mock(IndexShard.class);
            IndexSettings indexSettings = mock(IndexSettings.class);
            when(indexSettings.getSettings()).thenReturn(Settings.EMPTY);
            when(indexShard.indexSettings()).thenReturn(indexSettings);
            when(searchContext.indexShard()).thenReturn(indexShard);
        }

        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(TEXT_FIELD_NAME)).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQuery hybridQuery = new HybridQuery(
            List.of(termSubQuery.toQuery(mockQueryShardContext)),
            HybridQueryContext.builder().paginationDepth(10).build()
        );
        when(searchContext.query()).thenReturn(hybridQuery);

        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);
        return searchContext;
    }

    @SneakyThrows
    public void testNewCollector_whenNotConcurrentSearchAndSortingAndSearchAfterAreApplied_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        SortField sortField = new SortField("_doc", SortField.Type.DOC);
        Sort sort = new Sort(sortField);
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        FieldDoc after = new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 1 }, -1);
        when(searchContext.sort()).thenReturn(new SortAndFormats(sort, docValueFormat));
        when(searchContext.searchAfter()).thenReturn(after);
        when(searchContext.minimumScore()).thenReturn(null);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof PagingFieldCollector);
    }

    @SneakyThrows
    public void testReduce_whenMatchedDocsAndSortingIsApplied_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().build();

        HybridQuery hybridQueryWithMatchAll = new HybridQuery(
            List.of(QueryBuilders.matchAllQuery().toQuery(mockQueryShardContext)),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithMatchAll);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        when(searchContext.size()).thenReturn(1);

        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);
        SortField sortField = new SortField("_doc", SortField.Type.DOC);
        Sort sort = new Sort(sortField);
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        when(searchContext.sort()).thenReturn(new SortAndFormats(sort, docValueFormat));
        when(searchContext.minimumScore()).thenReturn(null);

        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
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
        IndexSearcher searcher = new IndexSearcher(reader);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(
            searchContext,
            hybridQueryWithMatchAll
        );
        SimpleFieldCollector simpleFieldCollector = (SimpleFieldCollector) hybridCollectorManager.newCollector();

        FieldDoc after = new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { docId1 }, -1);
        when(searchContext.searchAfter()).thenReturn(after);
        CollectorManager hybridCollectorManager1 = HybridCollectorManager.createHybridCollectorManager(
            searchContext,
            hybridQueryWithMatchAll
        );
        PagingFieldCollector pagingFieldCollector = (PagingFieldCollector) hybridCollectorManager1.newCollector();

        Weight weight = new HybridQueryWeight(hybridQueryWithMatchAll, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        simpleFieldCollector.setWeight(weight);
        pagingFieldCollector.setWeight(weight);
        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        LeafCollector leafCollector = simpleFieldCollector.getLeafCollector(leafReaderContext);
        LeafCollector leafCollector1 = pagingFieldCollector.getLeafCollector(leafReaderContext);
        BulkScorer scorer = weight.bulkScorer(leafReaderContext);
        scorer.score(leafCollector, leafReaderContext.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        leafCollector.finish();
        BulkScorer scorer1 = weight.bulkScorer(leafReaderContext);
        scorer1.score(leafCollector1, leafReaderContext.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        leafCollector1.finish();

        Object results = hybridCollectorManager.reduce(List.of(simpleFieldCollector));
        Object results1 = hybridCollectorManager1.reduce(List.of(pagingFieldCollector));

        assertNotNull(results);
        assertNotNull(results1);
        ReduceableSearchResult reduceableSearchResult = ((ReduceableSearchResult) results);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        reduceableSearchResult.reduce(querySearchResult);
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();

        assertNotNull(topDocsAndMaxScore);
        assertEquals(3, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation());
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

    @SneakyThrows
    public void testReduceWithConcurrentSegmentSearch_whenMultipleCollectorsMatchedDocs_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        // one hit per sub-query and shard, so the shard cut decides which "to" doc stays
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(1).build();

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY2).toQuery(mockQueryShardContext),
                // matches a doc in each segment
                QueryBuilders.termQuery(TEXT_FIELD_NAME, "to").toQuery(mockQueryShardContext)
            ),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.size()).thenReturn(10);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        // one shard with two segments searched as two slices: doc bases 0 and 2 keep doc ids unique across the slices
        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(false);
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, 1, TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, 2, TEST_DOC_TEXT2, ft));
        w.commit();
        w.addDocument(getDocument(TEXT_FIELD_NAME, 3, TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        assertEquals(2, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(reader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);
        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        List<LeafReaderContext> leaves = reader.leaves();
        Collector firstSlice = collectLeaves(hybridCollectorManager, weight, List.of(leaves.get(0)));
        Collector secondSlice = collectLeaves(hybridCollectorManager, weight, List.of(leaves.get(1)));
        Collector oneCollector = collectLeaves(hybridCollectorManager, weight, leaves);

        // the slices' lists are cut to one hit per sub-query, so two slices give what one collector gives
        TopDocsAndMaxScore topDocsAndMaxScore = reduceToTopDocs(hybridCollectorManager, List.of(firstSlice, secondSlice));
        TopDocsAndMaxScore oneCollectorTopDocs = reduceToTopDocs(hybridCollectorManager, List.of(oneCollector));

        assertNotNull(topDocsAndMaxScore);
        assertEquals(3, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(oneCollectorTopDocs.topDocs.totalHits, topDocsAndMaxScore.topDocs.totalHits);
        float maxScore = topDocsAndMaxScore.maxScore;
        assertTrue(maxScore > 0);
        assertEquals(oneCollectorTopDocs.maxScore, maxScore, DELTA_FOR_ASSERTION);

        ScoreDoc[] scoreDocs = topDocsAndMaxScore.topDocs.scoreDocs;
        ScoreDoc[] oneCollectorScoreDocs = oneCollectorTopDocs.topDocs.scoreDocs;
        assertEquals(8, scoreDocs.length);
        assertEquals(oneCollectorScoreDocs.length, scoreDocs.length);
        for (int i = 0; i < scoreDocs.length; i++) {
            assertEquals(oneCollectorScoreDocs[i].doc, scoreDocs[i].doc);
            assertEquals(oneCollectorScoreDocs[i].score, scoreDocs[i].score, DELTA_FOR_ASSERTION);
        }
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, DELTA_FOR_ASSERTION);
        assertEquals(0, scoreDocs[2].doc);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[3].score, DELTA_FOR_ASSERTION);
        assertEquals(1, scoreDocs[4].doc);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[5].score, DELTA_FOR_ASSERTION);
        // "to" keeps the first segment's shorter doc only; the second slice's doc 2 is cut
        assertEquals(1, scoreDocs[6].doc);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[7].score, DELTA_FOR_ASSERTION);
        assertEquals(maxScore, Math.max(scoreDocs[2].score, Math.max(scoreDocs[4].score, scoreDocs[6].score)), DELTA_FOR_ASSERTION);

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testReduceWithConcurrentSegmentSearch_whenMultipleCollectorsMatchedDocsWithSort_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().build();

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.matchAllQuery().toQuery(mockQueryShardContext)),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.size()).thenReturn(1);

        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("id", SortField.Type.DOC);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        when(searchContext.sort()).thenReturn(sortAndFormats);
        when(searchContext.minimumScore()).thenReturn(null);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        // one shard with two segments searched as two slices: doc bases 0 and 2 keep doc ids unique across the slices
        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        assertEquals(2, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(reader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);
        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        List<LeafReaderContext> leaves = reader.leaves();
        Collector firstSlice = collectLeaves(hybridCollectorManager, weight, List.of(leaves.get(0)));
        Collector secondSlice = collectLeaves(hybridCollectorManager, weight, List.of(leaves.get(1)));
        Collector oneCollector = collectLeaves(hybridCollectorManager, weight, leaves);
        assertTrue(firstSlice instanceof SimpleFieldCollector);

        // each slice keeps its lowest doc id, the shard keeps the lower of the two
        TopDocsAndMaxScore topDocsAndMaxScore = reduceToTopDocs(hybridCollectorManager, List.of(firstSlice, secondSlice));
        TopDocsAndMaxScore oneCollectorTopDocs = reduceToTopDocs(hybridCollectorManager, List.of(oneCollector));

        assertNotNull(topDocsAndMaxScore);
        assertEquals(3, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(oneCollectorTopDocs.topDocs.totalHits, topDocsAndMaxScore.topDocs.totalHits);
        float maxScore = topDocsAndMaxScore.maxScore;
        assertTrue(maxScore > 0);
        assertEquals(oneCollectorTopDocs.maxScore, maxScore, DELTA_FOR_ASSERTION);
        FieldDoc[] fieldDocs = (FieldDoc[]) topDocsAndMaxScore.topDocs.scoreDocs;
        FieldDoc[] oneCollectorFieldDocs = (FieldDoc[]) oneCollectorTopDocs.topDocs.scoreDocs;
        assertEquals(4, fieldDocs.length);
        assertEquals(oneCollectorFieldDocs.length, fieldDocs.length);
        for (int i = 0; i < fieldDocs.length; i++) {
            assertEquals(oneCollectorFieldDocs[i].doc, fieldDocs[i].doc);
            assertEquals(oneCollectorFieldDocs[i].score, fieldDocs[i].score, DELTA_FOR_ASSERTION);
            assertArrayEquals(oneCollectorFieldDocs[i].fields, fieldDocs[i].fields);
        }
        assertEquals(MAGIC_NUMBER_START_STOP, fieldDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, fieldDocs[1].score, DELTA_FOR_ASSERTION);
        assertEquals(0, fieldDocs[2].doc);
        assertEquals(0, fieldDocs[2].fields[0]);
        assertEquals(MAGIC_NUMBER_START_STOP, fieldDocs[3].score, DELTA_FOR_ASSERTION);

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testReduceAndRescore_whenMatchedDocsAndRescoreContextPresent_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        // mock to query to generate the parsed query
        doAnswer(invocationOnMock -> {
            final QueryBuilder queryBuilder = invocationOnMock.getArgument(0);
            return new ParsedQuery(queryBuilder.toQuery(mockQueryShardContext), Collections.emptyMap());
        }).when(mockQueryShardContext).toQuery(any());
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY2).toQuery(mockQueryShardContext)
            ),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        when(searchContext.size()).thenReturn(2);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
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
        IndexSearcher searcher = new IndexSearcher(reader);

        RescorerBuilder<QueryRescorerBuilder> rescorerBuilder = new QueryRescorerBuilder(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY2));
        RescoreContext rescoreContext = rescorerBuilder.buildContext(mockQueryShardContext);

        List<RescoreContext> rescoreContexts = List.of(rescoreContext);
        when(searchContext.rescore()).thenReturn(rescoreContexts);
        Weight rescoreWeight = mock(Weight.class);
        Scorer rescoreScorer = mock(Scorer.class);
        ScorerSupplier mockScorerSupplier = mock(ScorerSupplier.class);
        when(mockScorerSupplier.get(anyLong())).thenReturn(rescoreScorer);
        when(mockScorerSupplier.cost()).thenReturn(1L);
        when(rescoreWeight.scorerSupplier(any(LeafReaderContext.class))).thenReturn(mockScorerSupplier);

        when(rescoreScorer.docID()).thenReturn(1);
        DocIdSetIterator iterator = mock(DocIdSetIterator.class);
        when(rescoreScorer.iterator()).thenReturn(iterator);
        when(rescoreScorer.score()).thenReturn(0.9f);
        when(indexSearcher.createWeight(any(), eq(ScoreMode.COMPLETE), eq(1f))).thenReturn(rescoreWeight);

        CollectorManager hybridCollectorManager1 = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);
        HybridTopScoreDocCollector collector = (HybridTopScoreDocCollector) hybridCollectorManager1.newCollector();

        QueryBuilder postFilterQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);

        Query pfQuery = postFilterQuery.toQuery(mockQueryShardContext);
        ParsedQuery parsedQuery = new ParsedQuery(pfQuery);
        searchContext.parsedQuery(parsedQuery);
        when(searchContext.parsedPostFilter()).thenReturn(parsedQuery);
        when(indexSearcher.rewrite(pfQuery)).thenReturn(pfQuery);
        Weight postFilterWeight = mock(Weight.class);
        when(indexSearcher.createWeight(pfQuery, ScoreMode.COMPLETE_NO_SCORES, 1f)).thenReturn(postFilterWeight);

        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        collector.setWeight(weight);
        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);
        BulkScorer scorer = weight.bulkScorer(leafReaderContext);
        scorer.score(leafCollector, leafReaderContext.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        leafCollector.finish();

        Object results1 = hybridCollectorManager1.reduce(List.of(collector));

        assertNotNull(results1);
        ReduceableSearchResult reduceableSearchResult = ((ReduceableSearchResult) results1);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        reduceableSearchResult.reduce(querySearchResult);
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();

        assertNotNull(topDocsAndMaxScore);
        assertEquals(2, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation());
        float maxScore = topDocsAndMaxScore.maxScore;
        assertTrue(maxScore > 0);
        ScoreDoc[] scoreDocs = topDocsAndMaxScore.topDocs.scoreDocs;
        assertEquals(6, scoreDocs.length);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[3].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[5].score, DELTA_FOR_ASSERTION);

        if (Math.abs(scoreDocs[2].score - maxScore) <= DELTA_FOR_ASSERTION) {
            assertEquals(maxScore, scoreDocs[2].score, DELTA_FOR_ASSERTION);
            assertTrue(scoreDocs[4].score <= maxScore);
        } else if (Math.abs(scoreDocs[4].score - maxScore) <= DELTA_FOR_ASSERTION) {
            assertTrue(scoreDocs[2].score <= maxScore);
            assertEquals(maxScore, scoreDocs[4].score, DELTA_FOR_ASSERTION);
        } else {
            fail("neither scoreDocs[2] nor scoreDocs[4] equals maxScore");
        }

        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testRescoreWithConcurrentSegmentSearch_whenMatchedDocsAndRescore_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        // mock to query to generate the parsed query
        doAnswer(invocationOnMock -> {
            final QueryBuilder queryBuilder = invocationOnMock.getArgument(0);
            return new ParsedQuery(queryBuilder.toQuery(mockQueryShardContext), Collections.emptyMap());
        }).when(mockQueryShardContext).toQuery(any());
        // one hit per sub-query and shard, so the shard cut decides which "to" doc stays
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(1).build();

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY2).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY3).toQuery(mockQueryShardContext),
                // matches a doc in each segment
                QueryBuilders.termQuery(TEXT_FIELD_NAME, "to").toQuery(mockQueryShardContext)
            ),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.size()).thenReturn(1);
        when(searchContext.queryCollectorManagers()).thenReturn(new HashMap<>());
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        // one shard with two segments searched as two slices: doc bases 0 and 2 keep doc ids unique across the slices
        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.commit();
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();
        IndexReader reader = DirectoryReader.open(w);
        assertEquals(2, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);

        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(reader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        RescorerBuilder<QueryRescorerBuilder> rescorerBuilder = new QueryRescorerBuilder(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY2));
        RescoreContext rescoreContext = rescorerBuilder.buildContext(mockQueryShardContext);
        when(searchContext.rescore()).thenReturn(List.of(rescoreContext));
        // the rescore query matches the first segment's second doc (global doc 1) with score 0.9
        Weight rescoreWeight = mock(Weight.class);
        Scorer rescoreScorer = mock(Scorer.class);
        when(rescoreWeight.scorer(any(LeafReaderContext.class))).thenReturn(rescoreScorer);
        when(rescoreScorer.docID()).thenReturn(1);
        when(rescoreScorer.iterator()).thenReturn(mock(DocIdSetIterator.class));
        when(rescoreScorer.score()).thenReturn(0.9f);
        when(indexSearcher.createWeight(any(), eq(ScoreMode.COMPLETE), eq(1f))).thenReturn(rescoreWeight);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);
        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        List<LeafReaderContext> leaves = reader.leaves();
        Collector firstSlice = collectLeaves(hybridCollectorManager, weight, List.of(leaves.get(0)));
        Collector secondSlice = collectLeaves(hybridCollectorManager, weight, List.of(leaves.get(1)));
        Collector oneCollector = collectLeaves(hybridCollectorManager, weight, leaves);

        // the slices' lists are cut to one hit per sub-query, then rescored once, so two slices give what one collector gives
        TopDocsAndMaxScore topDocsAndMaxScore = reduceToTopDocs(hybridCollectorManager, List.of(firstSlice, secondSlice));
        TopDocsAndMaxScore oneCollectorTopDocs = reduceToTopDocs(hybridCollectorManager, List.of(oneCollector));

        assertNotNull(topDocsAndMaxScore);
        assertEquals(3, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocsAndMaxScore.topDocs.totalHits.relation());
        assertEquals(oneCollectorTopDocs.topDocs.totalHits, topDocsAndMaxScore.topDocs.totalHits);
        float maxScore = topDocsAndMaxScore.maxScore;
        assertTrue(maxScore > 0);
        assertEquals(oneCollectorTopDocs.maxScore, maxScore, DELTA_FOR_ASSERTION);
        ScoreDoc[] scoreDocs = topDocsAndMaxScore.topDocs.scoreDocs;
        ScoreDoc[] oneCollectorScoreDocs = oneCollectorTopDocs.topDocs.scoreDocs;
        assertEquals(10, scoreDocs.length);
        assertEquals(oneCollectorScoreDocs.length, scoreDocs.length);
        for (int i = 0; i < scoreDocs.length; i++) {
            assertEquals(oneCollectorScoreDocs[i].doc, scoreDocs[i].doc);
            assertEquals(oneCollectorScoreDocs[i].score, scoreDocs[i].score, DELTA_FOR_ASSERTION);
        }
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, DELTA_FOR_ASSERTION);
        assertTrue(maxScore > scoreDocs[2].score);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[3].score, DELTA_FOR_ASSERTION);
        // the second sub-query's hit is the rescored doc and carries the max score
        assertEquals(1, scoreDocs[4].doc);
        assertEquals(maxScore, scoreDocs[4].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[5].score, DELTA_FOR_ASSERTION);
        assertTrue(maxScore > scoreDocs[6].score);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[7].score, DELTA_FOR_ASSERTION);
        // "to" keeps the first segment's shorter doc only; the second slice's doc 2 is cut
        assertEquals(1, scoreDocs[8].doc);
        assertTrue(maxScore > scoreDocs[8].score);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[9].score, DELTA_FOR_ASSERTION);

        // release resources
        w.close();
        reader.close();
        directory.close();
    }

    private static Collector collectLeaves(
        final CollectorManager<?, ?> collectorManager,
        final Weight weight,
        final List<LeafReaderContext> leaves
    ) throws IOException {
        Collector collector = collectorManager.newCollector();
        collector.setWeight(weight);
        for (LeafReaderContext leaf : leaves) {
            LeafCollector leafCollector = collector.getLeafCollector(leaf);
            BulkScorer bulkScorer = weight.bulkScorer(leaf);
            bulkScorer.score(leafCollector, leaf.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
            leafCollector.finish();
        }
        return collector;
    }

    private static TopDocsAndMaxScore reduceToTopDocs(final CollectorManager collectorManager, final List<Collector> collectors)
        throws IOException {
        ReduceableSearchResult reduceableSearchResult = (ReduceableSearchResult) collectorManager.reduce(collectors);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        reduceableSearchResult.reduce(querySearchResult);
        return querySearchResult.topDocs();
    }

    @SneakyThrows
    public void testReduceAndRescore_whenRescorerThrowsException_thenFail() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1).toQuery(mockQueryShardContext),
                QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY2).toQuery(mockQueryShardContext)
            ),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        when(searchContext.size()).thenReturn(2);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        Directory directory = newDirectory();
        final IndexWriter w = new IndexWriter(directory, newIndexWriterConfig());
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.freeze();

        int docId1 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.flush();
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = new IndexSearcher(reader);

        RescoreContext rescoreContext = mock(RescoreContext.class);
        Rescorer rescorer = mock(Rescorer.class);
        when(rescoreContext.rescorer()).thenReturn(rescorer);
        when(rescorer.rescore(any(), any(), any())).thenThrow(new IOException("something happened with rescorer"));
        List<RescoreContext> rescoreContexts = List.of(rescoreContext);
        when(searchContext.rescore()).thenReturn(rescoreContexts);

        CollectorManager hybridCollectorManager1 = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);
        HybridTopScoreDocCollector collector = (HybridTopScoreDocCollector) hybridCollectorManager1.newCollector();

        Weight weight = new HybridQueryWeight(hybridQueryWithTerm, searcher, ScoreMode.TOP_SCORES, BoostingQueryBuilder.DEFAULT_BOOST);
        collector.setWeight(weight);

        LeafReaderContext leafReaderContext = searcher.getIndexReader().leaves().get(0);
        LeafCollector leafCollector = collector.getLeafCollector(leafReaderContext);

        BulkScorer scorer = weight.bulkScorer(leafReaderContext);
        scorer.score(leafCollector, leafReaderContext.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        leafCollector.finish();

        expectThrows(HybridSearchRescoreQueryException.class, () -> hybridCollectorManager1.reduce(List.of(collector)));

        // release resources
        w.close();
        reader.close();
        directory.close();
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenFromAreEqualToZeroAndPaginationDepthInRange_thenSuccessful() {
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        // pagination_depth=10
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery);
        assertNotNull(hybridCollectorManager);
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        Collector collector = hybridCollectorManager.newCollector();
        assertNotNull(collector);
        assertTrue(collector instanceof HybridTopScoreDocCollector);
    }

    @SneakyThrows
    public void testScrollWithHybridQuery_thenFail() {
        SearchContext searchContext = mock(SearchContext.class);
        ScrollContext scrollContext = new ScrollContext();
        when(searchContext.scrollContext()).thenReturn(scrollContext);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();

        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery)
        );
        assertEquals(
            String.format(Locale.ROOT, "Scroll operation is not supported in hybrid query"),
            illegalArgumentException.getMessage()
        );
    }

    @SneakyThrows
    public void testCreateCollectorManager_whenPaginationDepthIsEqualToNullAndFromIsGreaterThanZero_thenFail() {
        SearchContext searchContext = mock(SearchContext.class);
        // From >0
        when(searchContext.from()).thenReturn(5);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY1);

        HybridQuery hybridQuery = new HybridQuery(
            List.of(termSubQuery.toQuery(mockQueryShardContext)),
            HybridQueryContext.builder().build() // pagination_depth is set to null
        );

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQuery)
        );
        assertEquals(
            String.format(Locale.ROOT, "pagination_depth param is missing in the search request"),
            illegalArgumentException.getMessage()
        );
    }

    public void testReduceCollectorResults_whenCollapseEnabledWithEmptyFieldDocsButNonZeroTotalHits_thenSuccessful() throws IOException {
        /*
         * Two default-mode collapse collectors of one shard under concurrent segment search: the first counted hits (getTotalHits() > 0)
         * but kept none, every sub-query list is empty; the second kept hits. This shape once failed the reduce with "Collapse enabled
         * but source TopDocs is not an instance of CollapseTopFieldDocs", because the first collector's empty result was encoded as
         * plain TopFieldDocs. With the bounded slice merge (https://github.com/opensearch-project/neural-search/issues/2091) both
         * collectors' lists are merged per sub-query by the collapse sort and cut to numHits (1 here: the index has one document), the
         * first collector's hits still count in the shard total, and every kept hit keeps its collapse value.
         */

        // Setup search context with collapse enabled
        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        // Setup hybrid query
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().build();
        HybridQuery hybridQueryWithTerm = new HybridQuery(
            List.of(QueryBuilders.matchAllQuery().toQuery(mockQueryShardContext)),
            hybridQueryContext
        );
        when(searchContext.query()).thenReturn(hybridQueryWithTerm);

        // Setup mapper service and searcher
        MapperService mapperService = createMapperService();
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexObjects indexObjects = createIndexObjects(1);
        IndexReader indexReader = indexObjects.indexReader();
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        when(searchContext.size()).thenReturn(10);

        // Setup sort and collapse
        DocValueFormat docValueFormat[] = new DocValueFormat[] { DocValueFormat.RAW };
        SortField sortField = new SortField("id", SortField.Type.DOC);
        Sort sort = new Sort(sortField);
        SortAndFormats sortAndFormats = new SortAndFormats(sort, docValueFormat);
        when(searchContext.sort()).thenReturn(sortAndFormats);
        when(searchContext.minimumScore()).thenReturn(null);

        // Setup collapse context - THIS IS KEY
        CollapseContext collapseContext = mock(CollapseContext.class);
        when(collapseContext.getFieldName()).thenReturn("family_id_v2");
        when(searchContext.collapse()).thenReturn(collapseContext);

        // Setup concurrent search
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);
        when(searchContext.queryCollectorManagers()).thenReturn(new HashMap<>());

        // Setup index shard to avoid NPE
        IndexShard indexShard = mock(IndexShard.class);
        IndexSettings indexSettings = mock(IndexSettings.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(searchContext.indexShard()).thenReturn(indexShard);

        // Create two mock collectors
        HybridCollapsingTopDocsCollector collector1 = mock(HybridCollapsingTopDocsCollector.class);
        HybridCollapsingTopDocsCollector collector2 = mock(HybridCollapsingTopDocsCollector.class);

        // COLLECTOR 1: The scenario that previously caused the bug
        // - Has processed documents (getTotalHits > 0)
        // - But topDocs() returns list with empty FieldDocs arrays
        List<CollapseTopFieldDocs> collector1Results = new ArrayList<>();

        // Create CollapseTopFieldDocs with EMPTY scoreDocs but representing processed documents
        CollapseTopFieldDocs emptySubQuery1 = new CollapseTopFieldDocs(
            "family_id_v2",
            new TotalHits(0, TotalHits.Relation.EQUAL_TO), // No docs for this subquery
            new FieldDoc[0], // EMPTY scoreDocs
            new SortField[] { sortField },
            new Object[0] // EMPTY collapse values
        );

        CollapseTopFieldDocs emptySubQuery2 = new CollapseTopFieldDocs(
            "family_id_v2",
            new TotalHits(0, TotalHits.Relation.EQUAL_TO), // No docs for this subquery
            new FieldDoc[0], // EMPTY scoreDocs
            new SortField[] { sortField },
            new Object[0] // EMPTY collapse values
        );

        collector1Results.add(emptySubQuery1);
        collector1Results.add(emptySubQuery2);

        when(collector1.topDocs()).thenReturn(collector1Results);
        when(collector1.getTotalHits()).thenReturn(50); // KEY: Global hit count > 0
        when(collector1.getMaxScore()).thenReturn(1.0f);

        // COLLECTOR 2: Normal case with actual results
        List<CollapseTopFieldDocs> collector2Results = new ArrayList<>();

        CollapseTopFieldDocs nonEmptySubQuery1 = new CollapseTopFieldDocs(
            "family_id_v2",
            new TotalHits(10, TotalHits.Relation.EQUAL_TO),
            new FieldDoc[] { new FieldDoc(1, 1.0f, new Object[] { 1 }), new FieldDoc(2, 0.9f, new Object[] { 2 }) },
            new SortField[] { sortField },
            new Object[] { "group1", "group2" }
        );

        CollapseTopFieldDocs nonEmptySubQuery2 = new CollapseTopFieldDocs(
            "family_id_v2",
            new TotalHits(5, TotalHits.Relation.EQUAL_TO),
            new FieldDoc[] { new FieldDoc(3, 0.8f, new Object[] { 3 }) },
            new SortField[] { sortField },
            new Object[] { "group3" }
        );

        collector2Results.add(nonEmptySubQuery1);
        collector2Results.add(nonEmptySubQuery2);

        when(collector2.topDocs()).thenReturn(collector2Results);
        when(collector2.getTotalHits()).thenReturn(15);
        when(collector2.getMaxScore()).thenReturn(1.0f);

        // Create collector manager
        CollectorManager hybridCollectorManager = HybridCollectorManager.createHybridCollectorManager(searchContext, hybridQueryWithTerm);

        Object results = hybridCollectorManager.reduce(List.of(collector1, collector2));
        assertNotNull(results);
        assertTrue(results instanceof ReduceableSearchResult);

        // Verify the results are properly merged
        ReduceableSearchResult reduceableSearchResult = (ReduceableSearchResult) results;
        QuerySearchResult querySearchResult = new QuerySearchResult();
        reduceableSearchResult.reduce(querySearchResult);
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();

        assertNotNull(topDocsAndMaxScore);
        assertTrue(topDocsAndMaxScore.topDocs instanceof CollapseTopFieldDocs);
        assertEquals(65, topDocsAndMaxScore.topDocs.totalHits.value());
        assertEquals(1.0f, topDocsAndMaxScore.maxScore, DELTA_FOR_ASSERTION);
        // start, delimiter, the first sub-query's lowest doc only (doc 2 is cut), delimiter, the second sub-query's doc, stop
        CollapseTopFieldDocs collapseTopFieldDocs = (CollapseTopFieldDocs) topDocsAndMaxScore.topDocs;
        assertEquals("family_id_v2", collapseTopFieldDocs.field);
        ScoreDoc[] scoreDocs = collapseTopFieldDocs.scoreDocs;
        assertEquals(6, scoreDocs.length);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[0].score, DELTA_FOR_ASSERTION);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[1].score, DELTA_FOR_ASSERTION);
        assertEquals(1, scoreDocs[2].doc);
        assertEquals(MAGIC_NUMBER_DELIMITER, scoreDocs[3].score, DELTA_FOR_ASSERTION);
        assertEquals(3, scoreDocs[4].doc);
        assertEquals(MAGIC_NUMBER_START_STOP, scoreDocs[5].score, DELTA_FOR_ASSERTION);
        // collapse values stay aligned with the hits
        assertEquals(scoreDocs.length, collapseTopFieldDocs.collapseValues.length);
        assertEquals("group1", collapseTopFieldDocs.collapseValues[2]);
        assertEquals("group3", collapseTopFieldDocs.collapseValues[4]);
    }

    private IndexObjects createIndexObjects(int numDocs) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());

        // Add the specified number of documents
        for (int i = 1; i <= numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("field", "value" + i, Field.Store.YES));
            writer.addDocument(doc);
        }

        writer.commit();
        IndexReader indexReader = DirectoryReader.open(writer);

        return new IndexObjects(indexReader, directory, writer);
    }

    private record IndexObjects(IndexReader indexReader, Directory directory, IndexWriter writer) {
    }
}
