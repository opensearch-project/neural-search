/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import lombok.SneakyThrows;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

public class HybridQueryPhaseSearcherTests extends OpenSearchQueryTestCase {
    private static final String VECTOR_FIELD_NAME = "vectorField";
    private static final String TEXT_FIELD_NAME = "field";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "This is really nice place to be";
    private static final String QUERY_TEXT1 = "hello";
    private static final String QUERY_TEXT2 = "randomkeyword";
    private static final Index dummyIndex = new Index("dummy", "dummy");

    @SneakyThrows
    public void testQueryType_whenQueryIsHybrid_thenCallHybridDocCollector() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldMapper.KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        releaseResources(directory, w, reader);

        verify(hybridQueryPhaseSearcher, atLeastOnce()).searchWith(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }

    @SneakyThrows
    public void testQueryType_whenQueryIsNotHybrid_thenDoNotCallHybridDocCollector() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.queryResult()).thenReturn(new QuerySearchResult());
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);

        Query query = termSubQuery.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        releaseResources(directory, w, reader);

        verify(hybridQueryPhaseSearcher, never()).extractHybridQuery(any(), any());
    }

    @SneakyThrows
    public void testQueryResult_whenOneSubQueryWithHits_thenHybridResultsAreSet() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
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
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(3);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        CollectorManager<? extends Collector, ReduceableSearchResult> collectorManager = HybridCollectorManager
            .createHybridCollectorManager(searchContext);
        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers = new HashMap<>();
        queryCollectorManagers.put(HybridCollectorManager.class, collectorManager);
        when(searchContext.queryCollectorManagers()).thenReturn(queryCollectorManagers);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);
        hybridQueryPhaseSearcher.aggregationProcessor(searchContext).postProcess(searchContext);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertEquals(0, topDocs.totalHits.value);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        assertNotNull(scoreDocs);
        assertEquals(0, scoreDocs.length);

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testQueryResult_whenMultipleTextSubQueriesWithSomeHits_thenHybridResultsAreSet() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1));
        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2));
        queryBuilder.add(QueryBuilders.matchAllQuery());

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        CollectorManager<? extends Collector, ReduceableSearchResult> collectorManager = HybridCollectorManager
            .createHybridCollectorManager(searchContext);
        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers = new HashMap<>();
        queryCollectorManagers.put(HybridCollectorManager.class, collectorManager);
        when(searchContext.queryCollectorManagers()).thenReturn(queryCollectorManagers);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);
        hybridQueryPhaseSearcher.aggregationProcessor(searchContext).postProcess(searchContext);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertEquals(0, topDocs.totalHits.value);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        assertNotNull(scoreDocs);
        assertEquals(0, scoreDocs.length);

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testWrappedHybridQuery_whenHybridWrappedIntoBool_thenFail() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(false);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(1)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1));
        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2));
        TermQueryBuilder termQuery3 = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().should(queryBuilder).should(termQuery3);

        Query query = boolQueryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> hybridQueryPhaseSearcher.searchWith(
                searchContext,
                contextIndexSearcher,
                query,
                collectors,
                hasFilterCollector,
                hasTimeout
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            exception.getMessage(),
            containsString("hybrid query must be a top level query and cannot be wrapped into other queries")
        );

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testWrappedHybridQuery_whenHybridWrappedIntoBoolAndIncorrectStructure_thenFail() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "text")
                .field("fielddata", true)
                .startObject("fielddata_frequency_filter")
                .field("min", 2d)
                .field("min_segment_size", 1000)
                .endObject();
            b.endObject();
            b.startObject("user");
            b.field("type", "nested");
            b.endObject();
        }));

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(1)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1));
        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(Queries.newNonNestedFilter(), BooleanClause.Occur.FILTER)
            .add(queryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.SHOULD);
        Query query = builder.build();

        when(searchContext.query()).thenReturn(query);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> hybridQueryPhaseSearcher.searchWith(
                searchContext,
                contextIndexSearcher,
                query,
                collectors,
                hasFilterCollector,
                hasTimeout
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            exception.getMessage(),
            containsString("cannot process hybrid query due to incorrect structure of top level query")
        );

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testWrappedHybridQuery_whenHybridWrappedIntoBoolBecauseOfNested_thenSuccess() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "text")
                .field("fielddata", true)
                .startObject("fielddata_frequency_filter")
                .field("min", 2d)
                .field("min_segment_size", 1000)
                .endObject();
            b.endObject();
            b.startObject("user");
            b.field("type", "nested");
            b.endObject();
        }));

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        when(mockQueryShardContext.getMapperService()).thenReturn(mapperService);
        when(mockQueryShardContext.simpleMatchToIndexNames(anyString())).thenReturn(Set.of(TEXT_FIELD_NAME));
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(1)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        int docId4 = RandomizedTest.randomInt();
        w.addDocument(document(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.addDocument(document(TEXT_FIELD_NAME, docId2, TEST_DOC_TEXT2, ft));
        w.addDocument(document(TEXT_FIELD_NAME, docId3, TEST_DOC_TEXT3, ft));
        w.addDocument(document(TEXT_FIELD_NAME, docId4, TEST_DOC_TEXT4, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1));
        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(queryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.MUST)
            .add(Queries.newNonNestedFilter(), BooleanClause.Occur.FILTER);
        Query query = builder.build();

        when(searchContext.query()).thenReturn(query);

        CollectorManager<? extends Collector, ReduceableSearchResult> collectorManager = HybridCollectorManager
            .createHybridCollectorManager(searchContext);
        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers = new HashMap<>();
        queryCollectorManagers.put(HybridCollectorManager.class, collectorManager);
        when(searchContext.queryCollectorManagers()).thenReturn(queryCollectorManagers);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);
        hybridQueryPhaseSearcher.aggregationProcessor(searchContext).postProcess(searchContext);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertEquals(0, topDocs.totalHits.value);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        assertNotNull(scoreDocs);
        assertEquals(0, scoreDocs.length);

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testBoolQuery_whenTooManyNestedLevels_thenSuccess() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(false);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(searchContext.getQueryShardContext()).thenReturn(mockQueryShardContext);
        IndexMetadata indexMetadata = getIndexMetadata();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, Integer.toString(1)).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        when(mockQueryShardContext.getIndexSettings()).thenReturn(indexSettings);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        Query query = createNestedBoolQuery(
            QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1).toQuery(mockQueryShardContext),
            QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2).toQuery(mockQueryShardContext),
            (int) (MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getDefault(null) + 1)
        );

        when(searchContext.query()).thenReturn(query);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertTrue(topDocs.totalHits.value > 0);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        assertNotNull(scoreDocs);
        assertTrue(scoreDocs.length > 0);
        assertFalse(isHybridQueryStartStopElement(scoreDocs[0]));
        assertFalse(isHybridQueryStartStopElement(scoreDocs[scoreDocs.length - 1]));

        releaseResources(directory, w, reader);
    }

    @SneakyThrows
    public void testAggsProcessor_whenGettingAggsProcessor_thenSuccess() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        SearchContext searchContext = mock(SearchContext.class);
        AggregationProcessor aggregationProcessor = hybridQueryPhaseSearcher.aggregationProcessor(searchContext);
        assertNotNull(aggregationProcessor);
        assertTrue(aggregationProcessor instanceof HybridAggregationProcessor);
    }

    @SneakyThrows
    public void testAggregations_whenMetricAggregation_thenSuccessful() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = spy(new HybridQueryPhaseSearcher());
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        KNNVectorFieldMapper.KNNVectorFieldType mockKNNVectorField = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        when(mockKNNVectorField.getDimension()).thenReturn(4);
        when(mockQueryShardContext.fieldMapper(eq(VECTOR_FIELD_NAME))).thenReturn(mockKNNVectorField);
        MapperService mapperService = createMapperService();
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();

        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, RandomizedTest.randomInt(), TEST_DOC_TEXT3, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1);
        queryBuilder.add(termSubQuery);

        Query query = queryBuilder.toQuery(mockQueryShardContext);
        when(searchContext.query()).thenReturn(query);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);

        releaseResources(directory, w, reader);

        verify(hybridQueryPhaseSearcher, atLeastOnce()).searchWith(any(), any(), any(), any(), anyBoolean(), anyBoolean());
    }

    @SneakyThrows
    public void testAliasWithFilter_whenHybridWrappedIntoBoolBecauseOfIndexAlias_thenSuccess() {
        HybridQueryPhaseSearcher hybridQueryPhaseSearcher = new HybridQueryPhaseSearcher();
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        when(mockQueryShardContext.index()).thenReturn(dummyIndex);

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field");
            b.field("type", "text")
                .field("fielddata", true)
                .startObject("fielddata_frequency_filter")
                .field("min", 2d)
                .field("min_segment_size", 1000)
                .endObject();
            b.endObject();
        }));

        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) mapperService.fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        when(mockQueryShardContext.getMapperService()).thenReturn(mapperService);
        when(mockQueryShardContext.simpleMatchToIndexNames(anyString())).thenReturn(Set.of(TEXT_FIELD_NAME));

        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
        FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
        ft.setIndexOptions(random().nextBoolean() ? IndexOptions.DOCS : IndexOptions.DOCS_AND_FREQS);
        ft.setOmitNorms(random().nextBoolean());
        ft.freeze();
        int docId1 = RandomizedTest.randomInt();
        int docId2 = RandomizedTest.randomInt();
        int docId3 = RandomizedTest.randomInt();
        int docId4 = RandomizedTest.randomInt();
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId1, TEST_DOC_TEXT1, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId2, TEST_DOC_TEXT2, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId3, TEST_DOC_TEXT3, ft));
        w.addDocument(getDocument(TEXT_FIELD_NAME, docId4, TEST_DOC_TEXT4, ft));
        w.commit();

        IndexReader reader = DirectoryReader.open(w);
        SearchContext searchContext = mock(SearchContext.class);

        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        ShardId shardId = new ShardId(dummyIndex, 1);
        SearchShardTarget shardTarget = new SearchShardTarget(
            randomAlphaOfLength(10),
            shardId,
            randomAlphaOfLength(10),
            OriginalIndices.NONE
        );
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.size()).thenReturn(4);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        when(searchContext.queryResult()).thenReturn(querySearchResult);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.mapperService()).thenReturn(mapperService);

        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
        boolean hasFilterCollector = randomBoolean();
        boolean hasTimeout = randomBoolean();

        HybridQueryBuilder queryBuilder = new HybridQueryBuilder();

        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1));
        queryBuilder.add(QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT2));
        Query termFilter = QueryBuilders.termQuery(TEXT_FIELD_NAME, QUERY_TEXT1).toQuery(mockQueryShardContext);

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(queryBuilder.toQuery(mockQueryShardContext), BooleanClause.Occur.SHOULD).add(termFilter, BooleanClause.Occur.FILTER);
        Query query = builder.build();

        when(searchContext.query()).thenReturn(query);
        when(searchContext.aliasFilter()).thenReturn(termFilter);

        CollectorManager<? extends Collector, ReduceableSearchResult> collectorManager = HybridCollectorManager
            .createHybridCollectorManager(searchContext);
        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers = new HashMap<>();
        queryCollectorManagers.put(HybridCollectorManager.class, collectorManager);
        when(searchContext.queryCollectorManagers()).thenReturn(queryCollectorManagers);

        hybridQueryPhaseSearcher.searchWith(searchContext, contextIndexSearcher, query, collectors, hasFilterCollector, hasTimeout);
        hybridQueryPhaseSearcher.aggregationProcessor(searchContext).postProcess(searchContext);

        assertNotNull(querySearchResult.topDocs());
        TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
        TopDocs topDocs = topDocsAndMaxScore.topDocs;
        assertEquals(0, topDocs.totalHits.value);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        assertNotNull(scoreDocs);
        assertEquals(0, scoreDocs.length);

        releaseResources(directory, w, reader);
    }

    private void releaseResources(Directory directory, IndexWriter w, IndexReader reader) throws IOException {
        w.close();
        reader.close();
        directory.close();
    }

    private BooleanQuery createNestedBoolQuery(final Query query1, final Query query2, int level) {
        if (level == 0) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query1, BooleanClause.Occur.SHOULD).add(query2, BooleanClause.Occur.SHOULD);
            return builder.build();
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(createNestedBoolQuery(query1, query2, level - 1), BooleanClause.Occur.MUST);
        return builder.build();
    }

    private static Document document(final String fieldName, int docId, final String fieldValue, final FieldType ft) {
        Document doc = new Document();
        doc.add(new TextField("id", Integer.toString(docId), Field.Store.YES));
        doc.add(new Field(fieldName, fieldValue, ft));
        doc.add(new NumericDocValuesField(PRIMARY_TERM_NAME, 0));
        return doc;
    }

    private static IndexMetadata getIndexMetadata() {
        Map<String, String> remoteCustomData = Map.of(
            RemoteStoreEnums.PathType.NAME,
            HASHED_PREFIX.name(),
            RemoteStoreEnums.PathHashAlgorithm.NAME,
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.name()
        );
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData)
            .build();
        return indexMetadata;
    }
}
