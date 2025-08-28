/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.SneakyThrows;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.HybridQueryContext;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class HybridAggregationProcessorTests extends OpenSearchQueryTestCase {

    static final String TEXT_FIELD_NAME = "field";
    static final String TERM_QUERY_TEXT = "keyword";

    private Directory directory;
    private IndexWriter writer;
    private IndexReader indexReader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexObjects indexObjects = createIndexObjects(2);
        directory = indexObjects.directory();
        writer = indexObjects.writer();
        indexReader = indexObjects.indexReader();
    }

    @SneakyThrows
    public void testAggregationProcessorDelegate_whenPreAndPostAreCalled_thenSuccessful() {
        AggregationProcessor mockAggsProcessorDelegate = mock(AggregationProcessor.class);
        HybridAggregationProcessor hybridAggregationProcessor = new HybridAggregationProcessor(mockAggsProcessorDelegate);

        SearchContext searchContext = mock(SearchContext.class);
        hybridAggregationProcessor.preProcess(searchContext);
        verify(mockAggsProcessorDelegate).preProcess(any());

        hybridAggregationProcessor.postProcess(searchContext);
        verify(mockAggsProcessorDelegate).postProcess(any());
    }

    @SneakyThrows
    public void testCollectorManager_whenHybridQueryAndNotConcurrentSearch_thenSuccessful() {
        AggregationProcessor mockAggsProcessorDelegate = mock(AggregationProcessor.class);
        HybridAggregationProcessor hybridAggregationProcessor = new HybridAggregationProcessor(mockAggsProcessorDelegate);

        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        HybridQueryContext hybridQueryContext = HybridQueryContext.builder().paginationDepth(10).build();
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), hybridQueryContext);

        when(searchContext.query()).thenReturn(hybridQuery);
        MapperService mapperService = mock(MapperService.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);

        hybridAggregationProcessor.preProcess(searchContext);

        assertEquals(1, classCollectorManagerMap.size());
        assertTrue(classCollectorManagerMap.containsKey(HybridCollectorManager.class));
        CollectorManager<? extends Collector, ReduceableSearchResult> hybridCollectorManager = classCollectorManagerMap.get(
            HybridCollectorManager.class
        );
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        // setup query result for post processing
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f) }

        );
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), new DocValueFormat[0]);
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        // set captor on collector manager to track if reduce has been called
        CollectorManager<? extends Collector, ReduceableSearchResult> hybridCollectorManagerSpy = spy(hybridCollectorManager);
        classCollectorManagerMap.put(HybridCollectorManager.class, hybridCollectorManagerSpy);

        hybridAggregationProcessor.postProcess(searchContext);

        verify(hybridCollectorManagerSpy).reduce(any());
    }

    @SneakyThrows
    public void testCollectorManager_whenHybridQueryAndConcurrentSearch_thenSuccessful() {
        AggregationProcessor mockAggsProcessorDelegate = mock(AggregationProcessor.class);
        HybridAggregationProcessor hybridAggregationProcessor = new HybridAggregationProcessor(mockAggsProcessorDelegate);

        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
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
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        hybridAggregationProcessor.preProcess(searchContext);

        assertEquals(1, classCollectorManagerMap.size());
        assertTrue(classCollectorManagerMap.containsKey(HybridCollectorManager.class));
        CollectorManager<? extends Collector, ReduceableSearchResult> hybridCollectorManager = classCollectorManagerMap.get(
            HybridCollectorManager.class
        );
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager);

        // setup query result for post processing
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f) }

        );
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), new DocValueFormat[0]);
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        // set captor on collector manager to track if reduce has been called
        CollectorManager<? extends Collector, ReduceableSearchResult> hybridCollectorManagerSpy = spy(hybridCollectorManager);
        classCollectorManagerMap.put(HybridCollectorManager.class, hybridCollectorManagerSpy);

        hybridAggregationProcessor.postProcess(searchContext);

        verifyNoInteractions(hybridCollectorManagerSpy);

        indexReader.close();
        writer.close();
        directory.close();
    }

    @SneakyThrows
    public void testCollectorManager_whenNotHybridQueryAndNotConcurrentSearch_thenSuccessful() {
        AggregationProcessor mockAggsProcessorDelegate = mock(AggregationProcessor.class);
        HybridAggregationProcessor hybridAggregationProcessor = new HybridAggregationProcessor(mockAggsProcessorDelegate);

        SearchContext searchContext = mock(SearchContext.class);
        QueryShardContext mockQueryShardContext = mock(QueryShardContext.class);
        TermQueryBuilder termSubQuery = QueryBuilders.termQuery(TEXT_FIELD_NAME, TERM_QUERY_TEXT);
        TextFieldMapper.TextFieldType fieldType = (TextFieldMapper.TextFieldType) createMapperService().fieldType(TEXT_FIELD_NAME);
        when(mockQueryShardContext.fieldMapper(eq(TEXT_FIELD_NAME))).thenReturn(fieldType);

        Query termQuery = termSubQuery.toQuery(mockQueryShardContext);

        when(searchContext.query()).thenReturn(termQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        when(indexSearcher.getIndexReader()).thenReturn(indexReader);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> classCollectorManagerMap = new HashMap<>();
        when(searchContext.queryCollectorManagers()).thenReturn(classCollectorManagerMap);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);

        hybridAggregationProcessor.preProcess(searchContext);

        assertTrue(classCollectorManagerMap.isEmpty());

        // setup query result for post processing
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f) }

        );
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), new DocValueFormat[0]);
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        when(searchContext.queryResult()).thenReturn(querySearchResult);

        hybridAggregationProcessor.postProcess(searchContext);

        assertTrue(classCollectorManagerMap.isEmpty());
    }

    private record IndexObjects(IndexReader indexReader, Directory directory, IndexWriter writer) {
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
}
