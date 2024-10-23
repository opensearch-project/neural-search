/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.SneakyThrows;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

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
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), 0);

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
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
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager.HybridCollectorNonConcurrentManager);

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
        HybridQuery hybridQuery = new HybridQuery(List.of(termSubQuery.toQuery(mockQueryShardContext)), 0);

        when(searchContext.query()).thenReturn(hybridQuery);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        IndexReader indexReader = mock(IndexReader.class);
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
        assertTrue(hybridCollectorManager instanceof HybridCollectorManager.HybridCollectorConcurrentSearchManager);

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
        IndexReader indexReader = mock(IndexReader.class);
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
}
