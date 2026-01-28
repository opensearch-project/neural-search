/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import lombok.SneakyThrows;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.neuralsearch.query.HybridQuery;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.TestSearchContext;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HybridAggregationProcessorTests extends OpenSearchQueryTestCase {

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
    public void testPostProcess_whenHybridQueryOnSingleShard_thenSuccessful() {
        AggregationProcessor mockAggsProcessorDelegate = mock(AggregationProcessor.class);
        HybridAggregationProcessor hybridAggregationProcessor = new HybridAggregationProcessor(mockAggsProcessorDelegate);

        SearchContext searchContext = mock(SearchContext.class);
        AtomicInteger size = new AtomicInteger(0);  // Track size value

        // Setup size setter to actually store the value
        doAnswer(invocation -> {
            size.set(invocation.getArgument(0));
            return null;
        }).when(searchContext).size(anyInt());

        // Setup size getter to return the stored value
        when(searchContext.size()).thenAnswer(invocation -> size.get());

        when(searchContext.numberOfShards()).thenReturn(1);

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
        HybridQuery hybridQuery = mock(HybridQuery.class);
        when(searchContext.query()).thenReturn(hybridQuery);

        hybridAggregationProcessor.postProcess(searchContext);

        assertSame(2, size.get());
    }

    @SneakyThrows
    public void testPreProcess_whenMinScore_thenSuccessful() {
        AggregationProcessor mockAggsProcessorDelegate = mock(AggregationProcessor.class);
        HybridAggregationProcessor hybridAggregationProcessor = new HybridAggregationProcessor(mockAggsProcessorDelegate);

        TestSearchContext searchContext = new TestSearchContext(null);
        ParsedQuery parsedQuery = mock(ParsedQuery.class);
        HybridQuery hybridQuery = mock(HybridQuery.class);
        when(parsedQuery.query()).thenReturn(hybridQuery);
        searchContext.parsedQuery(parsedQuery);

        hybridAggregationProcessor.preProcess(searchContext);
        assertNull(searchContext.minimumScore());

        searchContext.minimumScore(0.5f);
        hybridAggregationProcessor.preProcess(searchContext);
        verify(mockAggsProcessorDelegate, times(2)).preProcess(any());
        assertEquals(Float.NEGATIVE_INFINITY, searchContext.minimumScore(), DELTA_FOR_ASSERTION);
    }
}
