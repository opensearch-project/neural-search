/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.After;
import org.junit.Before;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseController;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class NormalizationProcessorTests extends OpenSearchTestCase {
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";
    private static final String INDEX_NAME = "index1";
    private static final String NORMALIZATION_METHOD = "min_max";
    private static final String COMBINATION_METHOD = "arithmetic_mean";
    private SearchPhaseController searchPhaseController;
    private ThreadPool threadPool;
    private OpenSearchThreadPoolExecutor executor;

    @Before
    public void setup() {
        searchPhaseController = new SearchPhaseController(writableRegistry(), s -> new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    () -> PipelineAggregator.PipelineTree.EMPTY
                );
            }

            public InternalAggregation.ReduceContext forFinalReduction() {
                return InternalAggregation.ReduceContext.forFinalReduction(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    b -> {},
                    PipelineAggregator.PipelineTree.EMPTY
                );
            };
        });
        threadPool = new TestThreadPool(NormalizationProcessorTests.class.getName());
        executor = OpenSearchExecutors.newFixed(
            "test",
            1,
            10,
            OpenSearchExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext()
        );
        TestUtils.initializeEventStatsManager();
    }

    @After
    public void cleanup() {
        executor.shutdownNow();
        terminate(threadPool);
    }

    public void testClassFields_whenCreateNewObject_thenAllFieldsPresent() {
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(COMBINATION_METHOD),
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()),
            false
        );

        assertEquals(DESCRIPTION, normalizationProcessor.getDescription());
        assertEquals(PROCESSOR_TAG, normalizationProcessor.getTag());
        assertEquals(SearchPhaseName.FETCH, normalizationProcessor.getAfterPhase());
        assertEquals(SearchPhaseName.QUERY, normalizationProcessor.getBeforePhase());
        assertFalse(normalizationProcessor.isIgnoreFailure());
    }

    public void testSearchResultTypes_whenCompoundDocs_thenDoNormalizationCombination() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(COMBINATION_METHOD),
            normalizationProcessorWorkflow,
            false
        );

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.setBatchedReduceSize(4);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            searchPhaseController,
            SearchProgressListener.NOOP,
            writableRegistry(),
            10,
            e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                curr.addSuppressed(prev);
                return curr;
            })
        );
        CountDownLatch partialReduceLatch = new CountDownLatch(5);
        for (int shardId = 0; shardId < 4; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            TopDocs topDocs = new TopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),

                new ScoreDoc[] {
                    createStartStopElementForHybridSearchResults(4),
                    createDelimiterElementForHybridSearchResults(4),
                    new ScoreDoc(0, 0.5f),
                    new ScoreDoc(2, 0.3f),
                    new ScoreDoc(4, 0.25f),
                    new ScoreDoc(10, 0.2f),
                    createStartStopElementForHybridSearchResults(4) }

            );
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), null);
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);

            queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        normalizationProcessor.process(queryPhaseResultConsumer, searchPhaseContext);

        List<QuerySearchResult> querySearchResults = queryPhaseResultConsumer.getAtomicArray()
            .asList()
            .stream()
            .map(result -> result == null ? null : result.queryResult())
            .collect(Collectors.toList());

        TestUtils.assertQueryResultScores(querySearchResults);
    }

    public void testScoreCorrectness_whenCompoundDocs_thenDoNormalizationCombination() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(COMBINATION_METHOD),
            normalizationProcessorWorkflow,
            false
        );

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.setBatchedReduceSize(4);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            searchPhaseController,
            SearchProgressListener.NOOP,
            writableRegistry(),
            10,
            e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                curr.addSuppressed(prev);
                return curr;
            })
        );
        CountDownLatch partialReduceLatch = new CountDownLatch(1);
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(
            new TotalHits(4, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(10),
                createDelimiterElementForHybridSearchResults(10),
                new ScoreDoc(2429, 0.028685084f),
                new ScoreDoc(14, 0.025785536f),
                new ScoreDoc(10, 0.024871103f),
                createDelimiterElementForHybridSearchResults(10),
                new ScoreDoc(2429, 25.438505f),
                new ScoreDoc(10, 25.226639f),
                new ScoreDoc(14, 24.935198f),
                new ScoreDoc(2428, 21.614073f),
                createStartStopElementForHybridSearchResults(10) }

        );
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 25.438505f), null);
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);

        queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        when(searchPhaseContext.getNumShards()).thenReturn(1);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        normalizationProcessor.process(queryPhaseResultConsumer, searchPhaseContext);

        List<QuerySearchResult> querySearchResults = queryPhaseResultConsumer.getAtomicArray()
            .asList()
            .stream()
            .map(result -> result == null ? null : result.queryResult())
            .collect(Collectors.toList());

        TestUtils.assertQueryResultScores(querySearchResults);
    }

    public void testEmptySearchResults_whenEmptySearchResults_thenDoNotExecuteWorkflow() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(ArithmeticMeanScoreCombinationTechnique.TECHNIQUE_NAME),
            normalizationProcessorWorkflow,
            false
        );
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        normalizationProcessor.process(null, searchPhaseContext);
        verify(normalizationProcessorWorkflow, never()).execute(any());
    }

    public void testNotHybridSearchResult_whenResultsNotEmptyAndNotHybridSearchResult_thenDoNotExecuteWorkflow() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(COMBINATION_METHOD),
            normalizationProcessorWorkflow,
            false
        );

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.setBatchedReduceSize(4);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            searchPhaseController,
            SearchProgressListener.NOOP,
            writableRegistry(),
            10,
            e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                curr.addSuppressed(prev);
                return curr;
            })
        );
        CountDownLatch partialReduceLatch = new CountDownLatch(5);
        int numberOfShards = 4;
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            TopDocs topDocs = new TopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f), new ScoreDoc(4, 0.25f), new ScoreDoc(10, 0.2f) }
            );
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), null);
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);

            queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);
        normalizationProcessor.process(queryPhaseResultConsumer, searchPhaseContext);
        verify(normalizationProcessorWorkflow, never()).execute(any());
    }

    public void testResultTypes_whenQueryAndFetchPresentAndSizeSame_thenCallNormalization() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(COMBINATION_METHOD),
            normalizationProcessorWorkflow,
            false
        );

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        searchRequest.setBatchedReduceSize(4);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            searchPhaseController,
            SearchProgressListener.NOOP,
            writableRegistry(),
            10,
            e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                curr.addSuppressed(prev);
                return curr;
            })
        );
        CountDownLatch partialReduceLatch = new CountDownLatch(5);
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(
            new TotalHits(4, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(4),
                createDelimiterElementForHybridSearchResults(4),
                new ScoreDoc(0, 0.5f),
                new ScoreDoc(2, 0.3f),
                new ScoreDoc(4, 0.25f),
                new ScoreDoc(10, 0.2f),
                createStartStopElementForHybridSearchResults(4) }

        );
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), null);
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);

        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        fetchSearchResult.setShardIndex(shardId);
        fetchSearchResult.setSearchShardTarget(searchShardTarget);
        SearchHit[] searchHitArray = new SearchHit[] {
            new SearchHit(4, "2", Map.of(), Map.of()),
            new SearchHit(4, "2", Map.of(), Map.of()),
            new SearchHit(0, "10", Map.of(), Map.of()),
            new SearchHit(2, "1", Map.of(), Map.of()),
            new SearchHit(4, "2", Map.of(), Map.of()),
            new SearchHit(10, "3", Map.of(), Map.of()),
            new SearchHit(4, "2", Map.of(), Map.of()) };
        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(7, TotalHits.Relation.EQUAL_TO), 10);
        fetchSearchResult.hits(searchHits);

        QueryFetchSearchResult queryFetchSearchResult = new QueryFetchSearchResult(querySearchResult, fetchSearchResult);
        queryFetchSearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.TRUE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);

        queryPhaseResultConsumer.consumeResult(queryFetchSearchResult, partialReduceLatch::countDown);

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        normalizationProcessor.process(queryPhaseResultConsumer, searchPhaseContext);

        List<QuerySearchResult> querySearchResults = queryPhaseResultConsumer.getAtomicArray()
            .asList()
            .stream()
            .map(result -> result == null ? null : result.queryResult())
            .collect(Collectors.toList());

        TestUtils.assertQueryResultScores(querySearchResults);
        verify(normalizationProcessorWorkflow).execute(any());
    }

    public void testResultTypes_whenQueryAndFetchPresentButSizeDifferent_thenFail() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );
        NormalizationProcessor normalizationProcessor = new NormalizationProcessor(
            PROCESSOR_TAG,
            DESCRIPTION,
            new ScoreNormalizationFactory().createNormalization(NORMALIZATION_METHOD),
            new ScoreCombinationFactory().createCombination(COMBINATION_METHOD),
            normalizationProcessorWorkflow,
            false
        );

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.setBatchedReduceSize(4);
        AtomicReference<Exception> onPartialMergeFailure = new AtomicReference<>();
        QueryPhaseResultConsumer queryPhaseResultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            searchPhaseController,
            SearchProgressListener.NOOP,
            writableRegistry(),
            10,
            e -> onPartialMergeFailure.accumulateAndGet(e, (prev, curr) -> {
                curr.addSuppressed(prev);
                return curr;
            })
        );
        CountDownLatch partialReduceLatch = new CountDownLatch(5);
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        TopDocs topDocs = new TopDocs(
            new TotalHits(4, TotalHits.Relation.EQUAL_TO),

            new ScoreDoc[] {
                createStartStopElementForHybridSearchResults(4),
                createDelimiterElementForHybridSearchResults(4),
                new ScoreDoc(0, 0.5f),
                new ScoreDoc(2, 0.3f),
                new ScoreDoc(4, 0.25f),
                new ScoreDoc(10, 0.2f),
                createStartStopElementForHybridSearchResults(4) }

        );
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), null);
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);

        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        fetchSearchResult.setShardIndex(shardId);
        fetchSearchResult.setSearchShardTarget(searchShardTarget);
        SearchHit[] searchHitArray = new SearchHit[] {
            new SearchHit(0, "10", Map.of(), Map.of()),
            new SearchHit(2, "1", Map.of(), Map.of()),
            new SearchHit(4, "2", Map.of(), Map.of()),
            new SearchHit(10, "3", Map.of(), Map.of()),
            new SearchHit(0, "10", Map.of(), Map.of()), };
        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(5, TotalHits.Relation.EQUAL_TO), 10);
        fetchSearchResult.hits(searchHits);

        QueryFetchSearchResult queryFetchSearchResult = new QueryFetchSearchResult(querySearchResult, fetchSearchResult);
        queryFetchSearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.FALSE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);

        queryPhaseResultConsumer.consumeResult(queryFetchSearchResult, partialReduceLatch::countDown);

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> normalizationProcessor.process(queryPhaseResultConsumer, searchPhaseContext)
        );
        org.hamcrest.MatcherAssert.assertThat(
            exception.getMessage(),
            startsWith("score normalization processor cannot produce final query result")
        );
    }
}
