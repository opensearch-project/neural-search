/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;
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
import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.TestUtils;
import org.opensearch.neuralsearch.processor.combination.ArithmeticMeanScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
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
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
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
            normalizationProcessorWorkflow
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
            CompoundTopDocs topDocs = new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f), new ScoreDoc(4, 0.25f), new ScoreDoc(10, 0.2f) }
                    )
                )
            );
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), new DocValueFormat[0]);
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);

            queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
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
            normalizationProcessorWorkflow
        );
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        normalizationProcessor.process(null, searchPhaseContext);

        verify(normalizationProcessorWorkflow, never()).execute(any(), any(), any());
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
            normalizationProcessorWorkflow
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
                new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f), new ScoreDoc(4, 0.25f), new ScoreDoc(10, 0.2f) }
            );
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), new DocValueFormat[0]);
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);

            queryPhaseResultConsumer.consumeResult(querySearchResult, partialReduceLatch::countDown);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        normalizationProcessor.process(queryPhaseResultConsumer, searchPhaseContext);

        verify(normalizationProcessorWorkflow, never()).execute(any(), any(), any());
    }
}
