/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchPhaseResult;
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

    public void testSearchResultTypes_whenNotCompoundDocsOrEmptyResults_thenNoProcessing() {
        NormalizationProcessor normalizationProcessor = spy(
            new NormalizationProcessor(
                PROCESSOR_TAG,
                DESCRIPTION,
                ScoreNormalizationTechnique.MIN_MAX,
                ScoreCombinationTechnique.ARITHMETIC_MEAN
            )
        );

        assertEquals(SearchPhaseName.FETCH, normalizationProcessor.getAfterPhase());
        assertEquals(SearchPhaseName.QUERY, normalizationProcessor.getBeforePhase());
        assertEquals(DESCRIPTION, normalizationProcessor.getDescription());
        assertEquals(PROCESSOR_TAG, normalizationProcessor.getTag());
        assertEquals(true, normalizationProcessor.isIgnoreFailure());
        assertEquals("normalization-processor", normalizationProcessor.getType());

        SearchPhaseResults searchPhaseResults = mock(SearchPhaseResults.class);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        normalizationProcessor.process(searchPhaseResults, searchPhaseContext);

        verify(normalizationProcessor, never()).updateOriginalQueryResults(any(), any(), any(), any());

        AtomicArray<SearchPhaseResult> resultAtomicArray = new AtomicArray<>(1);
        when(searchPhaseResults.getAtomicArray()).thenReturn(resultAtomicArray);
        normalizationProcessor.process(searchPhaseResults, searchPhaseContext);

        verify(normalizationProcessor, never()).updateOriginalQueryResults(any(), any(), any(), any());
    }

    public void testSearchResultTypes_whenCompoundDocs_thenDoNormalizationCombination() {
        NormalizationProcessor normalizationProcessor = spy(
            new NormalizationProcessor(
                PROCESSOR_TAG,
                DESCRIPTION,
                ScoreNormalizationTechnique.MIN_MAX,
                ScoreCombinationTechnique.ARITHMETIC_MEAN
            )
        );

        assertEquals(SearchPhaseName.FETCH, normalizationProcessor.getAfterPhase());
        assertEquals(SearchPhaseName.QUERY, normalizationProcessor.getBeforePhase());

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

        verify(normalizationProcessor, times(1)).updateOriginalQueryResults(any(), any(), any(), any());
    }
}
