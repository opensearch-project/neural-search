/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractScoreHybridizationProcessorTests extends OpenSearchTestCase {
    private static final String TEST_TAG = "test_processor";
    private static final String TEST_DESCRIPTION = "Test Processor";

    private TestScoreHybridizationProcessor processor;
    private NormalizationProcessorWorkflow normalizationWorkflow;

    private static class TestScoreHybridizationProcessor extends AbstractScoreHybridizationProcessor {
        private final String tag;
        private final String description;
        private final NormalizationProcessorWorkflow normalizationWorkflow1;

        TestScoreHybridizationProcessor(String tag, String description, NormalizationProcessorWorkflow normalizationWorkflow) {
            this.tag = tag;
            this.description = description;
            normalizationWorkflow1 = normalizationWorkflow;
        }

        @Override
        <Result extends SearchPhaseResult> void hybridizeScores(
            SearchPhaseResults<Result> searchPhaseResult,
            SearchPhaseContext searchPhaseContext,
            Optional<PipelineProcessingContext> requestContextOptional
        ) {
            NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
                .pipelineProcessingContext(requestContextOptional.orElse(null))
                .build();
            normalizationWorkflow1.execute(normalizationExecuteDTO);
        }

        @Override
        public SearchPhaseName getBeforePhase() {
            return SearchPhaseName.FETCH;
        }

        @Override
        public SearchPhaseName getAfterPhase() {
            return SearchPhaseName.QUERY;
        }

        @Override
        public String getType() {
            return "my_processor";
        }

        @Override
        public String getTag() {
            return tag;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public boolean isIgnoreFailure() {
            return false;
        }
    }

    @Before
    public void setup() {
        normalizationWorkflow = mock(NormalizationProcessorWorkflow.class);

        processor = new TestScoreHybridizationProcessor(TEST_TAG, TEST_DESCRIPTION, normalizationWorkflow);
    }

    public void testProcessorMetadata() {
        assertEquals(TEST_TAG, processor.getTag());
        assertEquals(TEST_DESCRIPTION, processor.getDescription());
    }

    public void testProcessWithExplanations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SearchPhaseContext context = mock(SearchPhaseContext.class);
        SearchPhaseResults<SearchPhaseResult> results = mock(SearchPhaseResults.class);

        sourceBuilder.explain(true);
        searchRequest.source(sourceBuilder);
        when(context.getRequest()).thenReturn(searchRequest);

        AtomicArray<SearchPhaseResult> resultsArray = new AtomicArray<>(1);
        QuerySearchResult queryResult = createQuerySearchResult();
        resultsArray.set(0, queryResult);
        when(results.getAtomicArray()).thenReturn(resultsArray);

        TestScoreHybridizationProcessor spyProcessor = spy(processor);
        spyProcessor.process(results, context);

        verify(spyProcessor).hybridizeScores(any(SearchPhaseResults.class), any(SearchPhaseContext.class), any(Optional.class));
        verify(normalizationWorkflow).execute(any());
    }

    public void testProcess() {
        SearchPhaseResults<SearchPhaseResult> searchPhaseResult = mock(SearchPhaseResults.class);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        PipelineProcessingContext requestContext = mock(PipelineProcessingContext.class);

        TestScoreHybridizationProcessor spyProcessor = spy(processor);
        spyProcessor.process(searchPhaseResult, searchPhaseContext, requestContext);

        verify(spyProcessor).hybridizeScores(any(SearchPhaseResults.class), any(SearchPhaseContext.class), any(Optional.class));
    }

    private QuerySearchResult createQuerySearchResult() {
        QuerySearchResult result = new QuerySearchResult(
            new ShardSearchContextId("test", 1),
            new SearchShardTarget("node1", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE),
            null
        );
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, 1.0f) });
        result.topDocs(new TopDocsAndMaxScore(topDocs, 1.0f), new DocValueFormat[0]);
        return result;
    }
}
