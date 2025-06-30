/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RRFProcessorTests extends OpenSearchTestCase {

    @Mock
    private ScoreNormalizationTechnique mockNormalizationTechnique;
    @Mock
    private ScoreCombinationTechnique mockCombinationTechnique;
    @Mock
    private NormalizationProcessorWorkflow mockNormalizationWorkflow;
    @Mock
    private SearchPhaseResults<SearchPhaseResult> mockSearchPhaseResults;
    @Mock
    private SearchPhaseContext mockSearchPhaseContext;
    @Mock
    private QueryPhaseResultConsumer mockQueryPhaseResultConsumer;

    private RRFProcessor rrfProcessor;
    private static final String TAG = "tag";
    private static final String DESCRIPTION = "description";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        rrfProcessor = new RRFProcessor(
            TAG,
            DESCRIPTION,
            mockNormalizationTechnique,
            mockCombinationTechnique,
            mockNormalizationWorkflow,
            false
        );
        when(mockCombinationTechnique.techniqueName()).thenReturn(RRFScoreCombinationTechnique.TECHNIQUE_NAME);
        TestUtils.initializeEventStatsManager();
    }

    @SneakyThrows
    public void testGetType() {
        assertEquals(RRFProcessor.TYPE, rrfProcessor.getType());
    }

    @SneakyThrows
    public void testGetBeforePhase() {
        assertEquals(SearchPhaseName.QUERY, rrfProcessor.getBeforePhase());
    }

    @SneakyThrows
    public void testGetAfterPhase() {
        assertEquals(SearchPhaseName.FETCH, rrfProcessor.getAfterPhase());
    }

    @SneakyThrows
    public void testIsIgnoreFailure() {
        assertFalse(rrfProcessor.isIgnoreFailure());
    }

    @SneakyThrows
    public void testProcess_whenNullSearchPhaseResult_thenSkipWorkflow() {
        rrfProcessor.process(null, mockSearchPhaseContext);
        verify(mockNormalizationWorkflow, never()).execute(any());
    }

    @SneakyThrows
    public void testProcess_whenNonQueryPhaseResultConsumer_thenSkipWorkflow() {
        rrfProcessor.process(mockSearchPhaseResults, mockSearchPhaseContext);
        verify(mockNormalizationWorkflow, never()).execute(any());
    }

    @SneakyThrows
    public void testProcess_whenValidHybridInput_thenSucceed() {
        QuerySearchResult result = createQuerySearchResult(true);
        AtomicArray<SearchPhaseResult> atomicArray = new AtomicArray<>(1);
        atomicArray.set(0, result);

        when(mockQueryPhaseResultConsumer.getAtomicArray()).thenReturn(atomicArray);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        when(mockSearchPhaseContext.getRequest()).thenReturn(searchRequest);

        rrfProcessor.process(mockQueryPhaseResultConsumer, mockSearchPhaseContext);

        verify(mockNormalizationWorkflow).execute(any(NormalizationProcessorWorkflowExecuteRequest.class));
    }

    @SneakyThrows
    public void testProcess_whenValidNonHybridInput_thenSucceed() {
        QuerySearchResult result = createQuerySearchResult(false);
        AtomicArray<SearchPhaseResult> atomicArray = new AtomicArray<>(1);
        atomicArray.set(0, result);

        when(mockQueryPhaseResultConsumer.getAtomicArray()).thenReturn(atomicArray);

        rrfProcessor.process(mockQueryPhaseResultConsumer, mockSearchPhaseContext);

        verify(mockNormalizationWorkflow, never()).execute(any(NormalizationProcessorWorkflowExecuteRequest.class));
    }

    @SneakyThrows
    public void testGetTag() {
        assertEquals(TAG, rrfProcessor.getTag());
    }

    @SneakyThrows
    public void testGetDescription() {
        assertEquals(DESCRIPTION, rrfProcessor.getDescription());
    }

    @SneakyThrows
    public void testShouldSkipProcessor() {
        assertTrue(rrfProcessor.shouldSkipProcessor(null));
        assertTrue(rrfProcessor.shouldSkipProcessor(mockSearchPhaseResults));

        AtomicArray<SearchPhaseResult> atomicArray = new AtomicArray<>(1);
        atomicArray.set(0, createQuerySearchResult(false));
        when(mockQueryPhaseResultConsumer.getAtomicArray()).thenReturn(atomicArray);

        assertTrue(rrfProcessor.shouldSkipProcessor(mockQueryPhaseResultConsumer));

        atomicArray.set(0, createQuerySearchResult(true));
        assertFalse(rrfProcessor.shouldSkipProcessor(mockQueryPhaseResultConsumer));
    }

    @SneakyThrows
    public void testGetQueryPhaseSearchResults() {
        AtomicArray<SearchPhaseResult> atomicArray = new AtomicArray<>(2);
        atomicArray.set(0, createQuerySearchResult(true));
        atomicArray.set(1, createQuerySearchResult(false));
        when(mockQueryPhaseResultConsumer.getAtomicArray()).thenReturn(atomicArray);

        List<QuerySearchResult> results = rrfProcessor.getQueryPhaseSearchResults(mockQueryPhaseResultConsumer);
        assertEquals(2, results.size());
        assertNotNull(results.get(0));
        assertNotNull(results.get(1));
    }

    @SneakyThrows
    public void testGetFetchSearchResults() {
        AtomicArray<SearchPhaseResult> atomicArray = new AtomicArray<>(1);
        atomicArray.set(0, createQuerySearchResult(true));
        when(mockQueryPhaseResultConsumer.getAtomicArray()).thenReturn(atomicArray);

        Optional<FetchSearchResult> result = rrfProcessor.getFetchSearchResults(mockQueryPhaseResultConsumer);
        assertFalse(result.isPresent());
    }

    @SneakyThrows
    public void testProcess_whenExplainIsTrue_thenExplanationIsAdded() {
        QuerySearchResult result = createQuerySearchResult(true);
        AtomicArray<SearchPhaseResult> atomicArray = new AtomicArray<>(1);
        atomicArray.set(0, result);

        when(mockQueryPhaseResultConsumer.getAtomicArray()).thenReturn(atomicArray);

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.explain(true);
        searchRequest.source(sourceBuilder);
        when(mockSearchPhaseContext.getRequest()).thenReturn(searchRequest);

        rrfProcessor.process(mockQueryPhaseResultConsumer, mockSearchPhaseContext);

        // Capture the actual request
        ArgumentCaptor<NormalizationProcessorWorkflowExecuteRequest> requestCaptor = ArgumentCaptor.forClass(
            NormalizationProcessorWorkflowExecuteRequest.class
        );
        verify(mockNormalizationWorkflow).execute(requestCaptor.capture());

        // Verify the captured request
        NormalizationProcessorWorkflowExecuteRequest capturedRequest = requestCaptor.getValue();
        assertTrue(capturedRequest.isExplain());
        assertNull(capturedRequest.getPipelineProcessingContext());
    }

    private QuerySearchResult createQuerySearchResult(boolean isHybrid) {
        ShardId shardId = new ShardId("index", "uuid", 0);
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "index" }, IndicesOptions.strictExpandOpenAndForbidClosed());
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.allowPartialSearchResults(true);

        int numberOfShards = 1;
        AliasFilter aliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
        float indexBoost = 1.0f;
        long nowInMillis = System.currentTimeMillis();
        String clusterAlias = null;
        String[] indexRoutings = Strings.EMPTY_ARRAY;

        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            numberOfShards,
            aliasFilter,
            indexBoost,
            nowInMillis,
            clusterAlias,
            indexRoutings
        );

        QuerySearchResult result = new QuerySearchResult(
            new ShardSearchContextId("test", 1),
            new SearchShardTarget("node1", shardId, clusterAlias, originalIndices),
            shardSearchRequest
        );
        result.from(0).size(10);

        ScoreDoc[] scoreDocs;
        if (isHybrid) {
            scoreDocs = new ScoreDoc[] { HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults(0) };
        } else {
            scoreDocs = new ScoreDoc[] { new ScoreDoc(0, 1.0f) };
        }

        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), scoreDocs);
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, 1.0f);
        result.topDocs(topDocsAndMaxScore, new DocValueFormat[0]);

        return result;
    }
}
