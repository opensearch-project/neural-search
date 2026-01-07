/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.processor.explain.ExplanationPayload;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.test.OpenSearchTestCase;

public class NormalizationProcessorWorkflowTests extends OpenSearchTestCase {

    public void testSearchResultTypes_whenResultsOfHybridSearch_thenDoNormalizationCombination() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 4; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            querySearchResult.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            createStartStopElementForHybridSearchResults(0),
                            createDelimiterElementForHybridSearchResults(0),
                            new ScoreDoc(0, 0.5f),
                            new ScoreDoc(2, 0.3f),
                            new ScoreDoc(4, 0.25f),
                            new ScoreDoc(10, 0.2f),
                            createStartStopElementForHybridSearchResults(0) }
                    ),
                    0.5f
                ),
                null
            );
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);
            querySearchResults.add(querySearchResult);
        }
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        TestUtils.assertQueryResultScores(querySearchResults);
    }

    public void testSearchResultTypes_whenNoMatches_thenReturnZeroResults() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 4; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            querySearchResult.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(
                        new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            createStartStopElementForHybridSearchResults(-1),
                            createDelimiterElementForHybridSearchResults(-1),
                            createStartStopElementForHybridSearchResults(-1) }
                    ),
                    0.0f
                ),
                null
            );
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);
            querySearchResults.add(querySearchResult);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        TestUtils.assertQueryResultScoresWithNoMatches(querySearchResults);
    }

    public void testFetchResults_whenOneShardAndQueryAndFetchResultsPresent_thenDoNormalizationCombination() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        new ScoreDoc(4, 0.25f),
                        new ScoreDoc(10, 0.2f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.TRUE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);
        querySearchResults.add(querySearchResult);
        SearchHit[] searchHitArray = new SearchHit[] {
            new SearchHit(0, "10", Map.of(), Map.of()),
            new SearchHit(0, "10", Map.of(), Map.of()),
            new SearchHit(0, "10", Map.of(), Map.of()),
            new SearchHit(2, "1", Map.of(), Map.of()),
            new SearchHit(4, "2", Map.of(), Map.of()),
            new SearchHit(10, "3", Map.of(), Map.of()),
            new SearchHit(0, "10", Map.of(), Map.of()), };
        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(7, TotalHits.Relation.EQUAL_TO), 10);
        fetchSearchResult.hits(searchHits);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        TestUtils.assertQueryResultScores(querySearchResults);
        TestUtils.assertFetchResultScores(fetchSearchResult, 4);
    }

    public void testFetchResults_whenOneShardAndMultipleNodes_thenDoNormalizationCombination() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        new ScoreDoc(4, 0.25f),
                        new ScoreDoc(10, 0.2f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.TRUE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);
        querySearchResults.add(querySearchResult);
        SearchHit[] searchHitArray = new SearchHit[] {
            new SearchHit(-1, "10", Map.of(), Map.of()),
            new SearchHit(-1, "10", Map.of(), Map.of()),
            new SearchHit(-1, "10", Map.of(), Map.of()),
            new SearchHit(-1, "1", Map.of(), Map.of()),
            new SearchHit(-1, "2", Map.of(), Map.of()),
            new SearchHit(-1, "3", Map.of(), Map.of()),
            new SearchHit(-1, "10", Map.of(), Map.of()), };
        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(7, TotalHits.Relation.EQUAL_TO), 10);
        fetchSearchResult.hits(searchHits);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        TestUtils.assertQueryResultScores(querySearchResults);
        TestUtils.assertFetchResultScores(fetchSearchResult, 4);
    }

    public void testFetchResultsAndNoCache_whenOneShardAndMultipleNodesAndMismatchResults_thenFail() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        new ScoreDoc(4, 0.25f),
                        new ScoreDoc(10, 0.2f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.FALSE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);
        querySearchResults.add(querySearchResult);
        SearchHits searchHits = getSearchHits();
        fetchSearchResult.hits(searchHits);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        expectThrows(IllegalStateException.class, () -> normalizationProcessorWorkflow.execute(normalizationExecuteDTO));
    }

    public void testFetchResultsAndCache_whenOneShardAndMultipleNodesAndMismatchResults_thenSuccessful() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        new ScoreDoc(4, 0.25f),
                        new ScoreDoc(10, 0.2f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.TRUE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);
        querySearchResults.add(querySearchResult);
        SearchHits searchHits = getSearchHits();
        fetchSearchResult.hits(searchHits);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        TestUtils.assertQueryResultScores(querySearchResults);
        TestUtils.assertFetchResultScores(fetchSearchResult, 4);
    }

    public void testNormalization_whenOneShardAndFromIsNegativeOne_thenSuccess() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        int shardId = 0;
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            "node",
            new ShardId("index", "uuid", shardId),
            null,
            OriginalIndices.NONE
        );

        // Setup query search results
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        FetchSearchResult fetchSearchResult = new FetchSearchResult();
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        new ScoreDoc(4, 0.25f),
                        new ScoreDoc(10, 0.2f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(shardId);
        ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        when(shardSearchRequest.requestCache()).thenReturn(Boolean.TRUE);
        querySearchResult.setShardSearchRequest(shardSearchRequest);
        querySearchResults.add(querySearchResult);
        SearchHits searchHits = getSearchHits();
        fetchSearchResult.hits(searchHits);
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // searchSourceBuilder.from(); if no from is defined here it would initialize it to -1
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchPhaseContext.getNumShards()).thenReturn(1);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        // Setup fetch search result
        fetchSearchResult.hits(searchHits);

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        // Verify that the fetch result has been updated correctly
        TestUtils.assertQueryResultScores(querySearchResults);
        TestUtils.assertFetchResultScores(fetchSearchResult, 4);
    }

    public void testNormalization_whenFromIsGreaterThanResultsSize_thenFail() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 4; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            querySearchResult.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            createStartStopElementForHybridSearchResults(0),
                            createDelimiterElementForHybridSearchResults(0),
                            new ScoreDoc(0, 0.5f),
                            new ScoreDoc(2, 0.3f),
                            new ScoreDoc(4, 0.25f),
                            new ScoreDoc(10, 0.2f),
                            createStartStopElementForHybridSearchResults(0) }
                    ),
                    0.5f
                ),
                null
            );
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);
            // requested page is out of bound for the total number of results
            querySearchResult.from(17);
            querySearchResults.add(querySearchResult);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        when(searchPhaseContext.getNumShards()).thenReturn(4);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(17);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);

        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDto = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorWorkflow.execute(normalizationExecuteDto)
        );

        assertEquals(
            String.format(Locale.ROOT, "Reached end of search result, increase pagination_depth value to see more results"),
            illegalArgumentException.getMessage()
        );
    }

    private static SearchHits getSearchHits() {
        SearchHit[] searchHitArray = new SearchHit[] {
            new SearchHit(-1, "10", Map.of(), Map.of()),
            new SearchHit(-1, "10", Map.of(), Map.of()),
            new SearchHit(-1, "10", Map.of(), Map.of()),
            new SearchHit(-1, "1", Map.of(), Map.of()),
            new SearchHit(-1, "2", Map.of(), Map.of()),
            new SearchHit(-1, "3", Map.of(), Map.of()) };
        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(7, TotalHits.Relation.EQUAL_TO), 10);
        return searchHits;
    }

    public void testExplain_whenExplainIsEnabled_thenExplanationIsStoredInPipelineContext() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 2; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            querySearchResult.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            createStartStopElementForHybridSearchResults(0),
                            createDelimiterElementForHybridSearchResults(0),
                            new ScoreDoc(0, 0.5f),
                            new ScoreDoc(2, 0.3f),
                            new ScoreDoc(4, 0.25f),
                            new ScoreDoc(10, 0.2f),
                            createStartStopElementForHybridSearchResults(0) }
                    ),
                    0.5f
                ),
                null
            );
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);
            querySearchResults.add(querySearchResult);
        }

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.explain(true); // Enable explain
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchPhaseContext.getNumShards()).thenReturn(2);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();

        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .pipelineProcessingContext(pipelineProcessingContext)
            .explain(true) // Enable explain
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        // Verify that explanation payload was stored in pipeline context
        Object explanationAttribute = pipelineProcessingContext.getAttribute(EXPLANATION_RESPONSE_KEY);
        assertNotNull("Explanation should be stored in pipeline context when explain is enabled", explanationAttribute);
        assertTrue("Explanation attribute should be of type ExplanationPayload", explanationAttribute instanceof ExplanationPayload);

        ExplanationPayload explanationPayload = (ExplanationPayload) explanationAttribute;
        assertNotNull("Explanation payload should not be null", explanationPayload.getExplainPayload());
        assertTrue(
            "Explanation payload should contain normalization processor explanations",
            explanationPayload.getExplainPayload().containsKey(ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR)
        );
    }

    public void testExplain_whenExplainIsDisabled_thenNoExplanationIsStored() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(0);
        querySearchResults.add(querySearchResult);

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.explain(false); // Disable explain
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchPhaseContext.getNumShards()).thenReturn(1);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();

        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .pipelineProcessingContext(pipelineProcessingContext)
            .explain(false) // Disable explain
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        // Verify that no explanation payload was stored in pipeline context
        Object explanationAttribute = pipelineProcessingContext.getAttribute(EXPLANATION_RESPONSE_KEY);
        assertNull("No explanation should be stored in pipeline context when explain is disabled", explanationAttribute);
    }

    public void testExplain_whenPipelineProcessingContextIsNull_thenNoExceptionThrown() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        SearchShardTarget searchShardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE);
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopDocs(
                    new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        createStartStopElementForHybridSearchResults(0),
                        createDelimiterElementForHybridSearchResults(0),
                        new ScoreDoc(0, 0.5f),
                        new ScoreDoc(2, 0.3f),
                        createStartStopElementForHybridSearchResults(0) }
                ),
                0.5f
            ),
            null
        );
        querySearchResult.setSearchShardTarget(searchShardTarget);
        querySearchResult.setShardIndex(0);
        querySearchResults.add(querySearchResult);

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.explain(true); // Enable explain
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchPhaseContext.getNumShards()).thenReturn(1);

        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .pipelineProcessingContext(null) // Null pipeline context
            .explain(true) // Enable explain
            .build();

        // Should not throw exception even with null pipeline context
        try {
            normalizationProcessorWorkflow.execute(normalizationExecuteDTO);
            // If we reach here, no exception was thrown, which is what we expect
        } catch (Exception e) {
            fail("Should not throw exception even with null pipeline context, but got: " + e.getMessage());
        }
    }
}
