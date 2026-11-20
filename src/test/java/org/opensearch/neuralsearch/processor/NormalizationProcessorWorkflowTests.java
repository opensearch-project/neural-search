/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocDelimiterElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createFieldDocStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.createStartStopElementForHybridSearchResults;
import static org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
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
import org.opensearch.search.DocValueFormat;
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
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

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

    public void testNormalization_whenMinScoreInvalid_thenFail() {
        testNormalization_withMinScore_thenFail(Float.POSITIVE_INFINITY);
        testNormalization_withMinScore_thenFail(Float.NEGATIVE_INFINITY);
        testNormalization_withMinScore_thenFail(Float.NaN);
    }

    /**
     * A result whose topDocs a partial reduce has already consumed still held hybrid documents — they are now in the
     * partially reduced TopDocs, raw and in the delimiter format, where this workflow cannot rewrite them. Dropping the
     * result would normalize the rest and hand the final reduce a mix; the workflow refuses instead, naming the cause.
     */
    public void testExecute_whenAResultWasConsumedByAPartialReduce_thenRefusesWithAClearMessage() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = new NormalizationProcessorWorkflow(
            new ScoreNormalizer(),
            new ScoreCombiner()
        );
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shard = 0; shard < 2; shard++) {
            QuerySearchResult result = new QuerySearchResult();
            result.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(
                        new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            createStartStopElementForHybridSearchResults(0),
                            createDelimiterElementForHybridSearchResults(0),
                            new ScoreDoc(shard, 0.5f),
                            createStartStopElementForHybridSearchResults(0) }
                    ),
                    0.5f
                ),
                null
            );
            result.setSearchShardTarget(new SearchShardTarget("node", new ShardId("index", "uuid", shard), null, OriginalIndices.NONE));
            result.setShardIndex(shard);
            querySearchResults.add(result);
        }
        querySearchResults.get(1).consumeTopDocs();
        assertTrue(querySearchResults.get(1).hasConsumedTopDocs());
        assertFalse("a consumed result is not a null instance, so the null filter alone would keep it", querySearchResults.get(1).isNull());

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(new SearchSourceBuilder().from(0));
        when(searchPhaseContext.getNumShards()).thenReturn(2);
        NormalizationProcessorWorkflowExecuteRequest request = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        IllegalStateException refused = expectThrows(IllegalStateException.class, () -> normalizationProcessorWorkflow.execute(request));
        assertTrue(
            refused.getMessage(),
            refused.getMessage().startsWith("hybrid query results were partially reduced before normalization")
        );
        assertTrue("the remedy is named", refused.getMessage().contains("request processor"));
    }

    public void testSearchResultTypes_whenNullInstanceShard_thenHandleGracefully() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();

        // First shard returns normal results
        SearchShardTarget searchShardTarget1 = new SearchShardTarget("node", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE);
        QuerySearchResult normalQuerySearchResult = new QuerySearchResult();
        normalQuerySearchResult.topDocs(
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
        normalQuerySearchResult.setSearchShardTarget(searchShardTarget1);
        normalQuerySearchResult.setShardIndex(0);
        querySearchResults.add(normalQuerySearchResult);

        // Second shard returns nullInstance - simulates match_no_docs, empty index, or query rewrite to empty
        // This is what causes the bug - nullInstance has topDocsAndMaxScore = null (never set)
        QuerySearchResult nullInstanceResult = QuerySearchResult.nullInstance();
        SearchShardTarget searchShardTarget2 = new SearchShardTarget("node", new ShardId("index", "uuid", 1), null, OriginalIndices.NONE);
        nullInstanceResult.setSearchShardTarget(searchShardTarget2);
        nullInstanceResult.setShardIndex(1);
        querySearchResults.add(nullInstanceResult);

        // Third element is an actual null in the list - tests the Objects.nonNull() check
        querySearchResults.add(null);

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchPhaseContext.getNumShards()).thenReturn(3);

        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        // Assert
        assertNotNull("Query search results should not be null", querySearchResults);
        assertEquals("Should have 3 query search results in original list", 3, querySearchResults.size());
    }

    private void testNormalization_withMinScore_thenFail(Float minScore) {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.minScore(minScore);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDto = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(Collections.emptyList())
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
            String.format(Locale.ROOT, "min_score is invalid : [%f], must neither be Infinity nor NaN", minScore),
            illegalArgumentException.getMessage()
        );
    }

    /**
     * A request with a _score-led sort and a search_after cursor: the shards return their whole window (the cursor compares
     * against the combined score, which they do not know), and this workflow keeps only the documents that sort strictly
     * after the cursor, in combined-score order with the field as tie-breaker.
     */
    public void testScoreSortedSearchAfter_whenCursorInRequest_thenOnlyDocsAfterCursorRemain() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = new NormalizationProcessorWorkflow(
            new ScoreNormalizer(),
            new ScoreCombiner()
        );
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        Object[] delimiterFields = new Object[] { 0.0f, 0 };
        // two shards, two sub-queries each; raw scores, normalized with min_max per sub-query across both shards
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 2; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            FieldDoc[] fieldDocs = shardId == 0
                ? new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 10.0f, new Object[] { 10.0f, 300 }),
                    new FieldDoc(1, 2.0f, new Object[] { 2.0f, 100 }),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(1, 4.0f, new Object[] { 4.0f, 100 }),
                    new FieldDoc(2, 1.0f, new Object[] { 1.0f, 50 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
                : new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 6.0f, new Object[] { 6.0f, 200 }),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 4.0f, new Object[] { 4.0f, 200 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) };
            querySearchResult.topDocs(
                new TopDocsAndMaxScore(new TopFieldDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), fieldDocs, sortFields), 10.0f),
                new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }
            );
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);
            querySearchResults.add(querySearchResult);
        }
        // min_max per sub-query over both shards: sub-query 0 scores {10, 2, 6} -> shard0 doc0 1.0, doc1 0.001, shard1 doc0 0.5;
        // sub-query 1 scores {4, 1, 4} -> shard0 doc1 1.0, doc2 0.001, shard1 doc0 1.0. Arithmetic mean over both sub-queries:
        // shard0 doc0 0.5 (price 300), shard0 doc1 0.5005 (100), shard0 doc2 0.0005 (50), shard1 doc0 0.75 (200)
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(10);
        // cursor between shard0 doc1 (0.5005, 100) and shard0 doc0 (0.5, 300): keeps doc0 of shard 0 and doc2 of shard 0
        searchSourceBuilder.searchAfter(new Object[] { 0.5005f, 100 });
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(2);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDTO);

        TopDocs shard0 = querySearchResults.get(0).topDocs().topDocs;
        assertTrue(shard0 instanceof TopFieldDocs);
        assertEquals(2, shard0.scoreDocs.length);
        FieldDoc first = (FieldDoc) shard0.scoreDocs[0];
        FieldDoc second = (FieldDoc) shard0.scoreDocs[1];
        assertEquals(0, first.doc);
        assertEquals(0.5f, first.score, TestUtils.DELTA_FOR_SCORE_ASSERTION);
        assertEquals(first.score, ((Number) first.fields[0]).floatValue(), 0.0f);
        assertEquals(300, ((Number) first.fields[1]).intValue());
        assertEquals(2, second.doc);
        assertEquals(0.0005f, second.score, TestUtils.DELTA_FOR_SCORE_ASSERTION);
        // shard 1's only document (0.75) sorts before the cursor and is dropped
        TopDocs shard1 = querySearchResults.get(1).topDocs().topDocs;
        assertEquals(0, shard1.scoreDocs.length);
    }

    /**
     * Two shards and a _score-led sort: on shard 0 the document with the best combined score (doc 1: 0.75 and 0.5 after
     * min_max -> 0.625) is second in both sub-query lists. The shard's page must still start with it, so that core's
     * cross-shard merge, which takes every shard's page as sorted, returns it first.
     */
    public void testScoreSortedPage_whenBestCombinedScoreHeadsNoSubQueryList_thenShardPageStartsWithIt() {
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        Object[] delimiterFields = new Object[] { 0.0f, 0 };
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        querySearchResults.add(
            sortedShardResult(
                0,
                "index",
                sortFields,
                new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 10.0f, new Object[] { 10.0f, 100 }),
                    new FieldDoc(1, 8.0f, new Object[] { 8.0f, 300 }),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(2, 10.0f, new Object[] { 10.0f, 400 }),
                    new FieldDoc(1, 6.0f, new Object[] { 6.0f, 300 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
            )
        );
        querySearchResults.add(
            sortedShardResult(
                1,
                "index",
                sortFields,
                new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 2.0f, new Object[] { 2.0f, 500 }),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 2.0f, new Object[] { 2.0f, 500 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
            )
        );

        executeWorkflow(querySearchResults, null);

        TopFieldDocs[] shardTopDocs = new TopFieldDocs[2];
        for (int shardIndex = 0; shardIndex < 2; shardIndex++) {
            shardTopDocs[shardIndex] = (TopFieldDocs) querySearchResults.get(shardIndex).topDocs().topDocs;
            for (ScoreDoc scoreDoc : shardTopDocs[shardIndex].scoreDocs) {
                scoreDoc.shardIndex = shardIndex;
            }
        }
        // shard 0: doc 1 (0.625), then docs 0 and 2 at 0.5 broken by price
        assertEquals(List.of(1, 0, 2), Arrays.stream(shardTopDocs[0].scoreDocs).map(scoreDoc -> scoreDoc.doc).toList());
        // what core's SearchPhaseController#mergeTopDocs does with more than one shard
        TopDocs merged = TopDocs.merge(new Sort(shardTopDocs[0].fields), 0, 1, shardTopDocs);
        assertEquals(0, merged.scoreDocs[0].shardIndex);
        assertEquals(1, merged.scoreDocs[0].doc);
    }

    /**
     * Two indices behind one search map the tie-breaker as integer and long. A _score-led cursor is typed for every shard
     * with that shard's own sort fields, as core types it on the shards, and every shard's page keeps the sort fields it was
     * collected with, so that core can widen the field when it merges the shards' pages.
     */
    public void testScoreSortedSearchAfter_whenIndicesMapTieBreakerWithDifferentTypes_thenEachShardComparesItsOwnType() {
        List<QuerySearchResult> page1 = shardResultsWithIntAndLongTieBreaker();
        executeWorkflow(page1, null);
        float topCombinedScore = ((FieldDoc) page1.get(0).topDocs().topDocs.scoreDocs[0]).score;

        // the cursor ties both doc 0s on the combined score and sits between their prices (5 and 9)
        List<QuerySearchResult> page2 = shardResultsWithIntAndLongTieBreaker();
        executeWorkflow(page2, new Object[] { topCombinedScore, 7 });

        // index-int: doc 0 (price 5) is before the cursor; index-long: doc 0 (price 9) is after it
        TopFieldDocs intShard = (TopFieldDocs) page2.get(0).topDocs().topDocs;
        TopFieldDocs longShard = (TopFieldDocs) page2.get(1).topDocs().topDocs;
        assertEquals(List.of(1), Arrays.stream(intShard.scoreDocs).map(scoreDoc -> scoreDoc.doc).toList());
        assertEquals(List.of(0, 1), Arrays.stream(longShard.scoreDocs).map(scoreDoc -> scoreDoc.doc).toList());
        assertEquals(9L, ((FieldDoc) longShard.scoreDocs[0]).fields[1]);
        // the pages keep their own shard's sort field types for core's cross-shard merge
        assertEquals(SortField.Type.INT, intShard.fields[1].getType());
        assertEquals(SortField.Type.LONG, longShard.fields[1].getType());
    }

    /**
     * A shard result that carries no sort fields (no documents) is typed and rebuilt with the request's sort fields, next to
     * a shard that has its own.
     */
    public void testScoreSortedSearchAfter_whenAShardResultCarriesNoSortFields_thenTheRequestSortIsUsedForIt() {
        SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE, new SortField("price", SortField.Type.INT) };
        Object[] delimiterFields = new Object[] { 0.0f, 0 };
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        querySearchResults.add(
            sortedShardResult(
                0,
                "index",
                sortFields,
                new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 4.0f, new Object[] { 4.0f, 10 }),
                    new FieldDoc(1, 2.0f, new Object[] { 2.0f, 20 }),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 4.0f, new Object[] { 4.0f, 10 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
            )
        );
        QuerySearchResult emptyShardResult = new QuerySearchResult();
        emptyShardResult.topDocs(
            new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
            new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }
        );
        emptyShardResult.setSearchShardTarget(
            new SearchShardTarget("node", new ShardId("index", "index-uuid", 1), null, OriginalIndices.NONE)
        );
        emptyShardResult.setShardIndex(1);
        querySearchResults.add(emptyShardResult);

        executeWorkflow(querySearchResults, new Object[] { 1.0f, 10 });

        // shard 0: doc 0 is the cursor document itself, doc 1 is after it
        TopFieldDocs shard0 = (TopFieldDocs) querySearchResults.get(0).topDocs().topDocs;
        assertEquals(List.of(1), Arrays.stream(shard0.scoreDocs).map(scoreDoc -> scoreDoc.doc).toList());
        TopFieldDocs shard1 = (TopFieldDocs) querySearchResults.get(1).topDocs().topDocs;
        assertEquals(0, shard1.scoreDocs.length);
        assertArrayEquals(sortFields, shard1.fields);
    }

    /**
     * A field-sorted search_after cursor is applied on the shards, which compare their own field values against it; the
     * workflow must not apply it a second time. Here every document would sort before the cursor.
     */
    public void testFieldSortedSearchAfter_whenCursorInRequest_thenTheWorkflowLeavesTheShardPagesAsTheyAre() {
        SortField[] sortFields = new SortField[] { new SortField("price", SortField.Type.INT) };
        Object[] delimiterFields = new Object[] { 0 };
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        querySearchResults.add(
            sortedShardResult(
                0,
                "index",
                sortFields,
                new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 2.0f, new Object[] { 100 }),
                    new FieldDoc(1, 1.0f, new Object[] { 200 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
            )
        );
        querySearchResults.add(
            sortedShardResult(
                1,
                "index",
                sortFields,
                new FieldDoc[] {
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                    createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                    new FieldDoc(0, 3.0f, new Object[] { 150 }),
                    createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
            )
        );

        executeWorkflow(querySearchResults, new Object[] { 300 });

        TopDocs shard0 = querySearchResults.get(0).topDocs().topDocs;
        TopDocs shard1 = querySearchResults.get(1).topDocs().topDocs;
        assertEquals(List.of(0, 1), Arrays.stream(shard0.scoreDocs).map(scoreDoc -> scoreDoc.doc).toList());
        assertEquals(List.of(0), Arrays.stream(shard1.scoreDocs).map(scoreDoc -> scoreDoc.doc).toList());
    }

    /**
     * index-int maps the tie-breaker as integer, index-long as long; both shards hold the same scores, so every document has a
     * twin with the same combined score on the other shard.
     */
    private List<QuerySearchResult> shardResultsWithIntAndLongTieBreaker() {
        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 2; shardId++) {
            boolean intShard = shardId == 0;
            SortField[] sortFields = new SortField[] {
                SortField.FIELD_SCORE,
                new SortField("price", intShard ? SortField.Type.INT : SortField.Type.LONG) };
            Object[] delimiterFields = new Object[] { 0.0f, intShard ? (Object) 0 : (Object) 0L };
            Object price = intShard ? (Object) 5 : (Object) 9L;
            Object lowPrice = intShard ? (Object) 1 : (Object) 1L;
            querySearchResults.add(
                sortedShardResult(
                    shardId,
                    intShard ? "index-int" : "index-long",
                    sortFields,
                    new FieldDoc[] {
                        createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields),
                        createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                        new FieldDoc(0, 4.0f, new Object[] { 4.0f, price }),
                        new FieldDoc(1, 2.0f, new Object[] { 2.0f, lowPrice }),
                        createFieldDocDelimiterElementForHybridSearchResults(0, delimiterFields),
                        new FieldDoc(0, 4.0f, new Object[] { 4.0f, price }),
                        createFieldDocStartStopElementForHybridSearchResults(0, delimiterFields) }
                )
            );
        }
        return querySearchResults;
    }

    private QuerySearchResult sortedShardResult(
        final int shardId,
        final String indexName,
        final SortField[] sortFields,
        final FieldDoc[] fieldDocs
    ) {
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(
            new TopDocsAndMaxScore(
                new TopFieldDocs(new TotalHits(fieldDocs.length, TotalHits.Relation.EQUAL_TO), fieldDocs, sortFields),
                fieldDocs.length > 2 ? fieldDocs[2].score : 0.0f
            ),
            Collections.nCopies(sortFields.length, DocValueFormat.RAW).toArray(new DocValueFormat[0])
        );
        querySearchResult.setSearchShardTarget(
            new SearchShardTarget("node", new ShardId(indexName, indexName + "-uuid", shardId), null, OriginalIndices.NONE)
        );
        querySearchResult.setShardIndex(shardId);
        return querySearchResult;
    }

    private void executeWorkflow(final List<QuerySearchResult> querySearchResults, final Object[] searchAfter) {
        SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0).size(10);
        if (searchAfter != null) {
            searchSourceBuilder.searchAfter(searchAfter);
        }
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(querySearchResults.size());
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner()).execute(
            NormalizationProcessorWorkflowExecuteRequest.builder()
                .querySearchResults(querySearchResults)
                .fetchSearchResultOptional(Optional.empty())
                .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
                .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
                .searchPhaseContext(searchPhaseContext)
                .build()
        );
    }
}
