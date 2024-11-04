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
import org.opensearch.neuralsearch.processor.dto.NormalizationExecuteDto;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

public class NormalizationProcessorWorkflowTests extends OpenSearchTestCase {
    private static final String INDEX_NAME = "normalization-index";

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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        NormalizationExecuteDto normalizationExecuteDTO = NormalizationExecuteDto.builder()
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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        NormalizationExecuteDto normalizationExecuteDto = NormalizationExecuteDto.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.empty())
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDto);

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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        NormalizationExecuteDto normalizationExecuteDto = NormalizationExecuteDto.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();
        normalizationProcessorWorkflow.execute(normalizationExecuteDto);

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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        NormalizationExecuteDto normalizationExecuteDto = NormalizationExecuteDto.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();
        normalizationProcessorWorkflow.execute(normalizationExecuteDto);

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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        NormalizationExecuteDto normalizationExecuteDto = NormalizationExecuteDto.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        expectThrows(IllegalStateException.class, () -> normalizationProcessorWorkflow.execute(normalizationExecuteDto));
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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(0);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);

        NormalizationExecuteDto normalizationExecuteDto = NormalizationExecuteDto.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(Optional.of(fetchSearchResult))
            .normalizationTechnique(ScoreNormalizationFactory.DEFAULT_METHOD)
            .combinationTechnique(ScoreCombinationFactory.DEFAULT_METHOD)
            .searchPhaseContext(searchPhaseContext)
            .build();

        normalizationProcessorWorkflow.execute(normalizationExecuteDto);

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
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source().from(17);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);

        NormalizationExecuteDto normalizationExecuteDto = NormalizationExecuteDto.builder()
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
}
