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
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.index.shard.ShardId;
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

        normalizationProcessorWorkflow.execute(
            querySearchResults,
            Optional.empty(),
            ScoreNormalizationFactory.DEFAULT_METHOD,
            ScoreCombinationFactory.DEFAULT_METHOD,
            0,
            false
        );

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

        normalizationProcessorWorkflow.execute(
            querySearchResults,
            Optional.empty(),
            ScoreNormalizationFactory.DEFAULT_METHOD,
            ScoreCombinationFactory.DEFAULT_METHOD,
            0,
            false
        );

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

        normalizationProcessorWorkflow.execute(
            querySearchResults,
            Optional.of(fetchSearchResult),
            ScoreNormalizationFactory.DEFAULT_METHOD,
            ScoreCombinationFactory.DEFAULT_METHOD,
            0,
            false
        );

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

        normalizationProcessorWorkflow.execute(
            querySearchResults,
            Optional.of(fetchSearchResult),
            ScoreNormalizationFactory.DEFAULT_METHOD,
            ScoreCombinationFactory.DEFAULT_METHOD,
            0,
            false
        );

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

        expectThrows(
            IllegalStateException.class,
            () -> normalizationProcessorWorkflow.execute(
                querySearchResults,
                Optional.of(fetchSearchResult),
                ScoreNormalizationFactory.DEFAULT_METHOD,
                ScoreCombinationFactory.DEFAULT_METHOD,
                0,
                false
            )
        );
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

        normalizationProcessorWorkflow.execute(
            querySearchResults,
            Optional.of(fetchSearchResult),
            ScoreNormalizationFactory.DEFAULT_METHOD,
            ScoreCombinationFactory.DEFAULT_METHOD,
            0,
            false
        );

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

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> normalizationProcessorWorkflow.execute(
                querySearchResults,
                Optional.empty(),
                ScoreNormalizationFactory.DEFAULT_METHOD,
                ScoreCombinationFactory.DEFAULT_METHOD,
                0,
                false
            )
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
