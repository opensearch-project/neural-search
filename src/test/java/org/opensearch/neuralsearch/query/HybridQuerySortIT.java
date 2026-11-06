/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.BeforeClass;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.TestUtils.assertHitResultsFromQueryWhenSortIsEnabled;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import org.opensearch.search.sort.SortOrder;

public class HybridQuerySortIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS = "test-hybrid-sort-multi-doc-index-multiple-shards";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD = "test-hybrid-sort-multi-doc-index-single-shard";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-sort-pipeline";
    private static final String INTEGER_FIELD_1_STOCK = "stock";
    private static final String TEXT_FIELD_1_NAME = "name";
    private static final String KEYWORD_FIELD_2_CATEGORY = "category";
    private static final String TEXT_FIELD_VALUE_1_DUNES = "Dunes part 1";
    private static final String TEXT_FIELD_VALUE_2_DUNES = "Dunes part 2";
    private static final String TEXT_FIELD_VALUE_3_MI_1 = "Mission Impossible 1";
    private static final String TEXT_FIELD_VALUE_4_MI_2 = "Mission Impossible 2";
    private static final String TEXT_FIELD_VALUE_5_TERMINAL = "The Terminal";
    private static final String TEXT_FIELD_VALUE_6_AVENGERS = "Avengers";
    private static final int INTEGER_FIELD_STOCK_1_25 = 25;
    private static final int INTEGER_FIELD_STOCK_2_22 = 22;
    private static final int INTEGER_FIELD_STOCK_3_256 = 256;
    private static final int INTEGER_FIELD_STOCK_4_25 = 25;
    private static final int INTEGER_FIELD_STOCK_5_20 = 20;
    private static final String KEYWORD_FIELD_CATEGORY_1_DRAMA = "Drama";
    private static final String KEYWORD_FIELD_CATEGORY_2_ACTION = "Action";
    private static final String KEYWORD_FIELD_CATEGORY_3_SCI_FI = "Sci-fi";
    private static final int SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER = 1;
    private static final int SHARDS_COUNT_IN_MULTI_NODE_CLUSTER = 3;
    private static final int LTE_OF_RANGE_IN_HYBRID_QUERY = 400;
    private static final int GTE_OF_RANGE_IN_HYBRID_QUERY = 20;
    private static final int LARGEST_STOCK_VALUE_IN_QUERY_RESULT = 400;
    private static final int LARGEST_STOCK_VALUE_IN_SEARCH_AFTER_MULTIPLE_FIELD_QUERY_RESULT = 25;
    private static final int LARGEST_STOCK_VALUE_IN_SEARCH_AFTER_SINGLE_FIELD_QUERY_RESULT = 22;
    private static final Float MIN_SCORE = 0.6f;

    @BeforeClass
    @SneakyThrows
    public static void setUpCluster() {
        // we need new instance because we're calling non-static methods from static method.
        // main purpose is to minimize network calls, initialization is only needed once
        HybridQuerySortIT instance = new HybridQuerySortIT();
        instance.initClient();
        instance.updateClusterSettings();
    }

    @SneakyThrows
    public void testSortOnSingleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testSingleFieldSortNotOnScoreField_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testSortOnSingleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testSingleFieldSortNotOnScoreField_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testScoreSort_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testSortOnMultipleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testScoreSort_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testSortOnMultipleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testScoreSort_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    private void testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );

        Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            null,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertStockValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, LARGEST_STOCK_VALUE_IN_QUERY_RESULT, true, true);
    }

    @SneakyThrows
    private void testSingleFieldSortNotOnScoreField_whenMultipleSubQueries_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );

        Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
        fieldSortOrderMap.put("category", SortOrder.DESC);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            null,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertNullScoreWithSortOrderInHybridQueryResults(nestedHits);
    }

    @SneakyThrows
    private void testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );

        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        fieldSortOrderMap.put("_doc", SortOrder.ASC);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            null,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertStockValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, LARGEST_STOCK_VALUE_IN_QUERY_RESULT, true, false);
        assertDocValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.ASC, 0, false, false);
    }

    @SneakyThrows
    public void testSingleFieldSort_whenTrackScoresIsEnabled_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        assertThrows(
            "Hybrid search results when sorted by any field, docId or _id, track_scores must be set to false.",
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                createSortBuilders(fieldSortOrderMap, false),
                true,
                null,
                0,
                null
            )
        );
    }

    @SneakyThrows
    public void testSingleFieldSort_whenSortCriteriaIsByScoreAndField_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        fieldSortOrderMap.put("_score", SortOrder.DESC);
        assertThrows(
            "_score must be the primary sort key when combined with a field in a hybrid query",
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                createSortBuilders(fieldSortOrderMap, false),
                true,
                null,
                0,
                null
            )
        );
    }

    @SneakyThrows
    public void testSort_whenSortByScoreAscending_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> searchSortedByScore("[{\"_score\":{\"order\":\"asc\"}}]", null)
        );

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody, responseBody.contains("Sorting a hybrid query by _score in ascending order is not supported"));
    }

    @SneakyThrows
    public void testSort_whenSortByScoreMoreThanOnce_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> searchSortedByScore("[{\"_score\":{\"order\":\"asc\"}},{\"_score\":{\"order\":\"desc\"}}]", null)
        );

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody, responseBody.contains("Sorting a hybrid query by _score more than once is not supported"));
    }

    @SneakyThrows
    public void testSort_whenSortByScoreAscendingWithCollapse_thenSuccessful() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);

        Map<String, Object> response = searchSortedByScore(
            "[{\"_score\":{\"order\":\"asc\"}}]",
            "{\"field\":\"" + KEYWORD_FIELD_2_CATEGORY + "\"}"
        );

        assertEquals(0, ((Map<?, ?>) response.get("_shards")).get("failed"));
        List<Map<String, Object>> hits = getNestedHits(response);
        // one group per category among the matching documents
        assertEquals(3, hits.size());
        double previousScore = Double.NEGATIVE_INFINITY;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            assertTrue("_score must be a normalized combined score, got " + score, score <= 1.0);
            assertTrue("collapsed groups must be sorted by ascending _score", score >= previousScore);
            previousScore = score;
        }
    }

    @SneakyThrows
    public void testSort_whenSortByScoreMoreThanOnceWithCollapse_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> searchSortedByScore(
                "[{\"_score\":{\"order\":\"asc\"}},{\"_score\":{\"order\":\"desc\"}}]",
                "{\"field\":\"" + KEYWORD_FIELD_2_CATEGORY + "\"}"
            )
        );

        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
        assertTrue(responseBody, responseBody.contains("Sorting a hybrid query by _score more than once is not supported"));
    }

    @SneakyThrows
    public void testScoreSortWithTieBreaker_whenMultipleShardsAndConcurrentSearchEnabled_thenHitsCarrySortValuesInCombinedScoreOrder() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        assertScoreSortWithTieBreaker(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testScoreSortWithTieBreaker_whenMultipleShardsAndConcurrentSearchDisabled_thenHitsCarrySortValuesInCombinedScoreOrder() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        assertScoreSortWithTieBreaker(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testScoreSortWithTieBreaker_whenSingleShardAndConcurrentSearchEnabled_thenHitsCarrySortValuesInCombinedScoreOrder() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        assertScoreSortWithTieBreaker(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testScoreSortWithTieBreaker_whenSingleShardAndConcurrentSearchDisabled_thenHitsCarrySortValuesInCombinedScoreOrder() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        assertScoreSortWithTieBreaker(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    /**
     * [_score desc, _id asc] on a hybrid query: the hits come in the combined-score order of the same query without a sort
     * (equal scores broken by _id), every hit carries sort values, and sort[0] is the combined score the hit reports.
     */
    private void assertScoreSortWithTieBreaker(final String indexName) throws Exception {
        Map<String, Object> reference = searchHybrid(indexName, "{\"size\":10,\"query\":" + hybridQueryJson(null) + "}");
        List<Map<String, Object>> referenceHits = getNestedHits(reference);
        assertEquals(6, referenceHits.size());

        // the sort by _score and a field states its window; the reference without a sort collects a window of size 10 too
        Map<String, Object> response = searchHybrid(
            indexName,
            "{\"size\":10,\"query\":" + hybridQueryJson(10) + ",\"sort\":" + SCORE_DESC_ID_ASC_SORT + "}"
        );
        assertEquals(0, ((Map<?, ?>) response.get("_shards")).get("failed"));
        List<Map<String, Object>> hits = getNestedHits(response);
        assertEquals(referenceHits.size(), hits.size());

        double previousScore = Double.POSITIVE_INFINITY;
        String previousId = null;
        for (int i = 0; i < hits.size(); i++) {
            Map<String, Object> hit = hits.get(i);
            double score = ((Number) hit.get("_score")).doubleValue();
            List<?> sortValues = (List<?>) hit.get("sort");
            assertNotNull("every hit carries sort values", sortValues);
            assertEquals(2, sortValues.size());
            assertEquals("sort[0] is the combined score", score, ((Number) sortValues.get(0)).doubleValue(), DELTA_FOR_SCORE_ASSERTION);
            assertEquals("sort[1] is the tie-breaker", hit.get("_id"), sortValues.get(1));
            assertEquals(
                "the ranking is the one of the query without a sort",
                ((Number) referenceHits.get(i).get("_score")).doubleValue(),
                score,
                DELTA_FOR_SCORE_ASSERTION
            );
            assertTrue("scores descend", score <= previousScore);
            if (score == previousScore) {
                assertTrue("equal scores are broken by ascending _id", ((String) hit.get("_id")).compareTo(previousId) > 0);
            }
            previousScore = score;
            previousId = (String) hit.get("_id");
        }
        Object maxScore = ((Map<?, ?>) response.get("hits")).get("max_score");
        assertEquals(((Number) hits.get(0).get("_score")).doubleValue(), ((Number) maxScore).doubleValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testSearchAfterWithScoreSort_whenMultipleShardsAndConcurrentSearchEnabled_thenPagesWalkTheRankingExactly() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        assertSearchAfterWalksTheRanking(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testSearchAfterWithScoreSort_whenMultipleShardsAndConcurrentSearchDisabled_thenPagesWalkTheRankingExactly() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        assertSearchAfterWalksTheRanking(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testSearchAfterWithScoreSort_whenSingleShardAndConcurrentSearchEnabled_thenPagesWalkTheRankingExactly() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        assertSearchAfterWalksTheRanking(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testSearchAfterWithScoreSort_whenSingleShardAndConcurrentSearchDisabled_thenPagesWalkTheRankingExactly() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        assertSearchAfterWalksTheRanking(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    /**
     * Pages of two, each requested with the previous page's last sort values as search_after and a constant pagination_depth,
     * concatenate to exactly the single page of the same request: same documents, same order, same scores, no duplicates, and
     * the cursor of the last document returns an empty page.
     */
    private void assertSearchAfterWalksTheRanking(final String indexName) throws Exception {
        String query = hybridQueryJson(10);
        List<Map<String, Object>> expected = getNestedHits(
            searchHybrid(indexName, "{\"size\":10,\"query\":" + query + ",\"sort\":" + SCORE_DESC_ID_ASC_SORT + "}")
        );
        assertEquals(6, expected.size());

        List<Map<String, Object>> walked = new ArrayList<>();
        List<?> cursor = null;
        for (int page = 0; page < 4; page++) {
            String body = "{\"size\":2,\"query\":"
                + query
                + ",\"sort\":"
                + SCORE_DESC_ID_ASC_SORT
                + (cursor == null ? "" : ",\"search_after\":" + toJsonArray(cursor))
                + "}";
            List<Map<String, Object>> hits = getNestedHits(searchHybrid(indexName, body));
            if (hits.isEmpty()) {
                break;
            }
            walked.addAll(hits);
            cursor = (List<?>) hits.get(hits.size() - 1).get("sort");
        }
        assertEquals(expected.size(), walked.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("document at position " + i, expected.get(i).get("_id"), walked.get(i).get("_id"));
            assertEquals(
                "score at position " + i,
                ((Number) expected.get(i).get("_score")).doubleValue(),
                ((Number) walked.get(i).get("_score")).doubleValue(),
                DELTA_FOR_SCORE_ASSERTION
            );
            assertEquals(expected.get(i).get("sort"), walked.get(i).get("sort"));
        }
        // the cursor of the last document: nothing is after it
        List<?> lastCursor = (List<?>) expected.get(expected.size() - 1).get("sort");
        List<Map<String, Object>> pastTheEnd = getNestedHits(
            searchHybrid(
                indexName,
                "{\"size\":2,\"query\":"
                    + query
                    + ",\"sort\":"
                    + SCORE_DESC_ID_ASC_SORT
                    + ",\"search_after\":"
                    + toJsonArray(lastCursor)
                    + "}"
            )
        );
        assertTrue(pastTheEnd.isEmpty());
    }

    @SneakyThrows
    public void testSearchAfterWithScoreSort_whenPaginationDepthMissing_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        // a cursor page, and the first page of a walk, which carries no cursor: a cursor is only valid on a page computed over
        // the window of the page it was taken from, so the window is required on both
        for (String cursor : new String[] { ",\"search_after\":[0.5,\"3\"]", "" }) {
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> searchHybrid(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    "{\"size\":2,\"query\":" + hybridQueryJson(null) + ",\"sort\":" + SCORE_DESC_ID_ASC_SORT + cursor + "}"
                )
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
            String responseBody = EntityUtils.toString(exception.getResponse().getEntity());
            assertTrue(
                responseBody,
                responseBody.contains(
                    "pagination_depth param is missing in the search request; it is required when a hybrid query is sorted by _score "
                        + "and a field"
                )
            );
        }
    }

    /**
     * The document with the best combined score is ranked second by every sub-query: doc 3 matches both, docs 1 and 4 match
     * one each and rank first there. The page, and a search_after walk over it, follow the combined score; the per-sub-query
     * lists are not runs of that order. All documents are routed to one shard, so that one shard holds both lists, also when
     * the index has several.
     */
    @SneakyThrows
    public void testScoreSortWithTieBreaker_whenBestCombinedScoreIsSecondInEverySubQuery_thenPagesFollowTheCombinedScore() {
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        for (int numberOfShards : new int[] { SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER, SHARDS_COUNT_IN_MULTI_NODE_CLUSTER }) {
            String indexName = "test-hybrid-score-sort-combined-order-" + numberOfShards;
            createIndexWithTieBreakerField(indexName, numberOfShards, "integer");
            indexTieBreakerDocuments(indexName, 0);

            List<Map<String, Object>> hits = getNestedHits(searchHybrid(indexName, tieBreakerSearchBody(10, null)));
            assertEquals(List.of("3", "1", "4", "5", "6"), hits.stream().map(hit -> hit.get("_id")).toList());
            assertCombinedScoreOrder(hits);

            List<Map<String, Object>> walked = walkWithSearchAfter(indexName, 1);
            assertEquals(List.of("3", "1", "4", "5", "6"), walked.stream().map(hit -> hit.get("_id")).toList());
        }
    }

    /**
     * Two indices behind one search map the tie-breaker as integer and long, and hold the same documents, so every document
     * has a twin with the same combined score in the other index. The cursor is typed per shard and core widens the field
     * when it merges the shards' pages: the walk is the single page, with no error.
     */
    @SneakyThrows
    public void testSearchAfterWithScoreSort_whenIndicesMapTieBreakerAsIntegerAndLong_thenPagesWalkTheRankingExactly() {
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        createIndexWithTieBreakerField(TIE_BREAKER_INDEX_INTEGER, SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER, "integer");
        createIndexWithTieBreakerField(TIE_BREAKER_INDEX_LONG, SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER, "long");
        indexTieBreakerDocuments(TIE_BREAKER_INDEX_INTEGER, 0);
        indexTieBreakerDocuments(TIE_BREAKER_INDEX_LONG, 100);
        String indices = TIE_BREAKER_INDEX_INTEGER + "," + TIE_BREAKER_INDEX_LONG;

        List<Map<String, Object>> expected = getNestedHits(searchHybrid(indices, tieBreakerSearchBody(10, null)));
        // twins tie on the combined score and are broken by seq: the integer index's document (seq n) before the long one's
        assertEquals(
            List.of(3, 103, 1, 4, 101, 104, 5, 6, 105, 106),
            expected.stream().map(hit -> ((Number) ((List<?>) hit.get("sort")).get(1)).intValue()).toList()
        );
        assertCombinedScoreOrder(expected);

        List<Map<String, Object>> walked = walkWithSearchAfter(indices, 3);
        assertEquals(expected.size(), walked.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(
                "document at position " + i,
                expected.get(i).get("_index") + "/" + expected.get(i).get("_id"),
                walked.get(i).get("_index") + "/" + walked.get(i).get("_id")
            );
            assertEquals("sort values at position " + i, expected.get(i).get("sort"), walked.get(i).get("sort"));
        }
    }

    /**
     * A field sort over two indices that map the field as integer and long: core can only widen the field when it merges the
     * shards' pages if every page keeps the sort fields its shard collected with.
     */
    @SneakyThrows
    public void testFieldSort_whenIndicesMapFieldAsIntegerAndLong_thenShardPagesAreMerged() {
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        createIndexWithTieBreakerField(TIE_BREAKER_INDEX_INTEGER, SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER, "integer");
        createIndexWithTieBreakerField(TIE_BREAKER_INDEX_LONG, SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER, "long");
        indexTieBreakerDocuments(TIE_BREAKER_INDEX_INTEGER, 0);
        indexTieBreakerDocuments(TIE_BREAKER_INDEX_LONG, 100);

        List<Map<String, Object>> hits = getNestedHits(
            searchHybrid(
                TIE_BREAKER_INDEX_INTEGER + "," + TIE_BREAKER_INDEX_LONG,
                "{\"size\":10,\"query\":" + TIE_BREAKER_HYBRID_QUERY + ",\"sort\":[{\"seq\":{\"order\":\"desc\"}}]}"
            )
        );
        assertEquals(
            List.of(106, 105, 104, 103, 101, 6, 5, 4, 3, 1),
            hits.stream().map(hit -> ((Number) ((List<?>) hit.get("sort")).get(0)).intValue()).toList()
        );
    }

    private static final String TIE_BREAKER_INDEX_INTEGER = "test-hybrid-score-sort-tie-breaker-integer";
    private static final String TIE_BREAKER_INDEX_LONG = "test-hybrid-score-sort-tie-breaker-long";
    private static final String TIE_BREAKER_HYBRID_QUERY =
        "{\"hybrid\":{\"pagination_depth\":10,\"queries\":[{\"match\":{\"title\":\"alpha\"}},{\"match\":{\"title\":\"beta\"}}]}}";
    private static final String SCORE_DESC_SEQ_ASC_SORT = "[{\"_score\":{\"order\":\"desc\"}},{\"seq\":{\"order\":\"asc\"}}]";

    @SneakyThrows
    private void createIndexWithTieBreakerField(final String indexName, final int numberOfShards, final String seqType) {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(
            String.format(
                Locale.ROOT,
                "{\"settings\":{\"number_of_shards\":%d,\"number_of_replicas\":0},"
                    + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"},\"seq\":{\"type\":\"%s\"}}}}",
                numberOfShards,
                seqType
            )
        );
        assertEquals(RestStatus.OK, RestStatus.fromCode(client().performRequest(request).getStatusLine().getStatusCode()));
    }

    /**
     * Five documents, all routed to one shard. BM25 over "title" ranks a shorter document higher, so after min_max each
     * sub-query has its best document at 1.0, doc 3 in the middle and its weakest document at the floor:
     * alpha: doc 1 (1.0), doc 3, doc 5; beta: doc 4 (1.0), doc 3, doc 6. Doc 3's two normalized scores average above 0.5,
     * which is where docs 1 and 4 land, so the combined order is 3, then 1 and 4 by seq, then 5 and 6 by seq.
     */
    @SneakyThrows
    private void indexTieBreakerDocuments(final String indexName, final int seqOffset) {
        String[][] documents = new String[][] {
            { "1", "alpha" },
            { "3", "alpha beta w" },
            { "4", "beta" },
            { "5", "alpha w w w w w w w w" },
            { "6", "beta w w w w w w w w" } };
        StringBuilder bulk = new StringBuilder();
        for (String[] document : documents) {
            bulk.append(
                String.format(Locale.ROOT, "{\"index\":{\"_index\":\"%s\",\"_id\":\"%s\",\"routing\":\"r\"}}%n", indexName, document[0])
            );
            bulk.append(
                String.format(Locale.ROOT, "{\"title\":\"%s\",\"seq\":%d}%n", document[1], Integer.parseInt(document[0]) + seqOffset)
            );
        }
        Request request = new Request("POST", "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        assertFalse(EntityUtils.toString(response.getEntity()).contains("\"errors\":true"));
    }

    private String tieBreakerSearchBody(final int size, final List<?> searchAfter) {
        return "{\"size\":"
            + size
            + ",\"query\":"
            + TIE_BREAKER_HYBRID_QUERY
            + ",\"sort\":"
            + SCORE_DESC_SEQ_ASC_SORT
            + (searchAfter == null ? "" : ",\"search_after\":" + toJsonArray(searchAfter))
            + "}";
    }

    /** Pages of {@code size}, each requested with the previous page's last sort values, until a page comes back empty. */
    private List<Map<String, Object>> walkWithSearchAfter(final String indices, final int size) {
        List<Map<String, Object>> walked = new ArrayList<>();
        List<?> cursor = null;
        for (int page = 0; page < 20; page++) {
            List<Map<String, Object>> hits = getNestedHits(searchHybrid(indices, tieBreakerSearchBody(size, cursor)));
            if (hits.isEmpty()) {
                return walked;
            }
            walked.addAll(hits);
            cursor = (List<?>) hits.get(hits.size() - 1).get("sort");
        }
        fail("the walk did not end");
        return walked;
    }

    /** Scores never increase, sort[0] is the hit's combined score, and equal scores are broken by ascending seq (sort[1]). */
    private void assertCombinedScoreOrder(final List<Map<String, Object>> hits) {
        double previousScore = Double.POSITIVE_INFINITY;
        long previousSeq = Long.MIN_VALUE;
        for (Map<String, Object> hit : hits) {
            double score = ((Number) hit.get("_score")).doubleValue();
            List<?> sortValues = (List<?>) hit.get("sort");
            assertEquals(score, ((Number) sortValues.get(0)).doubleValue(), DELTA_FOR_SCORE_ASSERTION);
            long seq = ((Number) sortValues.get(1)).longValue();
            assertTrue("scores descend", score <= previousScore);
            if (score == previousScore) {
                assertTrue("equal scores are broken by ascending seq", seq > previousSeq);
            }
            previousScore = score;
            previousSeq = seq;
        }
    }

    private static final String SCORE_DESC_ID_ASC_SORT = "[{\"_score\":{\"order\":\"desc\"}},{\"_id\":{\"order\":\"asc\"}}]";

    private String hybridQueryJson(final Integer paginationDepth) {
        return String.format(
            Locale.ROOT,
            "{\"hybrid\":{%s\"queries\":[{\"match\":{\"%s\":\"mission\"}},{\"term\":{\"%s\":\"part\"}},"
                + "{\"range\":{\"%s\":{\"gte\":%d,\"lte\":%d}}}]}}",
            paginationDepth == null ? "" : "\"pagination_depth\":" + paginationDepth + ",",
            TEXT_FIELD_1_NAME,
            TEXT_FIELD_1_NAME,
            INTEGER_FIELD_1_STOCK,
            GTE_OF_RANGE_IN_HYBRID_QUERY,
            LTE_OF_RANGE_IN_HYBRID_QUERY
        );
    }

    private static String toJsonArray(final List<?> values) {
        return values.stream()
            .map(value -> value instanceof String ? "\"" + value + "\"" : String.valueOf(value))
            .collect(java.util.stream.Collectors.joining(",", "[", "]"));
    }

    @SneakyThrows
    private Map<String, Object> searchHybrid(final String indexName, final String body) {
        Request request = new Request("POST", "/" + indexName + "/_search");
        request.addParameter("search_pipeline", SEARCH_PIPELINE);
        // a failed shard has to fail the request instead of being reported as a partial result
        request.addParameter("allow_partial_search_results", Boolean.FALSE.toString());
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SneakyThrows
    private Map<String, Object> searchSortedByScore(final String sort, final String collapse) {
        Request request = new Request("POST", "/" + TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS + "/_search");
        request.addParameter("search_pipeline", SEARCH_PIPELINE);
        // a failed shard has to fail the request instead of being reported as a partial result
        request.addParameter("allow_partial_search_results", Boolean.FALSE.toString());
        String query = String.format(
            Locale.ROOT,
            "{\"hybrid\":{\"queries\":[{\"match\":{\"%s\":\"mission\"}},{\"term\":{\"%s\":\"part\"}},"
                + "{\"range\":{\"%s\":{\"gte\":%d,\"lte\":%d}}}]}}",
            TEXT_FIELD_1_NAME,
            TEXT_FIELD_1_NAME,
            INTEGER_FIELD_1_STOCK,
            GTE_OF_RANGE_IN_HYBRID_QUERY,
            LTE_OF_RANGE_IN_HYBRID_QUERY
        );
        request.setJsonEntity(
            "{\"size\":10,\"query\":" + query + ",\"sort\":" + sort + (collapse == null ? "" : ",\"collapse\":" + collapse) + "}"
        );
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
    }

    @SneakyThrows
    public void testSingleFieldSort_whenMinScoreIsSet_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        assertThrows(
            "Hybrid search results when sorted by any field, docId or _id, min_score cannot be set.",
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                createSortBuilders(fieldSortOrderMap, false),
                false,
                null,
                0,
                null,
                null,
                null,
                null,
                null,
                null,
                MIN_SCORE
            )
        );
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnSingleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnSingleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnMultipleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnMultipleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    private void testSearchAfter_whenSingleFieldSort_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        List<Object> searchAfter = new ArrayList<>();
        searchAfter.add(25);
        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            searchAfter,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 3, 6);
        assertStockValueWithSortOrderInHybridQueryResults(
            nestedHits,
            SortOrder.DESC,
            LARGEST_STOCK_VALUE_IN_SEARCH_AFTER_SINGLE_FIELD_QUERY_RESULT,
            true,
            true
        );
    }

    @SneakyThrows
    private void testSearchAfter_whenMultipleFieldSort_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        fieldSortOrderMap.put("_doc", SortOrder.DESC);
        List<Object> searchAfter = new ArrayList<>();
        searchAfter.add(25);
        searchAfter.add(4);
        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            searchAfter,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 5, 6);
        assertStockValueWithSortOrderInHybridQueryResults(
            nestedHits,
            SortOrder.DESC,
            LARGEST_STOCK_VALUE_IN_SEARCH_AFTER_MULTIPLE_FIELD_QUERY_RESULT,
            true,
            false
        );
        assertDocValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, 0, false, false);
    }

    @SneakyThrows
    private void testScoreSort_whenSingleFieldSort_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("_score", SortOrder.DESC);
        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            null,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertScoreWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, 1.0);
    }

    @SneakyThrows
    public void testSort_whenSortFieldsSizeNotEqualToSearchAfterSize_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        List<Object> searchAfter = new ArrayList<>();
        searchAfter.add(25);
        searchAfter.add(0);
        assertThrows(
            "after.fields has 2 values but sort has 1",
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                createSortBuilders(fieldSortOrderMap, false),
                true,
                searchAfter,
                0,
                null
            )
        );
    }

    @SneakyThrows
    public void testSearchAfter_whenAfterFieldIsNotPassed_thenFail() {
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        Map<String, SortOrder> fieldSortOrderMap = new LinkedHashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);
        List<Object> searchAfter = new ArrayList<>();
        searchAfter.add(null);
        assertThrows(
            "after.fields wasn't set; you must pass fillFields=true for the previous search",
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                createSortBuilders(fieldSortOrderMap, false),
                true,
                searchAfter,
                0,
                null
            )
        );
    }

    @SneakyThrows
    public void testSortingWithRescoreWhenConcurrentSegmentSearchEnabledAndDisabled_whenBothSortAndRescorePresent_thenFail() {
        try {
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
            HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
                "mission",
                "part",
                LTE_OF_RANGE_IN_HYBRID_QUERY,
                GTE_OF_RANGE_IN_HYBRID_QUERY
            );

            Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
            fieldSortOrderMap.put("stock", SortOrder.DESC);

            List<Object> searchAfter = new ArrayList<>();
            searchAfter.add(25);

            QueryBuilder rescoreQuery = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, TEXT_FIELD_VALUE_1_DUNES);

            assertThrows(
                "Cannot use [sort] option in conjunction with [rescore].",
                ResponseException.class,
                () -> search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    hybridQueryBuilder,
                    rescoreQuery,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    null,
                    createSortBuilders(fieldSortOrderMap, false),
                    false,
                    searchAfter,
                    0,
                    null
                )
            );

            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);

            assertThrows(
                "Cannot use [sort] option in conjunction with [rescore].",
                ResponseException.class,
                () -> search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    hybridQueryBuilder,
                    rescoreQuery,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    null,
                    createSortBuilders(fieldSortOrderMap, false),
                    false,
                    searchAfter,
                    0,
                    null
                )
            );
        } finally {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        }
    }

    @SneakyThrows
    public void testExplainAndSort_whenIndexWithMultipleShards_thenSuccessful() {
        // Setup
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);

        initializeIndexIfNotExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        createSearchPipeline(SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, Map.of(), DEFAULT_COMBINATION_METHOD, Map.of(), true);
        // Assert
        // scores for search hits
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );

        Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
        fieldSortOrderMap.put("stock", SortOrder.DESC);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE, "explain", Boolean.TRUE.toString()),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            null,
            0,
            null
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertStockValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, LARGEST_STOCK_VALUE_IN_QUERY_RESULT, true, true);

        // explain
        Map<String, Object> searchHit1 = nestedHits.get(0);
        Map<String, Object> explanationForHit1 = (Map<String, Object>) searchHit1.get("_explanation");
        assertNotNull(explanationForHit1);
        assertNull(searchHit1.get("_score"));
        String expectedGeneralCombineScoreDescription = "arithmetic_mean combination of:";
        assertEquals(expectedGeneralCombineScoreDescription, explanationForHit1.get("description"));
        List<Map<String, Object>> hit1Details = getListOfValues(explanationForHit1, "details");
        assertEquals(2, hit1Details.size());
        Map<String, Object> hit1DetailsForHit1 = hit1Details.get(0);
        assertEquals(1.0, hit1DetailsForHit1.get("value"));
        assertEquals("min_max normalization of:", hit1DetailsForHit1.get("description"));
        List<Map<String, Object>> hit1DetailsForHit1Details = getListOfValues(hit1DetailsForHit1, "details");
        assertEquals(1, hit1DetailsForHit1Details.size());

        Map<String, Object> hit1DetailsForHit1DetailsForHit1 = hit1DetailsForHit1Details.get(0);
        assertEquals("weight(name:mission in 0) [PerFieldSimilarity], result of:", hit1DetailsForHit1DetailsForHit1.get("description"));
        assertTrue((double) hit1DetailsForHit1DetailsForHit1.get("value") > 0.0f);
        assertEquals(1, getListOfValues(hit1DetailsForHit1DetailsForHit1, "details").size());

        Map<String, Object> hit1DetailsForHit1DetailsForHit1DetailsForHit1 = getListOfValues(hit1DetailsForHit1DetailsForHit1, "details")
            .get(0);
        assertEquals(
            "score(freq=1.0), computed as boost * idf * tf from:",
            hit1DetailsForHit1DetailsForHit1DetailsForHit1.get("description")
        );
        assertTrue((double) hit1DetailsForHit1DetailsForHit1DetailsForHit1.get("value") > 0.0f);
        assertEquals(2, getListOfValues(hit1DetailsForHit1DetailsForHit1DetailsForHit1, "details").size());

        assertEquals(
            "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
            getListOfValues(hit1DetailsForHit1DetailsForHit1DetailsForHit1, "details").get(0).get("description")
        );
        assertTrue((double) getListOfValues(hit1DetailsForHit1DetailsForHit1DetailsForHit1, "details").get(0).get("value") > 0.0f);
        assertEquals(
            "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
            getListOfValues(hit1DetailsForHit1DetailsForHit1DetailsForHit1, "details").get(1).get("description")
        );
        assertTrue((double) getListOfValues(hit1DetailsForHit1DetailsForHit1DetailsForHit1, "details").get(1).get("value") > 0.0f);

        // hit 4
        Map<String, Object> searchHit4 = nestedHits.get(3);
        Map<String, Object> explanationForHit4 = (Map<String, Object>) searchHit4.get("_explanation");
        assertNotNull(explanationForHit4);
        assertNull(searchHit4.get("_score"));
        assertEquals(expectedGeneralCombineScoreDescription, explanationForHit4.get("description"));
        List<Map<String, Object>> hit4Details = getListOfValues(explanationForHit4, "details");
        assertEquals(2, hit4Details.size());
        Map<String, Object> hit1DetailsForHit4 = hit4Details.get(0);
        assertEquals(1.0, hit1DetailsForHit4.get("value"));
        assertEquals("min_max normalization of:", hit1DetailsForHit4.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit4.get("details")).size());
        List<Map<String, Object>> hit1DetailsForHit4Details = getListOfValues(hit1DetailsForHit4, "details");
        assertEquals(1, hit1DetailsForHit4Details.size());

        Map<String, Object> hit1DetailsForHit1DetailsForHit4 = hit1DetailsForHit4Details.get(0);
        assertEquals("weight(name:part in 0) [PerFieldSimilarity], result of:", hit1DetailsForHit1DetailsForHit4.get("description"));
        assertTrue((double) hit1DetailsForHit1DetailsForHit4.get("value") > 0.0f);
        assertEquals(1, getListOfValues(hit1DetailsForHit1DetailsForHit4, "details").size());

        Map<String, Object> hit1DetailsForHit1DetailsForHit1DetailsForHit4 = getListOfValues(hit1DetailsForHit1DetailsForHit4, "details")
            .get(0);
        assertEquals(
            "score(freq=1.0), computed as boost * idf * tf from:",
            hit1DetailsForHit1DetailsForHit1DetailsForHit4.get("description")
        );
        assertTrue((double) hit1DetailsForHit1DetailsForHit1DetailsForHit4.get("value") > 0.0f);
        assertEquals(2, getListOfValues(hit1DetailsForHit1DetailsForHit1DetailsForHit4, "details").size());

        // hit 6
        Map<String, Object> searchHit6 = nestedHits.get(5);
        Map<String, Object> explanationForHit6 = (Map<String, Object>) searchHit6.get("_explanation");
        assertNotNull(explanationForHit6);
        assertNull(searchHit6.get("_score"));
        assertEquals(expectedGeneralCombineScoreDescription, explanationForHit6.get("description"));
        List<Map<String, Object>> hit6Details = getListOfValues(explanationForHit6, "details");
        assertEquals(1, hit6Details.size());
        Map<String, Object> hit1DetailsForHit6 = hit6Details.get(0);
        assertEquals(1.0, hit1DetailsForHit6.get("value"));
        assertEquals("min_max normalization of:", hit1DetailsForHit6.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit6.get("details")).size());
        List<Map<String, Object>> hit1DetailsForHit6Details = getListOfValues(hit1DetailsForHit6, "details");
        assertEquals(1, hit1DetailsForHit6Details.size());

        Map<String, Object> hit1DetailsForHit1DetailsForHit6 = hit1DetailsForHit6Details.get(0);
        assertEquals("weight(name:part in 0) [PerFieldSimilarity], result of:", hit1DetailsForHit1DetailsForHit4.get("description"));
        assertTrue((double) hit1DetailsForHit1DetailsForHit6.get("value") > 0.0f);
        assertEquals(0, getListOfValues(hit1DetailsForHit1DetailsForHit6, "details").size());
    }

    @SneakyThrows
    public void testMinScoreAndSortScore_thenSuccessful() {
        Map<String, SortOrder> fieldSortOrderMap = new HashMap<>();
        fieldSortOrderMap.put("_score", SortOrder.DESC);

        testMinScoreAndSort_thenSuccessful(fieldSortOrderMap, SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER, MIN_SCORE, 4);

        // For single shard, the expected hit count is 4, but for multiple shards, the expected hit count is 3.
        // This is because doc1["name"="Dunes part 2"] and doc2["name"="Dunes part 1"] locate in two shards and have the different scores.
        // so after normalization, one score of them will be 1.0f, the other one will be 0.001f (In single shard, they are both 1.0f).
        testMinScoreAndSort_thenSuccessful(fieldSortOrderMap, SHARDS_COUNT_IN_MULTI_NODE_CLUSTER, MIN_SCORE, 3);
    }

    @SneakyThrows
    private void testMinScoreAndSort_thenSuccessful(
        final Map<String, SortOrder> fieldSortOrderMap,
        final int numShards,
        final Float minScore,
        final int expectedHitsCount
    ) {
        // Setup
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);

        String targetIndex = numShards == SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER
            ? TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD
            : TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS;

        prepareResourcesBeforeTestExecution(numShards);
        // Assert
        // scores for search hits
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );

        Map<String, Object> searchResponseAsMap = search(
            targetIndex,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            createSortBuilders(fieldSortOrderMap, false),
            false,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            minScore
        );

        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(
            searchResponseAsMap,
            expectedHitsCount,
            expectedHitsCount
        );
        assertScoreWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, Double.MAX_VALUE, minScore);
    }

    private HybridQueryBuilder createHybridQueryBuilderWithMatchTermAndRangeQuery(String text, String value, int lte, int gte) {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, text);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEXT_FIELD_1_NAME, value);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder).add(termQueryBuilder).add(rangeQueryBuilder);
        return hybridQueryBuilder;
    }

    private void assertStockValueWithSortOrderInHybridQueryResults(
        List<Map<String, Object>> hitsNestedList,
        SortOrder sortOrder,
        int baseStockValue,
        boolean isPrimarySortField,
        boolean isSingleFieldSort
    ) {
        for (Map<String, Object> oneHit : hitsNestedList) {
            assertNotNull(oneHit.get("_source"));
            Map<String, Object> source = (Map<String, Object>) oneHit.get("_source");
            List<Object> sorts = (List<Object>) oneHit.get("sort");
            int stock = (int) source.get(INTEGER_FIELD_1_STOCK);
            if (isPrimarySortField) {
                int stockValueInSort = (int) sorts.get(0);
                if (sortOrder == SortOrder.DESC) {
                    assertTrue("Stock value is sorted as per sort order", stock <= baseStockValue);
                } else {
                    assertTrue("Stock value is sorted as per sort order", stock >= baseStockValue);
                }
                assertEquals(stock, stockValueInSort);
            }
            if (!isSingleFieldSort) {
                assertNotNull(sorts.get(1));
                int stockValueInSort = (int) sorts.get(0);
                assertEquals(stock, stockValueInSort);
            }
            baseStockValue = stock;
        }
    }

    private void assertDocValueWithSortOrderInHybridQueryResults(
        List<Map<String, Object>> hitsNestedList,
        SortOrder sortOrder,
        int baseDocIdValue,
        boolean isPrimarySortField,
        boolean isSingleFieldSort
    ) {
        for (Map<String, Object> oneHit : hitsNestedList) {
            assertNotNull(oneHit.get("_source"));
            List<Object> sorts = (List<Object>) oneHit.get("sort");
            if (isPrimarySortField) {
                int docId = (int) sorts.get(0);
                if (sortOrder == SortOrder.DESC) {
                    assertTrue("Doc Id value is sorted as per sort order", docId <= baseDocIdValue);
                } else {
                    assertTrue("Doc Id value is sorted as per sort order", docId >= baseDocIdValue);
                }
                baseDocIdValue = docId;
            }
            if (!isSingleFieldSort) {
                assertNotNull(sorts.get(1));
            }
        }
    }

    private void assertScoreWithSortOrderInHybridQueryResults(
        List<Map<String, Object>> hitsNestedList,
        SortOrder sortOrder,
        double baseScore
    ) {
        assertScoreWithSortOrderInHybridQueryResults(hitsNestedList, sortOrder, baseScore, null);
    }

    private void assertScoreWithSortOrderInHybridQueryResults(
        List<Map<String, Object>> hitsNestedList,
        SortOrder sortOrder,
        double baseScore,
        Float minScore
    ) {
        for (Map<String, Object> oneHit : hitsNestedList) {
            assertNotNull(oneHit.get("_source"));
            double score = (double) oneHit.get("_score");
            if (sortOrder == SortOrder.DESC) {
                assertTrue("Stock value is sorted by descending sort order", score <= baseScore);
            } else {
                assertTrue("Stock value is sorted by ascending sort order", score >= baseScore);
            }
            baseScore = score;

            if (minScore != null) {
                assertTrue("Score is less than min score", score >= minScore);
            }
        }
    }

    private void assertNullScoreWithSortOrderInHybridQueryResults(List<Map<String, Object>> hitsNestedList) {
        for (Map<String, Object> oneHit : hitsNestedList) {
            assertNotNull(oneHit.get("_source"));
            assertEquals(null, oneHit.get("_score"));
        }
    }

    private List<Map<String, Object>> validateHitsCountAndFetchNestedHits(
        Map<String, Object> searchResponseAsMap,
        int collectHitCountExpected,
        int resultsExpected
    ) {
        assertHitResultsFromQueryWhenSortIsEnabled(collectHitCountExpected, resultsExpected, searchResponseAsMap);
        return getNestedHits(searchResponseAsMap);
    }

    @SneakyThrows
    void prepareResourcesBeforeTestExecution(int numShards) {
        if (numShards == 1) {
            initializeIndexIfNotExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, numShards);
        } else {
            initializeIndexIfNotExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, numShards);
        }
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
    }

    @SneakyThrows
    private void initializeIndexIfNotExists(String indexName, int numShards) {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.singletonList(INTEGER_FIELD_1_STOCK),
                    Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                    Collections.emptyList(),
                    numShards
                ),
                ""
            );

            indexTheDocument(
                indexName,
                "1",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_2_DUNES),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.singletonList(INTEGER_FIELD_1_STOCK),
                Collections.singletonList(INTEGER_FIELD_STOCK_1_25),
                Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                Collections.singletonList(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                Collections.emptyList(),
                Collections.emptyList(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "2",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_1_DUNES),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.singletonList(INTEGER_FIELD_1_STOCK),
                Collections.singletonList(INTEGER_FIELD_STOCK_2_22),
                Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                Collections.singletonList(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                Collections.emptyList(),
                Collections.emptyList(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "3",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_3_MI_1),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.singletonList(INTEGER_FIELD_1_STOCK),
                Collections.singletonList(INTEGER_FIELD_STOCK_3_256),
                Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                Collections.singletonList(KEYWORD_FIELD_CATEGORY_2_ACTION),
                Collections.emptyList(),
                Collections.emptyList(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_4_MI_2),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_4_25),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_2_ACTION),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_5_TERMINAL),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_5_20),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_6_AVENGERS),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_5_20),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_3_SCI_FI),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );
        }
    }
}
