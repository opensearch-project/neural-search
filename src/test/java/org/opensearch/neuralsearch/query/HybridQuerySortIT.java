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
import lombok.SneakyThrows;
import org.junit.BeforeClass;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.TestUtils.assertHitResultsFromQueryWhenSortIsEnabled;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.FieldSortBuilder;

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
    private static final int SMALLEST_STOCK_VALUE_IN_QUERY_RESULT = 20;
    private static final int LARGEST_STOCK_VALUE_IN_QUERY_RESULT = 400;
    private static final int LARGEST_STOCK_VALUE_IN_SEARCH_AFTER_MULTIPLE_FIELD_QUERY_RESULT = 25;
    private static final int LARGEST_STOCK_VALUE_IN_SEARCH_AFTER_SINGLE_FIELD_QUERY_RESULT = 22;

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
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
            testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
            testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSortOnSingleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
            testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
            testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
            testScoreSort_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSortOnMultipleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
            testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            testScoreSort_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSortOnMultipleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
            testSingleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            testMultipleFieldSort_whenMultipleSubQueries_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            testScoreSort_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
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
            0
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertStockValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, LARGEST_STOCK_VALUE_IN_QUERY_RESULT, true, true);
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
            0
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertStockValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, LARGEST_STOCK_VALUE_IN_QUERY_RESULT, true, false);
        assertDocValueWithSortOrderInHybridQueryResults(nestedHits, SortOrder.ASC, 0, false, false);
    }

    @SneakyThrows
    public void testSingleFieldSort_whenTrackScoresIsEnabled_thenFail() {
        try {
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
                    0
                )
            );
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSingleFieldSort_whenSortCriteriaIsByScoreAndField_thenFail() {
        try {
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
                "_score sort criteria cannot be applied with any other criteria. Please select one sort criteria out of them.",
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
                    0
                )
            );
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnSingleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
            testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
            testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnSingleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
            testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
            testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnMultipleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
            testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSearchAfterWithSortOnMultipleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
            prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
            testSearchAfter_whenSingleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            testSearchAfter_whenMultipleFieldSort_thenSuccessful(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
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
            0
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
            0
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
            0
        );
        List<Map<String, Object>> nestedHits = validateHitsCountAndFetchNestedHits(searchResponseAsMap, 6, 6);
        assertScoreWithSortOrderInHybridQueryResults(nestedHits, SortOrder.DESC, 1.0);
    }

    @SneakyThrows
    public void testSort_whenSortFieldsSizeNotEqualToSearchAfterSize_thenFail() {
        try {
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
                    0
                )
            );
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testSearchAfter_whenAfterFieldIsNotPassed_thenFail() {
        try {
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
                    0
                )
            );
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
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
                    0
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
                    0
                )
            );
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        }
    }

    private HybridQueryBuilder createHybridQueryBuilderWithMatchTermAndRangeQuery(String text, String value, int lte, int gte) {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, text);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEXT_FIELD_1_NAME, value);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder).add(termQueryBuilder).add(rangeQueryBuilder);
        return hybridQueryBuilder;
    }

    private List<SortBuilder<?>> createSortBuilders(Map<String, SortOrder> fieldSortOrderMap, boolean isSortByScore) {
        List<SortBuilder<?>> sortBuilders = new ArrayList<>();
        if (fieldSortOrderMap != null) {
            for (Map.Entry<String, SortOrder> entry : fieldSortOrderMap.entrySet()) {
                FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(entry.getKey()).order(entry.getValue());
                sortBuilders.add(fieldSortBuilder);
            }
        }

        if (isSortByScore) {
            ScoreSortBuilder scoreSortBuilder = SortBuilders.scoreSort().order(SortOrder.ASC);
            sortBuilders.add(scoreSortBuilder);
        }
        return sortBuilders;
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
        for (Map<String, Object> oneHit : hitsNestedList) {
            assertNotNull(oneHit.get("_source"));
            double score = (double) oneHit.get("_score");
            if (sortOrder == SortOrder.DESC) {
                assertTrue("Stock value is sorted by descending sort order", score <= baseScore);
            } else {
                assertTrue("Stock value is sorted by ascending sort order", score >= baseScore);
            }
            baseScore = score;
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
                    Collections.emptyList(),
                    Collections.singletonList(INTEGER_FIELD_1_STOCK),
                    Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                    Collections.emptyList(),
                    numShards
                ),
                ""
            );

            addKnnDoc(
                indexName,
                "1",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_2_DUNES),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(INTEGER_FIELD_1_STOCK),
                Collections.singletonList(INTEGER_FIELD_STOCK_1_25),
                Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                Collections.singletonList(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                Collections.emptyList(),
                Collections.emptyList()
            );

            addKnnDoc(
                indexName,
                "2",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_1_DUNES),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(INTEGER_FIELD_1_STOCK),
                Collections.singletonList(INTEGER_FIELD_STOCK_2_22),
                Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                Collections.singletonList(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                Collections.emptyList(),
                Collections.emptyList()
            );

            addKnnDoc(
                indexName,
                "3",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_3_MI_1),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(INTEGER_FIELD_1_STOCK),
                Collections.singletonList(INTEGER_FIELD_STOCK_3_256),
                Collections.singletonList(KEYWORD_FIELD_2_CATEGORY),
                Collections.singletonList(KEYWORD_FIELD_CATEGORY_2_ACTION),
                Collections.emptyList(),
                Collections.emptyList()
            );

            addKnnDoc(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_4_MI_2),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_4_25),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_2_ACTION),
                List.of(),
                List.of()
            );

            addKnnDoc(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_5_TERMINAL),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_5_20),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                List.of(),
                List.of()
            );

            addKnnDoc(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_6_AVENGERS),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_5_20),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_3_SCI_FI),
                List.of(),
                List.of()
            );
        }
    }
}
