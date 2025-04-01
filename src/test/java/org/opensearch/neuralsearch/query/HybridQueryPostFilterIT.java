/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.BeforeClass;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import static org.opensearch.neuralsearch.util.TestUtils.assertHitResultsFromQuery;

public class HybridQueryPostFilterIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS =
        "test-hybrid-post-filter-multi-doc-index-multiple-shards";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD =
        "test-hybrid-post-filter-multi-doc-index-single-shard";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-post-filter-pipeline";
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
    private static final String STOCK_AVG_AGGREGATION_NAME = "avg_stock_size";
    private static final int SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER = 1;
    private static final int SHARDS_COUNT_IN_MULTI_NODE_CLUSTER = 3;
    private static final int LTE_OF_RANGE_IN_HYBRID_QUERY = 400;
    private static final int GTE_OF_RANGE_IN_HYBRID_QUERY = 200;
    private static final int LTE_OF_RANGE_IN_POST_FILTER_QUERY = 400;
    private static final int GTE_OF_RANGE_IN_POST_FILTER_QUERY = 230;

    @BeforeClass
    @SneakyThrows
    public static void setUpCluster() {
        // we need new instance because we're calling non-static methods from static method.
        // main purpose is to minimize network calls, initialization is only needed once
        HybridQueryPostFilterIT instance = new HybridQueryPostFilterIT();
        instance.initClient();
        instance.updateClusterSettings();
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithSingleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testPostFilterRangeQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testPostFilterBoolQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testPostFilterMatchAllAndMatchNoneQueries(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithSingleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testPostFilterRangeQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testPostFilterBoolQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
        testPostFilterMatchAllAndMatchNoneQueries(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithMultipleShards_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        testPostFilterRangeQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testPostFilterBoolQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testPostFilterMatchAllAndMatchNoneQueries(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithMultipleShards_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_MULTI_NODE_CLUSTER);
        testPostFilterRangeQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testPostFilterBoolQuery(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
        testPostFilterMatchAllAndMatchNoneQueries(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
    }

    /*{
            "query": {
            "hybrid":{
                "queries":[
                {
                    "match":{
                    "name": "mission"
                }
                },
                {
                    "term":{
                    "name":{
                        "value":"part"
                    }
                }
                },
                {
                    "range": {
                    "stock": {
                        "gte": 200,
                                "lte": 400
                    }
                }
                }
          ]
            }

        },
            "post_filter":{
            "range": {
                "stock": {
                    "gte": 230,
                            "lte": 400
                }
            }
        }
    }*/
    @SneakyThrows
    private void testPostFilterRangeQuery(String indexName) {
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        QueryBuilder postFilterQuery = createQueryBuilderWithRangeQuery(
            LTE_OF_RANGE_IN_POST_FILTER_QUERY,
            GTE_OF_RANGE_IN_POST_FILTER_QUERY
        );

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 1, 0, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);
    }

    /*{
            "query": {
            "hybrid":{
                "queries":[
                {
                    "match":{
                    "name": "mission"
                }
                },
                {
                    "term":{
                    "name":{
                        "value":"part"
                    }
                }
                },
                {
                    "range": {
                    "stock": {
                        "gte": 200,
                                "lte": 400
                    }
                }
                }
          ]
            }

        },
            "aggs": {
            "avg_stock_size": {
                "avg": { "field": "stock" }
            }
        },
            "post_filter":{
            "bool":{
                "should":[
                {
                    "range": {
                    "stock": {
                        "gte": 230,
                                "lte": 400
                    }
                }
                },
                {
                    "match":{
                    "name":"impossible"
                }
                }

           ]
            }
        }
    }*/
    @SneakyThrows
    private void testPostFilterBoolQuery(String indexName) {
        // Case 1 A Query with a combination of hybrid query (Match Query, Term Query, Range Query) and a post filter query (Range and a
        // Match Query).
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        QueryBuilder postFilterQuery = createQueryBuilderWithBoolShouldQuery(
            "impossible",
            LTE_OF_RANGE_IN_POST_FILTER_QUERY,
            GTE_OF_RANGE_IN_POST_FILTER_QUERY
        );

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 2, 1, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);
        // Case 2 A Query with a combination of hybrid query (Match Query, Term Query, Range Query), aggregation (Average stock price
        // `avg_stock_price`) and a post filter query (Range Query and a Match Query).
        AggregationBuilder aggsBuilder = createAvgAggregation();
        searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            List.of(aggsBuilder),
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 2, 1, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);
        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        Map<String, Object> aggValue = getAggregationValues(aggregations, STOCK_AVG_AGGREGATION_NAME);
        assertEquals(1, aggValue.size());
        // Case 3 A Query with a combination of hybrid query (Match Query, Term Query, Range Query) and a post filter query (Bool Query with
        // a must clause(Range Query and a Match Query)).
        postFilterQuery = createQueryBuilderWithBoolMustQuery(
            "terminal",
            LTE_OF_RANGE_IN_POST_FILTER_QUERY,
            GTE_OF_RANGE_IN_POST_FILTER_QUERY
        );
        searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 0, 0, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);
        // Case 4 A Query with a combination of hybrid query (Match Query, Range Query) and a post filter query (Bool Query with a should
        // clause(Range Query and a Match Query)).
        hybridQueryBuilder = createHybridQueryBuilderScenarioWithMatchAndRangeQuery("hero", 5000, 1000);
        postFilterQuery = createQueryBuilderWithBoolShouldQuery(
            "impossible",
            LTE_OF_RANGE_IN_POST_FILTER_QUERY,
            GTE_OF_RANGE_IN_POST_FILTER_QUERY
        );
        searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 0, 0, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);
    }

    /*{
            "query": {
            "hybrid": {
                "queries": [
                {
                    "match": {
                    "name": "mission"
                }
                },
                {
                    "term": {
                    "name": {
                        "value": "part"
                    }
                }
                },
                {
                    "range": {
                    "stock": {
                        "gte": 200,
                                "lte": 400
                    }
                }
                }
            ]
            }
        },
            "post_filter": {
            "match_all": {}
        }
    }*/
    @SneakyThrows
    private void testPostFilterMatchAllAndMatchNoneQueries(String indexName) {
        // CASE 1 A Query with a combination of hybrid query (Match Query, Term Query, Range Query) and a post filter query (Match ALL
        // Query).
        HybridQueryBuilder hybridQueryBuilder = createHybridQueryBuilderWithMatchTermAndRangeQuery(
            "mission",
            "part",
            LTE_OF_RANGE_IN_HYBRID_QUERY,
            GTE_OF_RANGE_IN_HYBRID_QUERY
        );
        QueryBuilder postFilterQuery = createPostFilterQueryBuilderWithMatchAllOrNoneQuery(true);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 4, 3, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);

        // CASE 2 A Query with a combination of hybrid query (Match Query, Term Query, Range Query) and a post filter query (Match NONE
        // Query).
        postFilterQuery = createPostFilterQueryBuilderWithMatchAllOrNoneQuery(false);
        searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            postFilterQuery,
            null,
            false,
            null,
            0
        );
        assertHybridQueryResults(searchResponseAsMap, 0, 0, GTE_OF_RANGE_IN_POST_FILTER_QUERY, LTE_OF_RANGE_IN_POST_FILTER_QUERY);
    }

    private void assertHybridQueryResults(
        Map<String, Object> searchResponseAsMap,
        int resultsExpected,
        int postFilterResultsValidationExpected,
        int lte,
        int gte
    ) {
        assertHitResultsFromQuery(resultsExpected, searchResponseAsMap);
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);

        List<Integer> docIndexes = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            assertNotNull(oneHit.get("_source"));
            Map<String, Object> source = (Map<String, Object>) oneHit.get("_source");
            int docIndex = (int) source.get(INTEGER_FIELD_1_STOCK);
            docIndexes.add(docIndex);
        }
        assertEquals(postFilterResultsValidationExpected, docIndexes.stream().filter(docIndex -> docIndex < lte || docIndex > gte).count());
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
                    List.of(),
                    Map.of(),
                    List.of(INTEGER_FIELD_1_STOCK),
                    List.of(KEYWORD_FIELD_2_CATEGORY),
                    List.of(),
                    numShards
                ),
                ""
            );

            indexTheDocument(
                indexName,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_2_DUNES),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_1_25),
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
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_1_DUNES),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_2_22),
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
                "3",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_3_MI_1),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_3_256),
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

    private HybridQueryBuilder createHybridQueryBuilderWithMatchTermAndRangeQuery(String text, String value, int lte, int gte) {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, text);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEXT_FIELD_1_NAME, value);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder).add(termQueryBuilder).add(rangeQueryBuilder);
        return hybridQueryBuilder;
    }

    private HybridQueryBuilder createHybridQueryBuilderScenarioWithMatchAndRangeQuery(String text, int lte, int gte) {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, text);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder).add(rangeQueryBuilder);
        return hybridQueryBuilder;
    }

    private QueryBuilder createQueryBuilderWithRangeQuery(int lte, int gte) {
        return QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
    }

    private QueryBuilder createQueryBuilderWithBoolShouldQuery(String query, int lte, int gte) {
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        QueryBuilder matchQuery = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, query);
        return QueryBuilders.boolQuery().should(rangeQuery).should(matchQuery);
    }

    private QueryBuilder createQueryBuilderWithBoolMustQuery(String query, int lte, int gte) {
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        QueryBuilder matchQuery = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, query);
        return QueryBuilders.boolQuery().must(rangeQuery).must(matchQuery);
    }

    private QueryBuilder createPostFilterQueryBuilderWithMatchAllOrNoneQuery(boolean isMatchAll) {
        return isMatchAll ? QueryBuilders.matchAllQuery() : new MatchNoneQueryBuilder();
    }

    private AggregationBuilder createAvgAggregation() {
        return AggregationBuilders.avg(STOCK_AVG_AGGREGATION_NAME).field(INTEGER_FIELD_1_STOCK);
    }

}
