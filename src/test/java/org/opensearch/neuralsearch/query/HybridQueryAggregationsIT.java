/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationBuckets;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.TestUtils.assertHitResultsFromQuery;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

/**
 * Integration tests for base scenarios when aggregations are combined with hybrid query
 */
public class HybridQueryAggregationsIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS = "test-hybrid-aggs-multi-doc-index-multiple-shards";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD = "test-hybrid-aggs-multi-doc-index-single-shard";
    private static final String TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS = "test-hybrid-nested-aggs-multi-doc-index";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "everyone";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-aggregation-pipeline";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "People keep telling me orange but I still prefer pink";
    private static final String TEST_DOC_TEXT6 = "She traveled because it cost the same as therapy and was a lot more enjoyable";
    private static final String INTEGER_FIELD_1 = "doc_index";
    private static final int INTEGER_FIELD_1_VALUE = 1234;
    private static final int INTEGER_FIELD_2_VALUE = 2345;
    private static final int INTEGER_FIELD_3_VALUE = 3456;
    private static final int INTEGER_FIELD_4_VALUE = 4567;
    private static final String KEYWORD_FIELD_1 = "doc_keyword";
    private static final String KEYWORD_FIELD_1_VALUE = "workable";
    private static final String KEYWORD_FIELD_2_VALUE = "angry";
    private static final String KEYWORD_FIELD_3_VALUE = "likeable";
    private static final String KEYWORD_FIELD_4_VALUE = "entire";
    private static final String DATE_FIELD_1 = "doc_date";
    private static final String DATE_FIELD_1_VALUE = "01/03/1995";
    private static final String DATE_FIELD_2_VALUE = "05/02/2015";
    private static final String DATE_FIELD_3_VALUE = "07/23/2007";
    private static final String DATE_FIELD_4_VALUE = "08/21/2012";
    private static final String INTEGER_FIELD_PRICE = "doc_price";
    private static final int INTEGER_FIELD_PRICE_1_VALUE = 130;
    private static final int INTEGER_FIELD_PRICE_2_VALUE = 100;
    private static final int INTEGER_FIELD_PRICE_3_VALUE = 200;
    private static final int INTEGER_FIELD_PRICE_4_VALUE = 25;
    private static final int INTEGER_FIELD_PRICE_5_VALUE = 30;
    private static final int INTEGER_FIELD_PRICE_6_VALUE = 350;
    private static final String BUCKET_AGG_DOC_COUNT_FIELD = "doc_count";
    private static final String KEY = "key";
    private static final String BUCKET_AGG_KEY_AS_STRING = "key_as_string";
    private static final String SUM_AGGREGATION_NAME = "sum_aggs";
    private static final String MAX_AGGREGATION_NAME = "max_aggs";
    private static final String DATE_AGGREGATION_NAME = "date_aggregation";
    private static final String GENERIC_AGGREGATION_NAME = "my_aggregation";
    private static final String BUCKETS_AGGREGATION_NAME_1 = "date_buckets_1";
    private static final String BUCKETS_AGGREGATION_NAME_2 = "date_buckets_2";
    private static final String BUCKETS_AGGREGATION_NAME_3 = "date_buckets_3";
    private static final String BUCKETS_AGGREGATION_NAME_4 = "date_buckets_4";
    protected static final String FLOAT_FIELD_NAME_IMDB = "imdb";
    protected static final String KEYWORD_FIELD_NAME_ACTOR = "actor";
    protected static final String CARDINALITY_OF_UNIQUE_NAMES = "cardinality_of_unique_names";
    protected static final String UNIQUE_NAMES = "unique_names";
    protected static final String AGGREGATION_NAME_MAX_SCORE = "max_score";
    protected static final String AGGREGATION_NAME_TOP_DOC = "top_doc";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @SneakyThrows
    public void testPipelineAggs_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testAvgSumMinMaxAggs();
    }

    @SneakyThrows
    public void testPipelineAggs_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testAvgSumMinMaxAggs();
    }

    @SneakyThrows
    public void testMetricAggsOnSingleShard_whenMaxAggsAndConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testMaxAggsOnSingleShardCluster();
    }

    @SneakyThrows
    public void testMetricAggsOnSingleShard_whenMaxAggsAndConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testMaxAggsOnSingleShardCluster();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateRange();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateRange();
    }

    @SneakyThrows
    public void testAggregationNotSupportedConcurrentSearch_whenUseSamplerAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);

        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.sampler(GENERIC_AGGREGATION_NAME)
                .shardSize(2)
                .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_1));

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggsBuilder),
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                3
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            Map<String, Object> aggValue = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(2, aggValue.size());
            assertEquals(3, aggValue.get(BUCKET_AGG_DOC_COUNT_FIELD));
            Map<String, Object> nestedAggs = getAggregationValues(aggValue, BUCKETS_AGGREGATION_NAME_1);
            assertNotNull(nestedAggs);
            assertEquals(0, nestedAggs.get("doc_count_error_upper_bound"));
            List<Map<String, Object>> buckets = getAggregationBuckets(aggValue, BUCKETS_AGGREGATION_NAME_1);
            assertEquals(2, buckets.size());

            Map<String, Object> firstBucket = buckets.get(0);
            assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("likeable", firstBucket.get(KEY));

            Map<String, Object> secondBucket = buckets.get(1);
            assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("workable", secondBucket.get(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithMultipleShards_WhenConcurrentSearchNotEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testPostFilterWithSimpleHybridQuery(false, true);
        testPostFilterWithComplexHybridQuery(false, true);
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithMultipleShards_WhenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testPostFilterWithSimpleHybridQuery(false, true);
        testPostFilterWithComplexHybridQuery(false, true);
    }

    @SneakyThrows
    private void testPostFilterWithSimpleHybridQuery(boolean isSingleShard, boolean hasPostFilterQuery) {
        try {
            if (isSingleShard) {
                prepareResourcesForSingleShardIndex(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, SEARCH_PIPELINE);
            } else {
                prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            }

            HybridQueryBuilder simpleHybridQueryBuilder = createHybridQueryBuilder(false);

            QueryBuilder rangeFilterQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_1).gte(2000).lte(5000);

            Map<String, Object> searchResponseAsMap;

            if (isSingleShard && hasPostFilterQuery) {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                    simpleHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    rangeFilterQuery,
                    null,
                    false,
                    null,
                    0
                );

                assertHitResultsFromQuery(1, searchResponseAsMap);
            } else if (isSingleShard && !hasPostFilterQuery) {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                    simpleHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    null,
                    null,
                    false,
                    null,
                    0
                );
                assertHitResultsFromQuery(2, searchResponseAsMap);
            } else if (!isSingleShard && hasPostFilterQuery) {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    simpleHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    rangeFilterQuery,
                    null,
                    false,
                    null,
                    0
                );
                assertHitResultsFromQuery(2, searchResponseAsMap);
            } else {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    simpleHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    null,
                    null,
                    false,
                    null,
                    0
                );
                assertHitResultsFromQuery(3, searchResponseAsMap);
            }

            // assert post-filter
            List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);

            List<Integer> docIndexes = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                assertNotNull(oneHit.get("_source"));
                Map<String, Object> source = (Map<String, Object>) oneHit.get("_source");
                int docIndex = (int) source.get(INTEGER_FIELD_1);
                docIndexes.add(docIndex);
            }
            if (isSingleShard && hasPostFilterQuery) {
                assertEquals(0, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());

            } else if (isSingleShard && !hasPostFilterQuery) {
                assertEquals(1, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());

            } else if (!isSingleShard && hasPostFilterQuery) {
                assertEquals(0, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());
            } else {
                assertEquals(1, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());
            }
        } finally {
            if (isSingleShard) {
                wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
            } else {
                wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
            }
        }
    }

    @SneakyThrows
    private void testPostFilterWithComplexHybridQuery(boolean isSingleShard, boolean hasPostFilterQuery) {
        try {
            if (isSingleShard) {
                prepareResourcesForSingleShardIndex(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, SEARCH_PIPELINE);
            } else {
                prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            }

            HybridQueryBuilder complexHybridQueryBuilder = createHybridQueryBuilder(true);

            QueryBuilder rangeFilterQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_1).gte(2000).lte(5000);

            Map<String, Object> searchResponseAsMap;

            if (isSingleShard && hasPostFilterQuery) {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                    complexHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    rangeFilterQuery,
                    null,
                    false,
                    null,
                    0
                );

                assertHitResultsFromQuery(1, searchResponseAsMap);
            } else if (isSingleShard && !hasPostFilterQuery) {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                    complexHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    null,
                    null,
                    false,
                    null,
                    0
                );
                assertHitResultsFromQuery(2, searchResponseAsMap);
            } else if (!isSingleShard && hasPostFilterQuery) {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    complexHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    rangeFilterQuery,
                    null,
                    false,
                    null,
                    0
                );
                assertHitResultsFromQuery(4, searchResponseAsMap);
            } else {
                searchResponseAsMap = search(
                    TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                    complexHybridQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE),
                    null,
                    null,
                    null,
                    false,
                    null,
                    0
                );
                assertHitResultsFromQuery(3, searchResponseAsMap);
            }

            // assert post-filter
            List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);

            List<Integer> docIndexes = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                assertNotNull(oneHit.get("_source"));
                Map<String, Object> source = (Map<String, Object>) oneHit.get("_source");
                int docIndex = (int) source.get(INTEGER_FIELD_1);
                docIndexes.add(docIndex);
            }
            if (isSingleShard && hasPostFilterQuery) {
                assertEquals(0, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());

            } else if (isSingleShard && !hasPostFilterQuery) {
                assertEquals(1, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());

            } else if (!isSingleShard && hasPostFilterQuery) {
                assertEquals(0, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());
            } else {
                assertEquals(1, docIndexes.stream().filter(docIndex -> docIndex < 2000 || docIndex > 5000).count());
            }
        } finally {
            if (isSingleShard) {
                wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
            } else {
                wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
            }
        }
    }

    @SneakyThrows
    private void testAvgSumMinMaxAggs() {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.dateHistogram(GENERIC_AGGREGATION_NAME)
                .calendarInterval(DateHistogramInterval.YEAR)
                .field(DATE_FIELD_1)
                .subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_1));

            BucketMetricsPipelineAggregationBuilder<AvgBucketPipelineAggregationBuilder> aggAvgBucket = PipelineAggregatorBuilders
                .avgBucket(BUCKETS_AGGREGATION_NAME_1, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            BucketMetricsPipelineAggregationBuilder<SumBucketPipelineAggregationBuilder> aggSumBucket = PipelineAggregatorBuilders
                .sumBucket(BUCKETS_AGGREGATION_NAME_2, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            BucketMetricsPipelineAggregationBuilder<MinBucketPipelineAggregationBuilder> aggMinBucket = PipelineAggregatorBuilders
                .minBucket(BUCKETS_AGGREGATION_NAME_3, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            BucketMetricsPipelineAggregationBuilder<MaxBucketPipelineAggregationBuilder> aggMaxBucket = PipelineAggregatorBuilders
                .maxBucket(BUCKETS_AGGREGATION_NAME_4, GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME);

            Map<String, Object> searchResponseAsMapAnngsBoolQuery = executeQueryAndGetAggsResults(
                List.of(aggsBuilder, aggAvgBucket, aggSumBucket, aggMinBucket, aggMaxBucket),
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                3
            );

            assertResultsOfPipelineSumtoDateHistogramAggs(searchResponseAsMapAnngsBoolQuery);

            // test only aggregation without query (handled as match_all query)
            Map<String, Object> searchResponseAsMapAggsNoQuery = executeQueryAndGetAggsResults(
                List.of(aggsBuilder, aggAvgBucket),
                null,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                6
            );

            assertResultsOfPipelineSumtoDateHistogramAggsForMatchAllQuery(searchResponseAsMapAggsNoQuery);

        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithSingleShards_WhenConcurrentSearchNotEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testPostFilterWithSimpleHybridQuery(true, true);
        testPostFilterWithComplexHybridQuery(true, true);
    }

    @SneakyThrows
    public void testPostFilterOnIndexWithSingleShards_WhenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testPostFilterWithSimpleHybridQuery(true, true);
        testPostFilterWithComplexHybridQuery(true, true);
    }

    @SneakyThrows
    public void testNestedAggs_whenMultipleShardsAndConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        try {
            prepareResourcesForNestegAggregationsScenario(TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS);
            assertNestedAggregations(TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testNestedAggs_whenMultipleShardsAndConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        try {
            prepareResourcesForNestegAggregationsScenario(TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS);
            assertNestedAggregations(TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_FOR_NESTED_AGGS_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void prepareResourcesForNestegAggregationsScenario(String index) throws Exception {
        if (!indexExists(index)) {
            createIndexWithConfiguration(
                index,
                buildIndexConfiguration(
                    List.of(new KNNFieldConfig("location", 2, TEST_SPACE_TYPE)),
                    List.of(),
                    List.of(),
                    List.of(FLOAT_FIELD_NAME_IMDB),
                    List.of(KEYWORD_FIELD_NAME_ACTOR),
                    List.of(),
                    3
                ),
                ""
            );

            String ingestBulkPayload = Files.readString(Path.of(classLoader.getResource("processor/ingest_bulk.json").toURI()))
                .replace("\"{indexname}\"", "\"" + index + "\"");

            bulkIngest(ingestBulkPayload, null);
        }
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
    }

    private void assertNestedAggregations(String index) {
        /* constructing following search query
        {
            "from": 0,
                "aggs": {
            "cardinality_of_unique_names": {
                "cardinality": {
                    "field": "actor"
                }
            },
            "unique_names": {
                "terms": {
                    "field": "actor",
                            "size": 10,
                            "order": {
                        "max_score": "desc"
                    }
                },
                "aggs": {
                    "top_doc": {
                        "top_hits": {
                            "size": 1,
                                    "sort": [
                            {
                                "_score": {
                                "order": "desc"
                            }
                            }
                    ]
                        }
                    },
                    "max_score": {
                        "max": {
                            "script": {
                                "source": "_score"
                            }
                        }
                    }
                }
            }
        },
            "query": {
            "hybrid": {
                "queries": [
                {
                    "match": {
                    "actor": "anil"
                }
                },
                {
                    "range": {
                    "imdb": {
                        "gte": 1.0,
                                "lte": 10.0
                    }
                }
                }
        ]}}}
        */

        QueryBuilder rangeFilterQuery = QueryBuilders.rangeQuery(FLOAT_FIELD_NAME_IMDB).gte(1.0).lte(10.0);
        QueryBuilder matchQuery = QueryBuilders.matchQuery(KEYWORD_FIELD_NAME_ACTOR, "anil");
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQuery).add(rangeFilterQuery);

        AggregationBuilder aggsBuilderCardinality = AggregationBuilders.cardinality(CARDINALITY_OF_UNIQUE_NAMES)
            .field(KEYWORD_FIELD_NAME_ACTOR);
        AggregationBuilder aggsBuilderUniqueNames = AggregationBuilders.terms(UNIQUE_NAMES)
            .field(KEYWORD_FIELD_NAME_ACTOR)
            .size(10)
            .order(BucketOrder.aggregation(AGGREGATION_NAME_MAX_SCORE, false))
            .subAggregation(
                AggregationBuilders.topHits(AGGREGATION_NAME_TOP_DOC).size(1).sort(SortBuilders.scoreSort().order(SortOrder.DESC))
            )
            .subAggregation(AggregationBuilders.max(AGGREGATION_NAME_MAX_SCORE).script(new Script("_score")));

        Map<String, Object> searchResponseAsMap = search(
            index,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            List.of(aggsBuilderCardinality, aggsBuilderUniqueNames),
            rangeFilterQuery,
            null,
            false,
            null,
            0
        );
        assertNotNull(searchResponseAsMap);

        // assert actual results
        // aggregations
        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        int cardinalityValue = getAggregationValue(aggregations, CARDINALITY_OF_UNIQUE_NAMES);
        assertEquals(7, cardinalityValue);

        Map<String, Object> uniqueAggValue = getAggregationValues(aggregations, UNIQUE_NAMES);
        assertEquals(3, uniqueAggValue.size());
        assertEquals(0, uniqueAggValue.get("doc_count_error_upper_bound"));
        assertEquals(0, uniqueAggValue.get("sum_other_doc_count"));

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, UNIQUE_NAMES);
        assertNotNull(buckets);
        assertEquals(7, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(4, firstBucket.size());
        assertEquals("anil", firstBucket.get(KEY));
        assertEquals(42, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertNotNull(getAggregationValue(firstBucket, AGGREGATION_NAME_MAX_SCORE));
        assertTrue((double) getAggregationValue(firstBucket, AGGREGATION_NAME_MAX_SCORE) > 1.0f);

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(4, secondBucket.size());
        assertEquals("abhishek", secondBucket.get(KEY));
        assertEquals(8, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertNotNull(getAggregationValue(secondBucket, AGGREGATION_NAME_MAX_SCORE));
        assertEquals(1.0, getAggregationValue(secondBucket, AGGREGATION_NAME_MAX_SCORE), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(4, lastBucket.size());
        assertEquals("sanjay", lastBucket.get(KEY));
        assertEquals(7, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertNotNull(getAggregationValue(lastBucket, AGGREGATION_NAME_MAX_SCORE));
        assertEquals(1.0, getAggregationValue(lastBucket, AGGREGATION_NAME_MAX_SCORE), DELTA_FOR_SCORE_ASSERTION);

        // assert the hybrid query scores
        assertHitResultsFromQuery(10, 92, searchResponseAsMap);
    }

    private void testMaxAggsOnSingleShardCluster() throws Exception {
        try {
            prepareResourcesForSingleShardIndex(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, SEARCH_PIPELINE);

            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

            AggregationBuilder aggsBuilder = AggregationBuilders.max(MAX_AGGREGATION_NAME).field(INTEGER_FIELD_1);
            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD,
                hybridQueryBuilderNeuralThenTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                List.of(aggsBuilder)
            );

            assertHitResultsFromQuery(2, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(MAX_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, MAX_AGGREGATION_NAME);
            assertTrue(maxAggsValue >= 0);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    private void testDateRange() throws IOException {
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.dateRange(DATE_AGGREGATION_NAME)
                .field(DATE_FIELD_1)
                .format("MM-yyyy")
                .addRange("01-2014", "02-2024");

            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                List.of(aggsBuilder),
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                3
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);
            assertNotNull(buckets);
            assertEquals(1, buckets.size());

            Map<String, Object> bucket = buckets.get(0);

            assertEquals(6, bucket.size());
            assertEquals("01-2014", bucket.get("from_as_string"));
            assertEquals(2, bucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
            assertEquals("02-2024", bucket.get("to_as_string"));
            assertTrue(bucket.containsKey("from"));
            assertTrue(bucket.containsKey("to"));
            assertTrue(bucket.containsKey(KEY));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) throws IOException {
        if (TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(List.of(), List.of(), List.of(INTEGER_FIELD_1), List.of(KEYWORD_FIELD_1), List.of(DATE_FIELD_1), 3),
                ""
            );

            addKnnDoc(
                indexName,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_1_VALUE, INTEGER_FIELD_PRICE_1_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_1_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_1_VALUE)
            );
            addKnnDoc(
                indexName,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_2_VALUE, INTEGER_FIELD_PRICE_2_VALUE),
                List.of(),
                List.of(),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_2_VALUE)
            );
            addKnnDoc(
                indexName,
                "3",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_PRICE_3_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_2_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_3_VALUE)
            );
            addKnnDoc(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_3_VALUE, INTEGER_FIELD_PRICE_4_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_3_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_2_VALUE)
            );
            addKnnDoc(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_3_VALUE, INTEGER_FIELD_PRICE_5_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_4_VALUE)
            );
            addKnnDoc(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT6),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_4_VALUE, INTEGER_FIELD_PRICE_6_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_4_VALUE)
            );
        }
    }

    @SneakyThrows
    private void initializeIndexWithOneShardIfNotExists(String indexName) {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(List.of(), List.of(), List.of(INTEGER_FIELD_1), List.of(KEYWORD_FIELD_1), List.of(), 1),
                ""
            );

            addKnnDoc(
                indexName,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_1_VALUE),
                List.of(),
                List.of(),
                List.of(),
                List.of()
            );

            addKnnDoc(
                indexName,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1),
                List.of(INTEGER_FIELD_2_VALUE),
                List.of(),
                List.of(),
                List.of(),
                List.of()
            );
        }
    }

    @SneakyThrows
    void prepareResources(String indexName, String pipelineName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipelineWithResultsPostProcessor(pipelineName);
    }

    @SneakyThrows
    void prepareResourcesForSingleShardIndex(String indexName, String pipelineName) {
        initializeIndexWithOneShardIfNotExists(indexName);
        createSearchPipelineWithResultsPostProcessor(pipelineName);
    }

    private void assertResultsOfPipelineSumtoDateHistogramAggs(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        double aggValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_1);
        assertEquals(3517.5, aggValue, DELTA_FOR_SCORE_ASSERTION);

        double sumValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_2);
        assertEquals(7035.0, sumValue, DELTA_FOR_SCORE_ASSERTION);

        double minValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_3);
        assertEquals(1234.0, minValue, DELTA_FOR_SCORE_ASSERTION);

        double maxValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_4);
        assertEquals(5801.0, maxValue, DELTA_FOR_SCORE_ASSERTION);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(21, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(4, firstBucket.size());
        assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(4, secondBucket.size());
        assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(4, lastBucket.size());
        assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(2, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(5801.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));
    }

    private void assertResultsOfPipelineSumtoDateHistogramAggsForMatchAllQuery(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        double aggValue = getAggregationValue(aggregations, BUCKETS_AGGREGATION_NAME_1);
        assertEquals(3764.5, aggValue, DELTA_FOR_SCORE_ASSERTION);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(21, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(4, firstBucket.size());
        assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(4, secondBucket.size());
        assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(4, lastBucket.size());
        assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(2, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(5801.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));
    }

    private Map<String, Object> executeQueryAndGetAggsResults(final List<Object> aggsBuilders, String indexName, int expectedHitsNumber) {

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

        return executeQueryAndGetAggsResults(aggsBuilders, hybridQueryBuilderNeuralThenTerm, indexName, expectedHitsNumber);
    }

    private Map<String, Object> executeQueryAndGetAggsResults(
        final List<Object> aggsBuilders,
        QueryBuilder queryBuilder,
        String indexName,
        int expectedHits
    ) {
        Map<String, Object> searchResponseAsMap = search(
            indexName,
            queryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            aggsBuilders
        );

        assertHitResultsFromQuery(expectedHits, searchResponseAsMap);
        return searchResponseAsMap;
    }

    private HybridQueryBuilder createHybridQueryBuilder(boolean isComplex) {
        if (isComplex) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should().add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

            QueryBuilder rangeFilterQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_1).gte(2000).lte(5000);

            QueryBuilder matchQuery = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(boolQueryBuilder).add(rangeFilterQuery).add(matchQuery);
            return hybridQueryBuilder;

        } else {
            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder3);
            return hybridQueryBuilderNeuralThenTerm;
        }
    }

}
