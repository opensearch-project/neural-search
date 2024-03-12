/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.neuralsearch.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationBuckets;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;

public class HybridQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-hybrid-basic-index";
    private static final String TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME = "test-hybrid-vector-doc-field-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-hybrid-multi-doc-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD = "test-hybrid-multi-doc-single-shard-index";
    private static final String TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD =
        "test-hybrid-multi-doc-nested-type-single-shard-index";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS =
        "test-neural-aggs-pipeline-multi-doc-index-multiple-shards";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD = "test-neural-aggs-multi-doc-index-single-shard";
    private static final String TEST_QUERY_TEXT = "greetings";
    private static final String TEST_QUERY_TEXT2 = "salute";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    private static final String NESTED_FIELD_1 = "firstname";
    private static final String NESTED_FIELD_2 = "lastname";
    private static final String NESTED_FIELD_1_VALUE = "john";
    private static final String NESTED_FIELD_2_VALUE = "black";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-pipeline";
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

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    public boolean isUpdateClusterSettings() {
        return false;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * Tests complex query with multiple nested sub-queries:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "bool": {
     *                          "should": [
     *                              {
     *                                  "term": {
     *                                      "text": "word1"
     *                                  }
     *                             },
     *                             {
     *                                  "term": {
     *                                      "text": "word2"
     *                                   }
     *                              }
     *                         ]
     *                      }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testComplexQuery_whenMultipleSubqueries_thenSuccessful() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
            TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(termQueryBuilder2).should(termQueryBuilder3);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(boolQueryBuilder);

            Map<String, Object> searchResponseAsMap1 = search(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                hybridQueryBuilderNeuralThenTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(3, getHitCount(searchResponseAsMap1));

            List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
            List<String> ids = new ArrayList<>();
            List<Double> scores = new ArrayList<>();
            for (Map<String, Object> oneHit : hits1NestedList) {
                ids.add((String) oneHit.get("_id"));
                scores.add((Double) oneHit.get("_score"));
            }

            // verify that scores are in desc order
            assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
            // verify that all ids are unique
            assertEquals(Set.copyOf(ids).size(), ids.size());

            Map<String, Object> total = getTotalHits(searchResponseAsMap1);
            assertNotNull(total.get("value"));
            assertEquals(3, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            wipeOfTestResources(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME, null, modelId, SEARCH_PIPELINE);
        }
    }

    /**
     * Tests complex query with multiple nested sub-queries, where some sub-queries are same
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "term": {
     *                         "text": "word1"
     *                       }
     *                  },
     *                  {
     *                      "term": {
     *                         "text": "word2"
     *                       }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testComplexQuery_whenMultipleIdenticalSubQueries_thenSuccessful() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
            TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

            HybridQueryBuilder hybridQueryBuilderThreeTerms = new HybridQueryBuilder();
            hybridQueryBuilderThreeTerms.add(termQueryBuilder1);
            hybridQueryBuilderThreeTerms.add(termQueryBuilder2);
            hybridQueryBuilderThreeTerms.add(termQueryBuilder3);

            Map<String, Object> searchResponseAsMap1 = search(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                hybridQueryBuilderThreeTerms,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(2, getHitCount(searchResponseAsMap1));

            List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
            List<String> ids = new ArrayList<>();
            List<Double> scores = new ArrayList<>();
            for (Map<String, Object> oneHit : hits1NestedList) {
                ids.add((String) oneHit.get("_id"));
                scores.add((Double) oneHit.get("_score"));
            }

            // verify that scores are in desc order
            assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
            // verify that all ids are unique
            assertEquals(Set.copyOf(ids).size(), ids.size());

            Map<String, Object> total = getTotalHits(searchResponseAsMap1);
            assertNotNull(total.get("value"));
            assertEquals(2, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            wipeOfTestResources(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME, null, modelId, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testNoMatchResults_whenOnlyTermSubQueryWithoutMatch_thenEmptyResult() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);
            TermQueryBuilder termQuery2Builder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT2);
            HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
            hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
            hybridQueryBuilderOnlyTerm.add(termQuery2Builder);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_NAME,
                hybridQueryBuilderOnlyTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(0, getHitCount(searchResponseAsMap));
            assertTrue(getMaxScore(searchResponseAsMap).isPresent());
            assertEquals(0.0f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> total = getTotalHits(searchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(0, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_NAME, null, modelId, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testNestedQuery_whenHybridQueryIsWrappedIntoOtherQuery_thenFail() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            MatchQueryBuilder matchQuery2Builder = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
            HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
            hybridQueryBuilderOnlyTerm.add(matchQueryBuilder);
            hybridQueryBuilderOnlyTerm.add(matchQuery2Builder);
            MatchQueryBuilder matchQuery3Builder = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().should(hybridQueryBuilderOnlyTerm).should(matchQuery3Builder);

            ResponseException exceptionNoNestedTypes = expectThrows(
                ResponseException.class,
                () -> search(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD, boolQueryBuilder, null, 10, Map.of("search_pipeline", SEARCH_PIPELINE))
            );

            org.hamcrest.MatcherAssert.assertThat(
                exceptionNoNestedTypes.getMessage(),
                allOf(
                    containsString("hybrid query must be a top level query and cannot be wrapped into other queries"),
                    containsString("illegal_argument_exception")
                )
            );

            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);

            ResponseException exceptionQWithNestedTypes = expectThrows(
                ResponseException.class,
                () -> search(
                    TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
                    boolQueryBuilder,
                    null,
                    10,
                    Map.of("search_pipeline", SEARCH_PIPELINE)
                )
            );

            org.hamcrest.MatcherAssert.assertThat(
                exceptionQWithNestedTypes.getMessage(),
                allOf(
                    containsString("hybrid query must be a top level query and cannot be wrapped into other queries"),
                    containsString("illegal_argument_exception")
                )
            );
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD, null, modelId, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testIndexWithNestedFields_whenHybridQuery_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQuery2Builder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT2);
            HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
            hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
            hybridQueryBuilderOnlyTerm.add(termQuery2Builder);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
                hybridQueryBuilderOnlyTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(1, getHitCount(searchResponseAsMap));
            assertTrue(getMaxScore(searchResponseAsMap).isPresent());
            assertEquals(0.5f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> total = getTotalHits(searchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(1, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD, null, modelId, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testIndexWithNestedFields_whenHybridQueryIncludesNested_thenSuccess() {
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);
            NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(
                TEST_NESTED_TYPE_FIELD_NAME_1,
                matchQuery(TEST_NESTED_TYPE_FIELD_NAME_1 + "." + NESTED_FIELD_1, NESTED_FIELD_1_VALUE),
                ScoreMode.Total
            );
            HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
            hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
            hybridQueryBuilderOnlyTerm.add(nestedQueryBuilder);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
                hybridQueryBuilderOnlyTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(1, getHitCount(searchResponseAsMap));
            assertTrue(getMaxScore(searchResponseAsMap).isPresent());
            assertEquals(0.5f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> total = getTotalHits(searchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(1, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD, null, null, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testPipelineAggs_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testAvgSumMinMaxAggs();
    }

    @SneakyThrows
    public void testPipelineAggs_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testAvgSumMinMaxAggs();
    }

    @SneakyThrows
    public void testMetricAggsOnSingleShard_whenMaxAggsAndConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testMaxAggsOnSingleShardCluster();
    }

    @SneakyThrows
    public void testMetricAggsOnSingleShard_whenMaxAggsAndConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testMaxAggsOnSingleShardCluster();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", false);
        testDateRange();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);
        testDateRange();
    }

    @SneakyThrows
    public void testAggregationNotSupportedConcurrentSearch_whenUseSamplerAgg_thenSuccessful() {
        updateClusterSettings("search.concurrent_segment_search.enabled", true);

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
            // try {
            // prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

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
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_BASIC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray())
            );
            assertEquals(1, getDocCount(TEST_BASIC_INDEX_NAME));
        }
        if (TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                )
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "1",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector1).toArray(), Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "2",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector2).toArray(), Floats.asList(testVector2).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "3",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector3).toArray(), Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            assertEquals(3, getDocCount(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME));
        }

        if (TEST_MULTI_DOC_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addDocsToIndex(TEST_MULTI_DOC_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD.equals(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                1
            );
            addDocsToIndex(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        }

        if (TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                    List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                    1
                ),
                ""
            );

            addDocsToIndex(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
                "4",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                List.of(),
                List.of(),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE, NESTED_FIELD_2, NESTED_FIELD_2_VALUE))
            );
        }

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

    private void addDocsToIndex(final String testMultiDocIndexName) {
        addKnnDoc(
            testMultiDocIndexName,
            "1",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
            Collections.singletonList(Floats.asList(testVector1).toArray()),
            Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
            Collections.singletonList(TEST_DOC_TEXT1)
        );
        addKnnDoc(
            testMultiDocIndexName,
            "2",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
            Collections.singletonList(Floats.asList(testVector2).toArray())
        );
        addKnnDoc(
            testMultiDocIndexName,
            "3",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
            Collections.singletonList(Floats.asList(testVector3).toArray()),
            Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
            Collections.singletonList(TEST_DOC_TEXT2)
        );
        assertEquals(3, getDocCount(testMultiDocIndexName));
    }

    private List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    private Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }

    private Optional<Float> getMaxScore(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return hitsMap.get("max_score") == null ? Optional.empty() : Optional.of(((Double) hitsMap.get("max_score")).floatValue());
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

    private void assertHitResultsFromQuery(int expected, Map<String, Object> searchResponseAsMap) {
        assertEquals(expected, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(expected, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }
}
