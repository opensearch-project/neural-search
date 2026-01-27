/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.aggregation;

import lombok.SneakyThrows;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationBuckets;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;

/**
 * Integration tests for pipeline type aggregations when they are bundled with hybrid query
 * Below is list of aggregations that are present in this test:
 * - bucket_sort
 * - cumulative_sum
 *
 * Following metric aggs are tested by other integ tests:
 * - min_bucket
 * - max_bucket
 * - sum_bucket
 * - avg_bucket
 *
 * Below aggregations are not part of any test:
 * - derivative
 * - moving_avg
 * - serial_diff
 */
public class PipelineAggregationsWithHybridQueryIT extends BaseAggregationsWithHybridQueryIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS =
        "test-aggs-pipeline-multi-doc-index-multiple-shards";
    private static final String SEARCH_PIPELINE = "search-pipeline-pipeline-aggs";

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketScriptedAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToBucketScriptedAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketSortAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateBucketedSumsPipelinedToBucketSortAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToBucketSortAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToBucketSortAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToCumulativeSumAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateBucketedSumsPipelinedToCumulativeSumAggs();
    }

    @SneakyThrows
    public void testPipelineParentAggs_whenDateBucketedSumsPipelinedToCumulativeSumAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToCumulativeSumAggs();
    }

    private void testDateBucketedSumsPipelinedToBucketStatsAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggDateHisto = AggregationBuilders.dateHistogram(GENERIC_AGGREGATION_NAME)
            .calendarInterval(DateHistogramInterval.YEAR)
            .field(DATE_FIELD)
            .subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX));

        StatsBucketPipelineAggregationBuilder aggStatsBucket = PipelineAggregatorBuilders.statsBucket(
            BUCKETS_AGGREGATION_NAME_1,
            GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME
        );

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            List.of(aggDateHisto, aggStatsBucket),
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        Map<String, Object> statsAggs = getAggregationValues(aggregations, BUCKETS_AGGREGATION_NAME_1);

        assertNotNull(statsAggs);

        assertEquals(3517.5, (Double) statsAggs.get("avg"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(7035.0, (Double) statsAggs.get("sum"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1234.0, (Double) statsAggs.get("min"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(5801.0, (Double) statsAggs.get("max"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(2, (int) statsAggs.get("count"));
    }

    private void testDateBucketedSumsPipelinedToBucketScriptedAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(DATE_AGGREGATION_NAME)
            .calendarInterval(DateHistogramInterval.YEAR)
            .field(DATE_FIELD)
            .subAggregations(
                new AggregatorFactories.Builder().addAggregator(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX))
                    .addAggregator(
                        AggregationBuilders.filter(
                            GENERIC_AGGREGATION_NAME,
                            QueryBuilders.boolQuery()
                                .should(
                                    QueryBuilders.boolQuery()
                                        .should(QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_WORKABLE))
                                        .should(QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_ANGRY))
                                )
                                .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(KEYWORD_FIELD_DOCKEYWORD)))
                        ).subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME_2).field(INTEGER_FIELD_PRICE))
                    )
                    .addPipelineAggregator(
                        PipelineAggregatorBuilders.bucketScript(
                            BUCKETS_AGGREGATION_NAME_1,
                            Map.of("docNum", GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME_2, "totalNum", SUM_AGGREGATION_NAME),
                            new Script("params.docNum / params.totalNum")
                        )
                    )
            );

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);

        assertNotNull(buckets);
        assertEquals(21, buckets.size());

        // check content of few buckets
        // first bucket have all the aggs values
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(6, firstBucket.size());
        assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.1053, getAggregationValue(firstBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        Map<String, Object> inBucketAggValues = getAggregationValues(firstBucket, GENERIC_AGGREGATION_NAME);
        assertNotNull(inBucketAggValues);
        assertEquals(1, inBucketAggValues.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(130.0, getAggregationValue(inBucketAggValues, SUM_AGGREGATION_NAME_2), DELTA_FOR_SCORE_ASSERTION);

        // second bucket is empty
        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(5, secondBucket.size());
        assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertFalse(secondBucket.containsKey(BUCKETS_AGGREGATION_NAME_1));
        assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        Map<String, Object> inSecondBucketAggValues = getAggregationValues(secondBucket, GENERIC_AGGREGATION_NAME);
        assertNotNull(inSecondBucketAggValues);
        assertEquals(0, inSecondBucketAggValues.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(inSecondBucketAggValues, SUM_AGGREGATION_NAME_2), DELTA_FOR_SCORE_ASSERTION);

        // last bucket has values
        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(6, lastBucket.size());
        assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(2, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0172, getAggregationValue(lastBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(5801.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));

        Map<String, Object> inLastBucketAggValues = getAggregationValues(lastBucket, GENERIC_AGGREGATION_NAME);
        assertNotNull(inLastBucketAggValues);
        assertEquals(1, inLastBucketAggValues.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(100.0, getAggregationValue(inLastBucketAggValues, SUM_AGGREGATION_NAME_2), DELTA_FOR_SCORE_ASSERTION);
    }

    private void testDateBucketedSumsPipelinedToBucketSortAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(DATE_AGGREGATION_NAME)
            .calendarInterval(DateHistogramInterval.YEAR)
            .field(DATE_FIELD)
            .subAggregations(
                new AggregatorFactories.Builder().addAggregator(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX))
                    .addPipelineAggregator(
                        PipelineAggregatorBuilders.bucketSort(
                            BUCKETS_AGGREGATION_NAME_1,
                            List.of(new FieldSortBuilder(SUM_AGGREGATION_NAME).order(SortOrder.DESC))
                        ).size(5)
                    )
            );

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .should(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_WORKABLE))
                    .should(QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_ANGRY))
            )
            .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(KEYWORD_FIELD_DOCKEYWORD)));

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            List.of(aggBuilder),
            queryBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);

        assertNotNull(buckets);
        assertEquals(3, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(4, firstBucket.size());
        assertEquals("01/01/2015", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(2345.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        // second bucket is empty
        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(4, secondBucket.size());
        assertEquals("01/01/1995", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        // last bucket has values
        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(4, lastBucket.size());
        assertEquals("01/01/2007", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));
    }

    private void testDateBucketedSumsPipelinedToCumulativeSumAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggBuilder = AggregationBuilders.dateHistogram(DATE_AGGREGATION_NAME)
            .calendarInterval(DateHistogramInterval.YEAR)
            .field(DATE_FIELD)
            .subAggregations(
                new AggregatorFactories.Builder().addAggregator(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX))
                    .addPipelineAggregator(PipelineAggregatorBuilders.cumulativeSum(BUCKETS_AGGREGATION_NAME_1, SUM_AGGREGATION_NAME))
            );

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .should(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_WORKABLE))
                    .should(QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_ANGRY))
            )
            .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(KEYWORD_FIELD_DOCKEYWORD)));

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            List.of(aggBuilder),
            queryBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, DATE_AGGREGATION_NAME);

        assertNotNull(buckets);
        assertEquals(21, buckets.size());

        // check content of few buckets
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(5, firstBucket.size());
        assertEquals("01/01/1995", firstBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1234.0, getAggregationValue(firstBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(firstBucket.containsKey(KEY));

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(5, secondBucket.size());
        assertEquals("01/01/1996", secondBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(0, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1234.0, getAggregationValue(secondBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(secondBucket.containsKey(KEY));

        // last bucket is empty
        Map<String, Object> lastBucket = buckets.get(buckets.size() - 1);
        assertEquals(5, lastBucket.size());
        assertEquals("01/01/2015", lastBucket.get(BUCKET_AGG_KEY_AS_STRING));
        assertEquals(1, lastBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(2345.0, getAggregationValue(lastBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(3579.0, getAggregationValue(lastBucket, BUCKETS_AGGREGATION_NAME_1), DELTA_FOR_SCORE_ASSERTION);
        assertTrue(lastBucket.containsKey(KEY));
    }

    private Map<String, Object> executeQueryAndGetAggsResults(final Object aggsBuilder, String indexName) {
        return executeQueryAndGetAggsResults(List.of(aggsBuilder), indexName);
    }

    private Map<String, Object> executeQueryAndGetAggsResults(
        final List<Object> aggsBuilders,
        QueryBuilder queryBuilder,
        String indexName,
        int expectedHits
    ) {
        initializeIndexIfNotExist(indexName);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            queryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            aggsBuilders,
            null
        );

        assertHitResultsFromQuery(expectedHits, searchResponseAsMap);
        return searchResponseAsMap;
    }

    private Map<String, Object> executeQueryAndGetAggsResults(
        final List<Object> aggsBuilders,
        QueryBuilder queryBuilder,
        String indexName
    ) {
        return executeQueryAndGetAggsResults(aggsBuilders, queryBuilder, indexName, 3);
    }

    private Map<String, Object> executeQueryAndGetAggsResults(final List<Object> aggsBuilders, String indexName) {

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

        return executeQueryAndGetAggsResults(aggsBuilders, hybridQueryBuilderNeuralThenTerm, indexName);
    }
}
