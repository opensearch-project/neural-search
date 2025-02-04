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
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationBuckets;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;

/**
 * Integration tests for bucket type aggregations when they are bundled with hybrid query
 * Below is list of aggregations that are present in this test:
 * - Adjacency matrix
 * - Diversified sampler
 * - Date histogram
 * - Nested
 * - Filter
 * - Global
 * - Sampler
 * - Histogram
 * - Significant terms
 * - Terms
 *
 * Following aggs are tested by other integ tests:
 * - Date range
 *
 * Below aggregations are not part of any test:
 * - Filters
 * - Geodistance
 * - Geohash grid
 * - Geohex grid
 * - Geotile grid
 * - IP range
 * - Missing
 * - Multi-terms
 * - Range
 * - Reverse nested
 * - Significant text
 */
public class BucketAggregationsWithHybridQueryIT extends BaseAggregationsWithHybridQueryIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS = "test-aggs-bucket-multi-doc-index-multiple-shards";
    private static final String SEARCH_PIPELINE = "search-pipeline-bucket-aggs";

    @SneakyThrows
    public void testBucketAndNestedAggs_whenAdjacencyMatrix_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testAdjacencyMatrixAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenAdjacencyMatrix_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testAdjacencyMatrixAggs();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenDiversifiedSampler_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDiversifiedSampler();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDiversifiedSampler_thenFail() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);

        testDiversifiedSampler();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenAvgNestedIntoFilter_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testAvgNestedIntoFilter();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenAvgNestedIntoFilter_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testAvgNestedIntoFilter();
    }

    @SneakyThrows
    public void testBucketAndNestedAggs_whenSumNestedIntoFilters_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testSumNestedIntoFilters();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSumNestedIntoFilters_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testSumNestedIntoFilters();
    }

    @SneakyThrows
    public void testBucketAggs_whenGlobalAggUsedWithQuery_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testGlobalAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenGlobalAggUsedWithQuery_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testGlobalAggs();
    }

    @SneakyThrows
    public void testBucketAggs_whenHistogramAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testHistogramAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenHistogramAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testHistogramAggs();
    }

    @SneakyThrows
    public void testBucketAggs_whenNestedAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testNestedAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenNestedAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testNestedAggs();
    }

    @SneakyThrows
    public void testBucketAggs_whenSamplerAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testSampler();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSamplerAgg_thenFail() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);

        testSampler();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenDateBucketedSumsPipelinedToBucketStatsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testDateBucketedSumsPipelinedToBucketStatsAggs();
    }

    @SneakyThrows
    public void testPipelineSiblingAggs_whenDateBucketedSumsPipelinedToBucketScriptAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testDateBucketedSumsPipelinedToBucketScriptedAggs();
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
    public void testMetricAggs_whenTermsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testTermsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenTermsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testTermsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenSignificantTermsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testSignificantTermsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSignificantTermsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testSignificantTermsAggs();
    }

    private void testAvgNestedIntoFilter() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.filter(
            GENERIC_AGGREGATION_NAME,
            QueryBuilders.rangeQuery(INTEGER_FIELD_DOCINDEX).lte(3000)
        ).subAggregation(AggregationBuilders.avg(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX));
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        double avgValue = getAggregationValue(getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME), AVG_AGGREGATION_NAME);
        assertEquals(1789.5, avgValue, DELTA_FOR_SCORE_ASSERTION);
    }

    private void testSumNestedIntoFilters() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.filters(
            GENERIC_AGGREGATION_NAME,
            QueryBuilders.rangeQuery(INTEGER_FIELD_DOCINDEX).lte(3000),
            QueryBuilders.termQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_WORKABLE)
        ).otherBucket(true).subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX));
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(3, buckets.size());

        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.size());
        assertEquals(2, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(3579.0, getAggregationValue(firstBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.size());
        assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(1234.0, getAggregationValue(secondBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.size());
        assertEquals(1, thirdBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(3456.0, getAggregationValue(thirdBucket, SUM_AGGREGATION_NAME), DELTA_FOR_SCORE_ASSERTION);
    }

    private void testGlobalAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);

        AggregationBuilder aggsBuilder = AggregationBuilders.global(GENERIC_AGGREGATION_NAME)
            .subAggregation(AggregationBuilders.sum(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX));

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            List.of(aggsBuilder),
            hybridQueryBuilderNeuralThenTerm,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
        double avgValue = getAggregationValue(getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME), AVG_AGGREGATION_NAME);
        assertEquals(15058.0, avgValue, DELTA_FOR_SCORE_ASSERTION);
    }

    private void testHistogramAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.histogram(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_PRICE).interval(100);

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(2, buckets.size());

        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.size());
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(0.0, (Double) firstBucket.get(KEY), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.size());
        assertEquals(2, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(100.0, (Double) secondBucket.get(KEY), DELTA_FOR_SCORE_ASSERTION);
    }

    private void testNestedAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.nested(GENERIC_AGGREGATION_NAME, NESTED_TYPE_FIELD_USER)
            .subAggregation(
                AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1)
                    .field(String.join(".", NESTED_TYPE_FIELD_USER, NESTED_FIELD_FIRSTNAME))
            );

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);

        Map<String, Object> nestedAgg = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(nestedAgg);

        assertEquals(3, nestedAgg.get(BUCKET_AGG_DOC_COUNT_FIELD));
        List<Map<String, Object>> buckets = getAggregationBuckets(nestedAgg, BUCKETS_AGGREGATION_NAME_1);

        assertNotNull(buckets);
        assertEquals(3, buckets.size());

        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(2, firstBucket.size());
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(NESTED_FIELD_FIRSTNAME_FRODO, firstBucket.get(KEY));

        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(2, secondBucket.size());
        assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(NESTED_FIELD_FIRSTNAME_JOHN, secondBucket.get(KEY));

        Map<String, Object> thirdBucket = buckets.get(2);
        assertEquals(2, thirdBucket.size());
        assertEquals(1, thirdBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(NESTED_FIELD_FIRSTNAME_SUN, thirdBucket.get(KEY));
    }

    private void testDiversifiedSampler() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.diversifiedSampler(GENERIC_AGGREGATION_NAME)
            .field(KEYWORD_FIELD_DOCKEYWORD)
            .shardSize(2)
            .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_DOCKEYWORD));

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
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
    }

    private void testAdjacencyMatrixAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.adjacencyMatrix(
            GENERIC_AGGREGATION_NAME,
            Map.of(
                "grpA",
                QueryBuilders.matchQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_WORKABLE),
                "grpB",
                QueryBuilders.matchQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_ANGRY),
                "grpC",
                QueryBuilders.matchQuery(KEYWORD_FIELD_DOCKEYWORD, KEYWORD_FIELD_DOCKEYWORD_LIKABLE)
            )
        );
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);
        assertEquals(2, buckets.size());
        Map<String, Object> grpA = buckets.get(0);
        assertEquals(1, grpA.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals("grpA", grpA.get(KEY));
        Map<String, Object> grpC = buckets.get(1);
        assertEquals(1, grpC.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals("grpC", grpC.get(KEY));
    }

    private void testDateBucketedSumsPipelinedToBucketMinMaxSumAvgAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggDateHisto = AggregationBuilders.dateHistogram(GENERIC_AGGREGATION_NAME)
            .calendarInterval(DateHistogramInterval.YEAR)
            .field(DATE_FIELD)
            .subAggregation(AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX));

        BucketMetricsPipelineAggregationBuilder<AvgBucketPipelineAggregationBuilder> aggAvgBucket = PipelineAggregatorBuilders.avgBucket(
            BUCKETS_AGGREGATION_NAME_1,
            GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME
        );

        BucketMetricsPipelineAggregationBuilder<SumBucketPipelineAggregationBuilder> aggSumBucket = PipelineAggregatorBuilders.sumBucket(
            BUCKETS_AGGREGATION_NAME_2,
            GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME
        );

        BucketMetricsPipelineAggregationBuilder<MinBucketPipelineAggregationBuilder> aggMinBucket = PipelineAggregatorBuilders.minBucket(
            BUCKETS_AGGREGATION_NAME_3,
            GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME
        );

        BucketMetricsPipelineAggregationBuilder<MaxBucketPipelineAggregationBuilder> aggMaxBucket = PipelineAggregatorBuilders.maxBucket(
            BUCKETS_AGGREGATION_NAME_4,
            GENERIC_AGGREGATION_NAME + ">" + SUM_AGGREGATION_NAME
        );

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            List.of(aggDateHisto, aggAvgBucket, aggSumBucket, aggMinBucket, aggMaxBucket),
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        assertResultsOfPipelineSumtoDateHistogramAggs(searchResponseAsMap);
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

    private void testSampler() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.sampler(GENERIC_AGGREGATION_NAME)
            .shardSize(2)
            .subAggregation(AggregationBuilders.terms(BUCKETS_AGGREGATION_NAME_1).field(KEYWORD_FIELD_DOCKEYWORD));

        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
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
    }

    private void testTermsAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.terms(GENERIC_AGGREGATION_NAME).field(KEYWORD_FIELD_DOCKEYWORD);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        List<Map<String, Object>> buckets = ((Map<String, List>) getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME)).get(
            "buckets"
        );
        assertNotNull(buckets);
        assertEquals(2, buckets.size());
        Map<String, Object> firstBucket = buckets.get(0);
        assertEquals(1, firstBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(KEYWORD_FIELD_DOCKEYWORD_LIKABLE, firstBucket.get(KEY));
        Map<String, Object> secondBucket = buckets.get(1);
        assertEquals(1, secondBucket.get(BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(KEYWORD_FIELD_DOCKEYWORD_WORKABLE, secondBucket.get(KEY));
    }

    private void testSignificantTermsAggs() throws IOException {
        prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

        AggregationBuilder aggsBuilder = AggregationBuilders.significantTerms(GENERIC_AGGREGATION_NAME).field(KEYWORD_FIELD_DOCKEYWORD);
        Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
            aggsBuilder,
            TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
        );

        Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
        assertNotNull(aggregations);
        List<Map<String, Object>> buckets = getAggregationBuckets(aggregations, GENERIC_AGGREGATION_NAME);
        assertNotNull(buckets);

        Map<String, Object> significantTermsAggregations = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);

        assertNotNull(significantTermsAggregations);
        assertEquals(3, (int) getAggregationValues(significantTermsAggregations, BUCKET_AGG_DOC_COUNT_FIELD));
        assertEquals(11, (int) getAggregationValues(significantTermsAggregations, "bg_count"));
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
            aggsBuilders
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
