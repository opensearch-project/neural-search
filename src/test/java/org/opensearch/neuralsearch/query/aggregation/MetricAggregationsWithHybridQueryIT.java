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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValue;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregationValues;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getAggregations;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;

/**
 * Integration tests for metric type aggregations when they are bundled with hybrid query
 * Below is list of metric aggregations that are present in this test:
 * - Average
 * - Cardinality
 * - Extended stats
 * - Top hits
 * - Percentile ranks
 * - Scripted metric
 * - Sum
 * - Value count
 *
 * Following metric aggs are tested by other integ tests
 * - Maximum
 *
 *
 * Below aggregations are not part of any test
 * - Geobounds
 * - Matrix stats
 * - Minimum
 * - Percentile
 * - Stats
 */
public class MetricAggregationsWithHybridQueryIT extends BaseAggregationsWithHybridQueryIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS = "test-aggs-metric-multi-doc-index-multiple-shards";
    private static final String SEARCH_PIPELINE = "search-pipeline-metric-aggs";

    /**
     * Tests complex query with multiple nested sub-queries:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "term": {
     *                          "text": "word1"
     *                       }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      },
     *     "aggs": {
     *         "max_index": {
     *             "max": {
     *                 "field": "doc_index"
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenAvgAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testAvgAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenCardinalityAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testCardinalityAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenCardinalityAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testCardinalityAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenExtendedStatsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testExtendedStatsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenExtendedStatsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testExtendedStatsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenTopHitsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testTopHitsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenTopHitsAggs_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testTopHitsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenPercentileRank_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testPercentileRankAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenPercentileRank_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testPercentileRankAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenPercentile_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testPercentileAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenPercentile_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testPercentileAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenScriptedMetrics_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testScriptedMetricsAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenScriptedMetrics_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testScriptedMetricsAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenSumAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testSumAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenSumAgg_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testSumAggs();
    }

    @SneakyThrows
    public void testMetricAggs_whenValueCount_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        testValueCountAggs();
    }

    @SneakyThrows
    public void testWithConcurrentSegmentSearch_whenValueCount_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        testValueCountAggs();
    }

    private void testAvgAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.avg(AVG_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(AVG_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, AVG_AGGREGATION_NAME);
            assertEquals(maxAggsValue, 2345.0, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testCardinalityAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.cardinality(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            int aggsValue = getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(aggsValue, 3);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testExtendedStatsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.extendedStats(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Object> extendedStatsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(extendedStatsValues);

            assertEquals((double) extendedStatsValues.get("max"), 3456.0, DELTA_FOR_SCORE_ASSERTION);
            assertEquals((int) extendedStatsValues.get("count"), 3);
            assertEquals((double) extendedStatsValues.get("sum"), 7035.0, DELTA_FOR_SCORE_ASSERTION);
            assertEquals((double) extendedStatsValues.get("avg"), 2345.0, DELTA_FOR_SCORE_ASSERTION);
            assertEquals((double) extendedStatsValues.get("variance"), 822880.666, DELTA_FOR_SCORE_ASSERTION);
            assertEquals((double) extendedStatsValues.get("std_deviation"), 907.127, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testTopHitsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.topHits(GENERIC_AGGREGATION_NAME).size(4);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Object> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(aggsValues);
            assertHitResultsFromQuery(3, aggsValues);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testScriptedMetricsAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            // compute sum of all int fields that are not blank
            AggregationBuilder aggsBuilder = AggregationBuilders.scriptedMetric(GENERIC_AGGREGATION_NAME)
                .initScript(new Script("state.price = []"))
                .mapScript(
                    new Script(
                        "state.price.add(doc[\""
                            + INTEGER_FIELD_DOCINDEX
                            + "\"].size() == 0 ? 0 : doc."
                            + INTEGER_FIELD_DOCINDEX
                            + ".value)"
                    )
                )
                .combineScript(new Script("state.price.stream().mapToInt(Integer::intValue).sum()"))
                .reduceScript(new Script("states.stream().mapToInt(Integer::intValue).sum()"));
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            int aggsValue = getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME);
            assertEquals(7035, aggsValue);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testPercentileAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.percentiles(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertHitResultsFromQuery(3, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Map<String, Double>> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(aggsValues);

            Map<String, Double> values = aggsValues.get("values");
            assertNotNull(values);
            assertEquals(7, values.size());
            assertEquals(1234.0, values.get("1.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1234.0, values.get("5.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(1511.75, values.get("25.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(2345.0, values.get("50.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3178.25, values.get("75.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3456.0, values.get("95.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(3456.0, values.get("99.0"), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testPercentileRankAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.percentileRanks(GENERIC_AGGREGATION_NAME, new double[] { 2000, 3000 })
                .field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertHitResultsFromQuery(3, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            Map<String, Map<String, Double>> aggsValues = getAggregationValues(aggregations, GENERIC_AGGREGATION_NAME);
            assertNotNull(aggsValues);
            Map<String, Double> values = aggsValues.get("values");
            assertNotNull(values);
            assertEquals(39.648, values.get("2000.0"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(100.0, values.get("3000.0"), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testSumAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(SUM_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, SUM_AGGREGATION_NAME);
            assertEquals(7035.0, maxAggsValue, DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testValueCountAggs() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);
            AggregationBuilder aggsBuilder = AggregationBuilders.count(GENERIC_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);
            Map<String, Object> searchResponseAsMap = executeQueryAndGetAggsResults(
                aggsBuilder,
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS
            );

            assertHitResultsFromQuery(3, searchResponseAsMap);

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);

            assertTrue(aggregations.containsKey(GENERIC_AGGREGATION_NAME));
            assertEquals(3, (int) getAggregationValue(aggregations, GENERIC_AGGREGATION_NAME));
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
    }

    private void testSumAggsAndRangePostFilter() throws IOException {
        try {
            prepareResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, SEARCH_PIPELINE);

            AggregationBuilder aggsBuilder = AggregationBuilders.sum(SUM_AGGREGATION_NAME).field(INTEGER_FIELD_DOCINDEX);

            TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
            TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
            TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);

            HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder2);
            hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder3);

            QueryBuilder rangeFilterQuery = QueryBuilders.rangeQuery(INTEGER_FIELD_DOCINDEX).gte(3000).lte(5000);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS,
                hybridQueryBuilderNeuralThenTerm,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                List.of(aggsBuilder),
                rangeFilterQuery,
                null,
                false,
                null
            );

            Map<String, Object> aggregations = getAggregations(searchResponseAsMap);
            assertNotNull(aggregations);
            assertTrue(aggregations.containsKey(SUM_AGGREGATION_NAME));
            double maxAggsValue = getAggregationValue(aggregations, SUM_AGGREGATION_NAME);
            assertEquals(11602.0, maxAggsValue, DELTA_FOR_SCORE_ASSERTION);

            assertHitResultsFromQuery(2, searchResponseAsMap);

            // assert post-filter
            List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);

            List<Integer> docIndexes = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                assertNotNull(oneHit.get("_source"));
                Map<String, Object> source = (Map<String, Object>) oneHit.get("_source");
                int docIndex = (int) source.get(INTEGER_FIELD_DOCINDEX);
                docIndexes.add(docIndex);
            }
            assertEquals(0, docIndexes.stream().filter(docIndex -> docIndex < 3000 || docIndex > 5000).count());
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, null, null, SEARCH_PIPELINE);
        }
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
