/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.TestUtils.SEARCH_PIPELINE;
import static org.opensearch.neuralsearch.TestUtils.TEST_DOC_TEXT1;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT3;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT4;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT6;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT7;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_FIELD_NAME_1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import lombok.SneakyThrows;

import org.apache.commons.lang3.Range;
import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class NormalizationProcessorIT extends BaseNeuralSearchIT {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        prepareModel();
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteSearchPipeline(SEARCH_PIPELINE);
        findDeployedModels().forEach(this::deleteModel);
    }

    /**
     * Using search pipelines with result processor configs like below:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "min-max"
     *                 },
     *                 "combination": {
     *                     "technique": "arithmetic_mean"
     *                 }
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testResultProcessor_whenOneShardAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        String modelId = getDeployedModelId();

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_DOC_TEXT1,
            "",
            modelId,
            5,
            null,
            null
        );
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(neuralQueryBuilder);
        hybridQueryBuilder.add(termQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertQueryResults(searchResponseAsMap, 5, false);
    }

    /**
     * Using search pipelines with default result processor configs:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testResultProcessor_whenDefaultProcessorConfigAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipelineWithDefaultResultsPostProcessor(SEARCH_PIPELINE);
        String modelId = getDeployedModelId();

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_DOC_TEXT1,
            "",
            modelId,
            5,
            null,
            null
        );
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(neuralQueryBuilder);
        hybridQueryBuilder.add(termQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertQueryResults(searchResponseAsMap, 5, false);
    }

    @SneakyThrows
    public void testResultProcessor_whenMultipleShardsAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        String modelId = getDeployedModelId();
        int totalExpectedDocQty = 6;

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_DOC_TEXT1,
            "",
            modelId,
            6,
            null,
            null
        );
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(neuralQueryBuilder);
        hybridQueryBuilder.add(termQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            6,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertNotNull(searchResponseAsMap);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(totalExpectedDocQty, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        assertTrue(Range.between(.5f, 1.0f).contains(getMaxScore(searchResponseAsMap).get()));
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }
        // verify scores order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));

        // verify the scores are normalized. we need special assert logic because combined score may vary as neural search query
        // based on random vectors and return results for every doc. In some cases that may affect 1.0 score from term query and make it
        // lower.
        float highestScore = scores.stream().max(Double::compare).get().floatValue();
        assertTrue(Range.between(.5f, 1.0f).contains(highestScore));
        float lowestScore = scores.stream().min(Double::compare).get().floatValue();
        assertTrue(Range.between(.0f, .5f).contains(lowestScore));

        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());
    }

    @SneakyThrows
    public void testResultProcessor_whenMultipleShardsAndNoMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT6));
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT7));

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertQueryResults(searchResponseAsMap, 0, true);
    }

    @SneakyThrows
    public void testResultProcessor_whenMultipleShardsAndPartialMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4));
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT7));

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertQueryResults(searchResponseAsMap, 4, true, Range.between(0.33f, 1.0f));
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

    private void assertQueryResults(Map<String, Object> searchResponseAsMap, int totalExpectedDocQty, boolean assertMinScore) {
        assertQueryResults(searchResponseAsMap, totalExpectedDocQty, assertMinScore, Range.between(0.5f, 1.0f));
    }

    private void assertQueryResults(
        Map<String, Object> searchResponseAsMap,
        int totalExpectedDocQty,
        boolean assertMinScore,
        Range<Float> maxScoreRange
    ) {
        assertNotNull(searchResponseAsMap);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(totalExpectedDocQty, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        if (totalExpectedDocQty > 0) {
            assertTrue(maxScoreRange.contains(getMaxScore(searchResponseAsMap).get()));
        } else {
            assertEquals(0.0, getMaxScore(searchResponseAsMap).get(), 0.001f);
        }

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }
        // verify scores order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify the scores are normalized
        if (totalExpectedDocQty > 0) {
            assertTrue(maxScoreRange.contains(scores.stream().max(Double::compare).get().floatValue()));
            if (assertMinScore) {
                assertEquals(0.001, (double) scores.stream().min(Double::compare).get(), 0.001);
            }
        }
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());
    }
}
