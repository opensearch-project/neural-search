/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.createRandomVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import lombok.SneakyThrows;

import org.apache.commons.lang3.Range;
import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import com.google.common.primitives.Floats;

public class ScoreNormalizationCombinationIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME = "test-neural-multi-doc-one-shard-index";
    private static final String TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME = "test-neural-multi-doc-three-shards-index";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_QUERY_TEXT6 = "notexistingword";
    private static final String TEST_QUERY_TEXT7 = "notexistingwordtwo";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "Say hello and enter my friend";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final int TEST_DIMENSION = 768;
    private static final SpaceType TEST_SPACE_TYPE = SpaceType.L2;
    private static final AtomicReference<String> modelId = new AtomicReference<>();
    private static final String SEARCH_PIPELINE = "phase-results-pipeline";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector4 = createRandomVector(TEST_DIMENSION);
    private final static String RELATION_EQUAL_TO = "eq";
    private final static String RELATION_GREATER_OR_EQUAL_TO = "gte";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        modelId.compareAndSet(null, prepareModel());
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteSearchPipeline(SEARCH_PIPELINE);
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
     *                     "technique": "sum",
     *                     "parameters": {
     *                         "weights": [
     *                             0.4, 0.7
     *                         ]
     *                     }
     *                 }
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testResultProcessor_whenOneShardAndQueryMatches_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, "", modelId.get(), 5, null, null);
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
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipelineWithDefaultResultsPostProcessor(SEARCH_PIPELINE);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, "", modelId.get(), 5, null, null);
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
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        int totalExpectedDocQty = 6;

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, "", modelId.get(), 6, null, null);
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
        assertTrue(Range.between(.75f, 1.0f).contains(getMaxScore(searchResponseAsMap).get()));
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
        assertTrue(Range.between(.75f, 1.0f).contains(highestScore));
        float lowestScore = scores.stream().min(Double::compare).get().floatValue();
        assertTrue(Range.between(.0f, .5f).contains(lowestScore));

        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());
    }

    @SneakyThrows
    public void testResultProcessor_whenMultipleShardsAndNoMatches_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
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
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
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
        assertQueryResults(searchResponseAsMap, 4, true);
    }

    @SneakyThrows
    public void testArithmeticWeightedMean_whenWeightsPassed_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        // check case when number of weights and sub-queries are same
        createSearchPipeline(
            SEARCH_PIPELINE,
            NORMALIZATION_METHOD,
            COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.6f, 0.5f, 0.5f }))
        );

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4));
        hybridQueryBuilder.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT7));

        Map<String, Object> searchResponseWithWeights1AsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertWeightedScores(searchResponseWithWeights1AsMap, 1.0, 1.0, 0.001);

        // delete existing pipeline and create a new one with another set of weights
        deleteSearchPipeline(SEARCH_PIPELINE);
        createSearchPipeline(
            SEARCH_PIPELINE,
            NORMALIZATION_METHOD,
            COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.8f, 2.0f, 0.5f }))
        );

        Map<String, Object> searchResponseWithWeights2AsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertWeightedScores(searchResponseWithWeights2AsMap, 1.0, 1.0, 0.001);

        // check case when number of weights is less than number of sub-queries
        // delete existing pipeline and create a new one with another set of weights
        deleteSearchPipeline(SEARCH_PIPELINE);
        createSearchPipeline(
            SEARCH_PIPELINE,
            NORMALIZATION_METHOD,
            COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.8f }))
        );

        Map<String, Object> searchResponseWithWeights3AsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertWeightedScores(searchResponseWithWeights3AsMap, 1.0, 1.0, 0.001);

        // check case when number of weights is more than number of sub-queries
        // delete existing pipeline and create a new one with another set of weights
        deleteSearchPipeline(SEARCH_PIPELINE);
        createSearchPipeline(
            SEARCH_PIPELINE,
            NORMALIZATION_METHOD,
            COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.6f, 0.5f, 0.5f, 1.5f }))
        );

        Map<String, Object> searchResponseWithWeights4AsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertWeightedScores(searchResponseWithWeights4AsMap, 1.0, 1.0, 0.001);
    }

    private void initializeIndexIfNotExist(String indexName) throws IOException {
        if (TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME.equalsIgnoreCase(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                1
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector2).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "4",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "5",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4)
            );
            assertEquals(5, getDocCount(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME));
        }

        if (TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME.equalsIgnoreCase(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                3
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector2).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "4",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "5",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "6",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5)
            );
            assertEquals(6, getDocCount(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME));
        }
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
        assertNotNull(searchResponseAsMap);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(totalExpectedDocQty, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        if (totalExpectedDocQty > 0) {
            assertEquals(1.0, getMaxScore(searchResponseAsMap).get(), 0.001f);
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
            assertEquals(1.0, (double) scores.stream().max(Double::compare).get(), 0.001);
            if (assertMinScore) {
                assertEquals(0.001, (double) scores.stream().min(Double::compare).get(), 0.001);
            }
        }
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());
    }

    private void assertWeightedScores(
        Map<String, Object> searchResponseWithWeightsAsMap,
        double expectedMaxScore,
        double expectedMaxMinusOneScore,
        double expectedMinScore
    ) {
        assertNotNull(searchResponseWithWeightsAsMap);
        Map<String, Object> totalWeights = getTotalHits(searchResponseWithWeightsAsMap);
        assertNotNull(totalWeights.get("value"));
        assertEquals(4, totalWeights.get("value"));
        assertNotNull(totalWeights.get("relation"));
        assertEquals(RELATION_EQUAL_TO, totalWeights.get("relation"));
        assertTrue(getMaxScore(searchResponseWithWeightsAsMap).isPresent());
        assertEquals(expectedMaxScore, getMaxScore(searchResponseWithWeightsAsMap).get(), 0.001f);

        List<Double> scoresWeights = new ArrayList<>();
        for (Map<String, Object> oneHit : getNestedHits(searchResponseWithWeightsAsMap)) {
            scoresWeights.add((Double) oneHit.get("_score"));
        }
        // verify scores order
        assertTrue(IntStream.range(0, scoresWeights.size() - 1).noneMatch(idx -> scoresWeights.get(idx) < scoresWeights.get(idx + 1)));
        // verify the scores are normalized with inclusion of weights
        assertEquals(expectedMaxScore, scoresWeights.get(0), 0.001);
        assertEquals(expectedMaxMinusOneScore, scoresWeights.get(1), 0.001);
        assertEquals(expectedMinScore, scoresWeights.get(scoresWeights.size() - 1), 0.001);
    }
}
