/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.neuralsearch.TestUtils.GEOMETRIC_MEAN_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.HARMONIC_MEAN_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.L2_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.SEARCH_PIPELINE;
import static org.opensearch.neuralsearch.TestUtils.TEST_DOC_TEXT1;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT3;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT4;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT7;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.assertHybridSearchResults;
import static org.opensearch.neuralsearch.TestUtils.assertWeightedScores;

import java.util.Arrays;
import java.util.Map;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class ScoreCombinationIT extends BaseNeuralSearchIT {

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
     *                     "technique": "arithmetic_mean",
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
    public void testArithmeticWeightedMean_whenWeightsPassed_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        // check case when number of weights and sub-queries are same
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.4f, 0.3f, 0.3f }))
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

        assertWeightedScores(searchResponseWithWeights1AsMap, 0.4, 0.3, 0.001);

        // delete existing pipeline and create a new one with another set of weights
        deleteSearchPipeline(SEARCH_PIPELINE);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.233f, 0.666f, 0.1f }))
        );

        Map<String, Object> searchResponseWithWeights2AsMap = search(
            TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertWeightedScores(searchResponseWithWeights2AsMap, 0.6666, 0.2332, 0.001);

        // check case when number of weights is less than number of sub-queries
        // delete existing pipeline and create a new one with another set of weights
        deleteSearchPipeline(SEARCH_PIPELINE);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 1.0f }))
        );

        ResponseException exception1 = expectThrows(
            ResponseException.class,
            () -> search(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME, hybridQueryBuilder, null, 5, Map.of("search_pipeline", SEARCH_PIPELINE))
        );
        org.hamcrest.MatcherAssert.assertThat(
            exception1.getMessage(),
            allOf(
                containsString("number of weights"),
                containsString("must match number of sub-queries"),
                containsString("in hybrid query")
            )
        );

        // check case when number of weights is more than number of sub-queries
        // delete existing pipeline and create a new one with another set of weights
        deleteSearchPipeline(SEARCH_PIPELINE);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.25f, 0.25f, 0.2f }))
        );

        ResponseException exception2 = expectThrows(
            ResponseException.class,
            () -> search(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME, hybridQueryBuilder, null, 5, Map.of("search_pipeline", SEARCH_PIPELINE))
        );
        org.hamcrest.MatcherAssert.assertThat(
            exception2.getMessage(),
            allOf(
                containsString("number of weights"),
                containsString("must match number of sub-queries"),
                containsString("in hybrid query")
            )
        );
    }

    /**
     * Using search pipelines with config for harmonic mean:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "l2"
     *                 },
     *                 "combination": {
     *                     "technique": "harmonic_mean"
     *                 }
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testHarmonicMeanCombination_whenOneShardAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );
        String modelId = getDeployedModelId();

        HybridQueryBuilder hybridQueryBuilderDefaultNorm = new HybridQueryBuilder();
        hybridQueryBuilderDefaultNorm.add(new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null));
        hybridQueryBuilderDefaultNorm.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapDefaultNorm = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderDefaultNorm,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertHybridSearchResults(searchResponseAsMapDefaultNorm, 5, new float[] { 0.5f, 1.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderL2Norm = new HybridQueryBuilder();
        hybridQueryBuilderL2Norm.add(new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null));
        hybridQueryBuilderL2Norm.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapL2Norm = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderL2Norm,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapL2Norm, 5, new float[] { 0.5f, 1.0f });
    }

    /**
     * Using search pipelines with config for geometric mean:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "l2"
     *                 },
     *                 "combination": {
     *                     "technique": "geometric_mean"
     *                 }
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testGeometricMeanCombination_whenOneShardAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );
        String modelId = getDeployedModelId();

        HybridQueryBuilder hybridQueryBuilderDefaultNorm = new HybridQueryBuilder();
        hybridQueryBuilderDefaultNorm.add(new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null));
        hybridQueryBuilderDefaultNorm.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapDefaultNorm = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderDefaultNorm,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertHybridSearchResults(searchResponseAsMapDefaultNorm, 5, new float[] { 0.5f, 1.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderL2Norm = new HybridQueryBuilder();
        hybridQueryBuilderL2Norm.add(new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null));
        hybridQueryBuilderL2Norm.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapL2Norm = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderL2Norm,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapL2Norm, 5, new float[] { 0.5f, 1.0f });
    }
}
