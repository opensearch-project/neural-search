/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.GEOMETRIC_MEAN_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.HARMONIC_MEAN_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.L2_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.TestUtils.SEARCH_PIPELINE;
import static org.opensearch.neuralsearch.TestUtils.TEST_DOC_TEXT1;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT3;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.assertHybridSearchResults;

import java.util.Arrays;
import java.util.Map;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class ScoreNormalizationIT extends BaseNeuralSearchIT {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        prepareModel();
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteSearchPipeline(SEARCH_PIPELINE);
        findDeployedModels().forEach(this::deleteModel);
    }

    @Override
    public boolean isUpdateClusterSettings() {
        return false;
    }

    /**
     * Using search pipelines with config for l2 norm:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "l2"
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
    public void testL2Norm_whenOneShardAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );
        String modelId = getDeployedModelId();

        HybridQueryBuilder hybridQueryBuilderArithmeticMean = new HybridQueryBuilder();
        hybridQueryBuilderArithmeticMean.add(
            new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null)
        );
        hybridQueryBuilderArithmeticMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapArithmeticMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderArithmeticMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapArithmeticMean, 5, new float[] { 0.6f, 1.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderHarmonicMean = new HybridQueryBuilder();
        hybridQueryBuilderHarmonicMean.add(
            new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null)
        );
        hybridQueryBuilderHarmonicMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapHarmonicMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderHarmonicMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapHarmonicMean, 5, new float[] { 0.5f, 1.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderGeometricMean = new HybridQueryBuilder();
        hybridQueryBuilderGeometricMean.add(
            new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null)
        );
        hybridQueryBuilderGeometricMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapGeometricMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderGeometricMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapGeometricMean, 5, new float[] { 0.5f, 1.0f });
    }

    /**
     * Using search pipelines with config for min-max norm:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "l2"
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
    public void testMinMaxNorm_whenOneShardAndQueryMatches_thenSuccessful() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );
        String modelId = getDeployedModelId();

        HybridQueryBuilder hybridQueryBuilderArithmeticMean = new HybridQueryBuilder();
        hybridQueryBuilderArithmeticMean.add(
            new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null)
        );
        hybridQueryBuilderArithmeticMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapArithmeticMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderArithmeticMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapArithmeticMean, 5, new float[] { 0.5f, 1.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderHarmonicMean = new HybridQueryBuilder();
        hybridQueryBuilderHarmonicMean.add(
            new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null)
        );
        hybridQueryBuilderHarmonicMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapHarmonicMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderHarmonicMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapHarmonicMean, 5, new float[] { 0.6f, 1.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderGeometricMean = new HybridQueryBuilder();
        hybridQueryBuilderGeometricMean.add(
            new NeuralQueryBuilder(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DOC_TEXT1, "", modelId, 5, null, null)
        );
        hybridQueryBuilderGeometricMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapGeometricMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderGeometricMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );
        assertHybridSearchResults(searchResponseAsMapGeometricMean, 5, new float[] { 0.6f, 1.0f });
    }
}
