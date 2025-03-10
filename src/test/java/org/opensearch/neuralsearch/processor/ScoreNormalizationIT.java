/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.assertHybridSearchResults;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

public class ScoreNormalizationIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME = "test-score-normalization-neural-multi-doc-one-shard-index";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String SEARCH_PIPELINE = "phase-results-normalization-pipeline";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector4 = createRandomVector(TEST_DIMENSION);

    private static final String L2_NORMALIZATION_METHOD = "l2";
    private static final String Z_SCORE_NORMALIZATION_METHOD = "z_score";
    private static final String HARMONIC_MEAN_COMBINATION_METHOD = "harmonic_mean";
    private static final String GEOMETRIC_MEAN_COMBINATION_METHOD = "geometric_mean";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
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
        String modelId = null;
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        modelId = prepareModel();
        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderArithmeticMean = new HybridQueryBuilder();
        hybridQueryBuilderArithmeticMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
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

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderHarmonicMean = new HybridQueryBuilder();
        hybridQueryBuilderHarmonicMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
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

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            L2_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderGeometricMean = new HybridQueryBuilder();
        hybridQueryBuilderGeometricMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
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
     *                     "technique": "min_max"
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
        String modelId = null;
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        modelId = prepareModel();
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderArithmeticMean = new HybridQueryBuilder();
        hybridQueryBuilderArithmeticMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
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

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderHarmonicMean = new HybridQueryBuilder();
        hybridQueryBuilderHarmonicMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
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

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderGeometricMean = new HybridQueryBuilder();
        hybridQueryBuilderGeometricMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
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

    /**
     * Using search pipelines with config for z score norm:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "z_score"
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
    public void testZScoreNorm_whenOneShardAndQueryMatches_thenSuccessful() {
        String modelId = null;
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        modelId = prepareModel();
        createSearchPipeline(
            SEARCH_PIPELINE,
            Z_SCORE_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
        );

        HybridQueryBuilder hybridQueryBuilderArithmeticMean = new HybridQueryBuilder();
        hybridQueryBuilderArithmeticMean.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
        );
        hybridQueryBuilderArithmeticMean.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));

        Map<String, Object> searchResponseAsMapArithmeticMean = search(
            TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
            hybridQueryBuilderArithmeticMean,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertHybridSearchResults(searchResponseAsMapArithmeticMean, 5, new float[] { 0.4f, 2.0f });

        deleteSearchPipeline(SEARCH_PIPELINE);

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);

        ResponseException exceptionWithHP = expectThrows(
            ResponseException.class,
            () -> createSearchPipeline(
                SEARCH_PIPELINE,
                Z_SCORE_NORMALIZATION_METHOD,
                HARMONIC_MEAN_COMBINATION_METHOD,
                Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            exceptionWithHP.getMessage(),
            allOf(
                containsString("Z Score supports only arithmetic_mean combination technique"),
                containsString("illegal_argument_exception")
            )
        );

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);

        ResponseException exceptionWithGP = expectThrows(
            ResponseException.class,
            () -> createSearchPipeline(
                SEARCH_PIPELINE,
                Z_SCORE_NORMALIZATION_METHOD,
                GEOMETRIC_MEAN_COMBINATION_METHOD,
                Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f }))
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            exceptionWithGP.getMessage(),
            allOf(
                containsString("Z Score supports only arithmetic_mean combination technique"),
                containsString("illegal_argument_exception")
            )
        );
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
    }
}
