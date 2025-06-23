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
import static org.opensearch.neuralsearch.util.TestUtils.assertWeightedScores;
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

public class ScoreCombinationIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME = "test-score-combination-neural-multi-doc-one-shard-index";
    private static final String TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME = "test-score-combination-neural-multi-doc-three-shards-index";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT7 = "notexistingwordtwo";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "Say hello and enter my friend";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String SEARCH_PIPELINE = "phase-results-score-combination-pipeline";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector4 = createRandomVector(TEST_DIMENSION);

    private static final String L2_NORMALIZATION_METHOD = "l2";
    private static final String HARMONIC_MEAN_COMBINATION_METHOD = "harmonic_mean";
    private static final String GEOMETRIC_MEAN_COMBINATION_METHOD = "geometric_mean";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
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
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
        // check case when number of weights and sub-queries are same
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.4f, 0.3f, 0.3f })),
            false
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
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.233f, 0.666f, 0.1f })),
            false
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
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 1.0f })),
            false
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
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.25f, 0.25f, 0.2f })),
            false
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
        String modelId = null;
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        modelId = prepareModel();
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            HARMONIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f })),
            false
        );

        HybridQueryBuilder hybridQueryBuilderDefaultNorm = new HybridQueryBuilder();
        hybridQueryBuilderDefaultNorm.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()
        );
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
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f })),
            false
        );

        HybridQueryBuilder hybridQueryBuilderL2Norm = new HybridQueryBuilder();
        hybridQueryBuilderL2Norm.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()

        );
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
        String modelId = null;
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
        modelId = prepareModel();
        createSearchPipeline(
            SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            GEOMETRIC_MEAN_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f })),
            false
        );

        HybridQueryBuilder hybridQueryBuilderDefaultNorm = new HybridQueryBuilder();
        hybridQueryBuilderDefaultNorm.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()

        );
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
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.533f, 0.466f })),
            false
        );

        HybridQueryBuilder hybridQueryBuilderL2Norm = new HybridQueryBuilder();
        hybridQueryBuilderL2Norm.add(
            NeuralQueryBuilder.builder().fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).queryText(TEST_DOC_TEXT1).modelId(modelId).k(5).build()

        );
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
}
