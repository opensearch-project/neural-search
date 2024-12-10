/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;
import static org.opensearch.neuralsearch.util.TestUtils.objectToFloat;

public class NeuralSparseTwoPhaseProcessorIT extends BaseNeuralSearchIT {

    private static final String index = "two-phase-index";
    private static final String search_pipeline = "two-phase-search-pipeline";
    private final String TYPE = "neural_sparse_two_phase_processor";
    private static final String TEST_TWO_PHASE_BASIC_INDEX_NAME = "test-sparse-basic-index-two-phase";
    private static final String TEST_BASIC_INDEX_NAME = "test-sparse-basic-index";
    private static final String TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME = "test-sparse-multi-field-index";
    private static final String TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME = "test-sparse-text-and-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-sparse-nested-index";
    private static final String TEST_QUERY_TEXT = "Hello world a b";
    private static final String TEST_NEURAL_SPARSE_FIELD_NAME_1 = "test-sparse-encoding-1";
    private static final String TEST_NEURAL_SPARSE_FIELD_NAME_2 = "test-sparse-encoding-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field";
    private static final String TEST_NEURAL_SPARSE_FIELD_NAME_NESTED = "nested.neural_sparse.field";

    private static final List<String> TEST_TOKENS = List.of("hello", "world", "a", "b", "c");

    private static final Float DELTA = 1e-4f;
    private final Map<String, Float> testRankFeaturesDoc = createRandomTokenWeightMap(TEST_TOKENS);
    private static final List<String> TWO_PHASE_TEST_TOKEN = List.of("hello", "world");

    private static final Map<String, Float> testFixedQueryTokens = Map.of("hello", 5.0f, "world", 4.0f, "a", 3.0f, "b", 2.0f, "c", 1.0f);
    private static final Supplier<Map<String, Float>> testFixedQueryTokenSupplier = () -> testFixedQueryTokens;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @SneakyThrows
    public void testCreateOutOfRangePipeline_thenThrowsException() {
        expectThrows(ResponseException.class, () -> createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 1.1f, 5.0f, 1000));
        expectThrows(ResponseException.class, () -> createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.1f, 0.0f, 1000));
        expectThrows(ResponseException.class, () -> createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.1f, 3.0f, 4));
    }

    @SneakyThrows
    public void testBooleanQuery_withMultipleSparseEncodingQueries_whenTwoPhaseEnabled() {
        try {
            initializeTwoPhaseProcessor();
            initializeIndexIfNotExist(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME);
            setDefaultSearchPipelineForIndex(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            Map<String, Float> randomTokenWeight = createRandomTokenWeightMap(TWO_PHASE_TEST_TOKEN);
            Supplier<Map<String, Float>> randomTokenWeightSupplier = () -> randomTokenWeight;
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(randomTokenWeightSupplier);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_2)
                .queryTokensSupplier(randomTokenWeightSupplier);
            boolQueryBuilder.should(sparseEncodingQueryBuilder1).should(sparseEncodingQueryBuilder2);

            Map<String, Object> searchResponseAsMap = search(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, randomTokenWeightSupplier.get());
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, null, null, search_pipeline);
        }
    }

    @SneakyThrows
    private void initializeTwoPhaseProcessor() {
        createNeuralSparseTwoPhaseSearchProcessor(search_pipeline);
    }

    @SneakyThrows
    private void setDefaultSearchPipelineForIndex(String indexName) {
        updateIndexSettings(indexName, Settings.builder().put("index.search.default_pipeline", search_pipeline));
    }

    /**
     * Tests the neuralSparseQuery when twoPhase enabled with DSL query:
     * {
     *     "query": {
     *         "bool": {
     *             "should": [
     *                 {
     *                     "neural_sparse": {
     *                         "field": "test-sparse-encoding-1",
     *                         "query_tokens": testFixedQueryTokens,
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0
     *                     }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryTokens_whenTwoPhaseEnabled_thenGetExpectedScore() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            initializeTwoPhaseProcessor();
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, testFixedQueryTokens);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    @SneakyThrows
    public void testBasicQueryUsingQueryTokens_whenTwoPhaseEnabledAndDisabled_thenGetSameScore() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            initializeTwoPhaseProcessor();

            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float scoreWithoutTwoPhase = objectToFloat(firstInnerHit.get("_score"));

            sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(2.0f);
            searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float scoreWithTwoPhase = objectToFloat(firstInnerHit.get("_score"));
            assertEquals(scoreWithTwoPhase, scoreWithoutTwoPhase, DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    /**
     * Tests neuralSparseQuery as rescoreQuery with DSL query:
     * {
     *     "query": {
     *         "match_all": {}
     *     },
     *     "rescore": {
     *         "query": {
     *             "bool": {
     *                 "should": [
     *                     {
     *                         "neural_sparse": {
     *                             "field": "test-sparse-encoding-1",
     *                             "query_tokens": testFixedQueryTokens,
     *                             "model_id": "dcsdcasd",
     *                             "boost": 2.0
     *                         }
     *                     }
     *                 ]
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testNeuralSparseQueryAsRescoreQuery_whenTwoPhase_thenGetExpectedScore() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            initializeTwoPhaseProcessor();

            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(2.0f);
            QueryBuilder queryBuilder = new MatchAllQueryBuilder();
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, queryBuilder, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, testFixedQueryTokens);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    /**
     * Tests multi neuralSparseQuery in BooleanQuery with DSL query:
     * {
     *     "query": {
     *         "bool": {
     *             "should": [
     *                 {
     *                     "neural_sparse": {
     *                         "field": "test-sparse-encoding-1",
     *                         "query_tokens": testFixedQueryTokens,
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0
     *                     }
     *                 },
     *                 {
     *                     "neural_sparse": {
     *                         "field": "test-sparse-encoding-1",
     *                         "query_tokens": testFixedQueryTokens,
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0
     *                     }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhase_thenGetExpectedScore() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            initializeTwoPhaseProcessor();
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(2.0f);
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 4 * computeExpectedScore(testRankFeaturesDoc, testFixedQueryTokens);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    /**
    * This test case aim to test different score caused by different two-phase parameters.
    * First, with a default parameter, two-phase get same score at most times.
    * Second, With a high ratio, there may some docs including lots of low score tokens are missed.
    * And then, lower ratio or higher windows size can improve accuracy.
    */
    @SneakyThrows
    public void testNeuralSparseQuery_whenDifferentTwoPhaseParameter_thenGetDifferentResult() {
        try {
            initializeIndexIfNotExist(TEST_TWO_PHASE_BASIC_INDEX_NAME);
            Map<String, Float> queryToken = new HashMap<>();
            for (int i = 1; i < 6; i++) {
                queryToken.put(String.valueOf(i + 10), (float) i);
            }
            for (int i = 1; i < 8; i++) {
                queryToken.put(String.valueOf(i), (float) i);
            }
            Supplier<Map<String, Float>> queryTokenSupplier = () -> queryToken;

            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(queryTokenSupplier);
            assertSearchScore(sparseEncodingQueryBuilder, TEST_TWO_PHASE_BASIC_INDEX_NAME, 110);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.3f, 1f, 10000);
            setDefaultSearchPipelineForIndex(TEST_TWO_PHASE_BASIC_INDEX_NAME);
            assertSearchScore(sparseEncodingQueryBuilder, TEST_TWO_PHASE_BASIC_INDEX_NAME, 110);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.7f, 1f, 10000);
            setDefaultSearchPipelineForIndex(TEST_TWO_PHASE_BASIC_INDEX_NAME);
            assertSearchScore(sparseEncodingQueryBuilder, TEST_TWO_PHASE_BASIC_INDEX_NAME, 61);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.7f, 30f, 10000);
            setDefaultSearchPipelineForIndex(TEST_TWO_PHASE_BASIC_INDEX_NAME);
            assertSearchScore(sparseEncodingQueryBuilder, TEST_TWO_PHASE_BASIC_INDEX_NAME, 110);
        } finally {
            wipeOfTestResources(TEST_TWO_PHASE_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhaseAndFilter_thenGetExpectedScore() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, .8f, 5f, 1000);
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(2.0f);
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            boolQueryBuilder.filter(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, testFixedQueryTokens);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhaseAndMultiBoolean_thenGetExpectedScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);

            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, .6f, 5f, 1000);
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(1.0f);
            boolQueryBuilder.should(sparseEncodingQueryBuilder1);
            boolQueryBuilder.should(sparseEncodingQueryBuilder1);
            BoolQueryBuilder subBoolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder3 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(3.0f);
            subBoolQueryBuilder.should(sparseEncodingQueryBuilder2);
            subBoolQueryBuilder.should(sparseEncodingQueryBuilder3);
            subBoolQueryBuilder.boost(2.0f);
            boolQueryBuilder.should(subBoolQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 12 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhaseAndNoLowScoreToken_thenGetExpectedScore() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, .6f, 5f, 1000);
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            Map<String, Float> queryTokens = new HashMap<>();
            queryTokens.put("hello", 10.0f);
            queryTokens.put("world", 10.0f);
            queryTokens.put("a", 10.0f);
            queryTokens.put("b", 10.0f);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(() -> queryTokens)
                .boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, sparseEncodingQueryBuilder.queryTokensSupplier().get());
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, null);
        }
    }

    @SneakyThrows
    public void testNeuralSParseQuery_whenTwoPhaseAndNestedInConstantScoreQuery_thenSuccess() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.6f, 5f, 10000);
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(1.0f);
            ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, constantScoreQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            assertEquals(1.0f, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    @SneakyThrows
    public void testNeuralSParseQuery_whenTwoPhaseAndNestedInDisjunctionMaxQuery_thenSuccess() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.6f, 5f, 10000);
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(5.0f);
            DisMaxQueryBuilder disMaxQueryBuilder = new DisMaxQueryBuilder();
            disMaxQueryBuilder.add(sparseEncodingQueryBuilder);
            disMaxQueryBuilder.add(new MatchAllQueryBuilder());
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, disMaxQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 5f * computeExpectedScore(testRankFeaturesDoc, testFixedQueryTokens);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    @SneakyThrows
    public void testNeuralSparseQuery_whenTwoPhaseAndNestedInFunctionScoreQuery_thenSuccess() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            createNeuralSparseTwoPhaseSearchProcessor(search_pipeline, 0.6f, 5f, 10000);
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(testFixedQueryTokenSupplier)
                .boost(5.0f);
            FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(sparseEncodingQueryBuilder);
            functionScoreQueryBuilder.boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, functionScoreQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 10f * computeExpectedScore(testRankFeaturesDoc, testFixedQueryTokens);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, search_pipeline);
        }
    }

    /**
     * Tests multi neuralSparseQuery in BooleanQuery with DSL query:
     * {
     *     "query": {
     *         "bool": {
     *             "should": [
     *                 {
     *                     "neural_sparse": {
     *                         "field": "test-sparse-encoding-1",
     *                         "query_text": "Hello world a b",
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0
     *                     }
     *                 },
     *                 {
     *                     "neural_sparse": {
     *                         "field": "test-sparse-encoding-1",
     *                         "query_text": "Hello world a b",
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0
     *                     }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhaseAndModelInference_thenGetExpectedScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            initializeTwoPhaseProcessor();
            setDefaultSearchPipelineForIndex(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(3.0f);
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 6 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, search_pipeline);
        }
    }

    @SneakyThrows
    protected void initializeIndexIfNotExist(String indexName) {
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(indexName)) {
            prepareSparseEncodingIndex(indexName, List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1));
            addSparseEncodingDoc(indexName, "1", List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1), List.of(testRankFeaturesDoc));
            assertEquals(1, getDocCount(indexName));
        }

        if (TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME.equals(indexName) && !indexExists(indexName)) {
            prepareSparseEncodingIndex(indexName, List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1, TEST_NEURAL_SPARSE_FIELD_NAME_2));
            addSparseEncodingDoc(
                indexName,
                "1",
                List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1, TEST_NEURAL_SPARSE_FIELD_NAME_2),
                List.of(testRankFeaturesDoc, testRankFeaturesDoc)
            );
            assertEquals(1, getDocCount(indexName));
        }

        if (TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME.equals(indexName) && !indexExists(indexName)) {
            prepareSparseEncodingIndex(indexName, List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1));
            addSparseEncodingDoc(
                indexName,
                "1",
                List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1),
                List.of(testRankFeaturesDoc),
                List.of(TEST_TEXT_FIELD_NAME_1),
                List.of(TEST_QUERY_TEXT)
            );
            assertEquals(1, getDocCount(indexName));
        }

        if (TEST_NESTED_INDEX_NAME.equals(indexName) && !indexExists(indexName)) {
            prepareSparseEncodingIndex(indexName, List.of(TEST_NEURAL_SPARSE_FIELD_NAME_NESTED));
            addSparseEncodingDoc(indexName, "1", List.of(TEST_NEURAL_SPARSE_FIELD_NAME_NESTED), List.of(testRankFeaturesDoc));
            assertEquals(1, getDocCount(TEST_NESTED_INDEX_NAME));
        }

        if (TEST_TWO_PHASE_BASIC_INDEX_NAME.equals(indexName) && !indexExists(indexName)) {
            Map<String, Float> twoPhaseRandFeatures = new HashMap<>();
            Map<String, Float> normalRandFeatures = new HashMap<>();
            prepareSparseEncodingIndex(indexName, List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1));
            // put [(5,5.0), (6,6.0)] into twoPhaseRandFeatures
            for (int i = 5; i < 7; i++) {
                twoPhaseRandFeatures.put(String.valueOf(i), (float) i);
            }

            // put 10 token [(1,1.0),(11,1.0),....(5,5.0),(55,5.0)] into normalRandFeatures
            for (int i = 1; i < 6; i++) {
                normalRandFeatures.put(String.valueOf(i), (float) i);
                normalRandFeatures.put(String.valueOf(10 + i), (float) i);

            }

            for (int i = 0; i < 10; i++) {
                addSparseEncodingDoc(indexName, String.valueOf(i), List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1), List.of(normalRandFeatures));
                addSparseEncodingDoc(
                    indexName,
                    String.valueOf(i + 10),
                    List.of(TEST_NEURAL_SPARSE_FIELD_NAME_1),
                    List.of(twoPhaseRandFeatures)
                );
                ;
            }
            assertEquals(20, getDocCount(indexName));
        }
    }

    private void assertSearchScore(NeuralSparseQueryBuilder builder, String indexName, float expectedScore) {
        Map<String, Object> searchResponse = search(indexName, builder, 10);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponse);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
    }

}
