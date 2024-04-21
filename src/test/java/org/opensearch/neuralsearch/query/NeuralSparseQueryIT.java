/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import static org.opensearch.neuralsearch.TestUtils.createRandomTokenWeightMap;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.TestUtils;

import lombok.SneakyThrows;

public class NeuralSparseQueryIT extends BaseNeuralSearchIT {

    private static final String TWO_PHASE_ENABLED_SETTING_KEY = "plugins.neural_search.neural_sparse.two_phase.default_enabled";
    private static final String TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY =
        "plugins.neural_search.neural_sparse.two_phase.default_window_size_expansion";
    private static final String TWO_PHASE_PRUNE_RATIO_SETTING_KEY = "plugins.neural_search.neural_sparse.two_phase.default_pruning_ratio";
    private static final String TWO_PHASE_MAX_WINDOW_SIZE_SETTING_KEY = "plugins.neural_search.neural_sparse.two_phase.max_window_size";
    private static final String TEST_BASIC_INDEX_NAME = "test-sparse-basic-index";
    private static final String TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME = "test-sparse-multi-field-index";
    private static final String TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME = "test-sparse-text-and-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-sparse-nested-index";
    private static final String TEST_QUERY_TEXT = "Hello world a b";
    private static final String TEST_QUERY_TEXT_V2 = "Hello world a b cat cache dog war pop nice nick neck question";
    private static final String TEST_NEURAL_SPARSE_FIELD_NAME_1 = "test-sparse-encoding-1";
    private static final String TEST_NEURAL_SPARSE_FIELD_NAME_2 = "test-sparse-encoding-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field";
    private static final String TEST_NEURAL_SPARSE_FIELD_NAME_NESTED = "nested.neural_sparse.field";

    private static final List<String> TEST_TOKENS = List.of("hello", "world", "a", "b", "c");

    private static final Float DELTA = 1e-5f;
    private final Map<String, Float> testRankFeaturesDoc = TestUtils.createRandomTokenWeightMap(TEST_TOKENS);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @SneakyThrows
    private void updateTwoPhaseClusterSettings(boolean enabled, float windowSizeExpansion, float ratio, int maxWindowSize) {
        updateClusterSettings(TWO_PHASE_ENABLED_SETTING_KEY, enabled);
        updateClusterSettings(TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY, windowSizeExpansion);
        updateClusterSettings(TWO_PHASE_PRUNE_RATIO_SETTING_KEY, ratio);
        updateClusterSettings(TWO_PHASE_MAX_WINDOW_SIZE_SETTING_KEY, maxWindowSize);
    }

    @SneakyThrows
    private NeuralSparseTwoPhaseParameters getDefaultTwoPhaseParameter(boolean enabled, float windowSizeExpansion, float ratio) {
        return new NeuralSparseTwoPhaseParameters().enabled(enabled).window_size_expansion(windowSizeExpansion).pruning_ratio(ratio);
    }

    /**
     * Tests basic query with boost:
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_text": "Hello world a b",
     *                 "model_id": "dcsdcasd",
     *                 "boost": 2
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryText() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    /**
     * Tests basic query with boost:
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_tokens": {
     *                     "hello": float,
     *                     "world": float,
     *                     "a": float,
     *                     "b": float,
     *                     "c": float
     *                 },
     *                 "boost": 2
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryTokens() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            Map<String, Float> queryTokens = createRandomTokenWeightMap(TEST_TOKENS);
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

    /**
     * Tests basic query with boost:
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_tokens": {
     *                     "hello": float,
     *                     "world": float,
     *                     "a": float,
     *                     "b": float,
     *                     "c": float
     *                 },
     *                 "boost": 2
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryTokens_whenTwoPhaseEnabled() {
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            Map<String, Float> queryTokens = createRandomTokenWeightMap(TEST_TOKENS);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(() -> queryTokens)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(
                    new NeuralSparseTwoPhaseParameters().pruning_ratio(0.8f).window_size_expansion(5.0f).enabled(true)
                );
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, sparseEncodingQueryBuilder.queryTokensSupplier().get());
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, null);
        }
    }

    /**
     * Tests rescore query:
     * {
     *     "query" : {
     *       "match_all": {}
     *     },
     *     "rescore": {
     *         "query": {
     *              "rescore_query": {
     *                  "neural_sparse": {
     *                      "text_sparse": {
     *      *                 "query_text": "Hello world a b",
     *      *                 "model_id": "dcsdcasd"
     *      *             }
     *                  }
     *              }
     *          }
     *    }
     * }
     */
    @SneakyThrows
    public void testRescoreQuery() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    /**
     * Tests bool should query with query text:
     * {
     *     "query": {
     *         "bool" : {
     *             "should": [
     *                "neural_sparse": {
     *                  "field1": {
     *                      "query_text": "Hello world a b",
     *                      "model_id": "dcsdcasd"
     *                    }
     *                 },
     *                "neural_sparse": {
     *                  "field2": {
     *                      "query_text": "Hello world a b",
     *                      "model_id": "dcsdcasd"
     *                    }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBooleanQuery_withMultipleSparseEncodingQueries() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

            NeuralSparseQueryBuilder sparseEncodingQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_2)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);

            boolQueryBuilder.should(sparseEncodingQueryBuilder1).should(sparseEncodingQueryBuilder2);

            Map<String, Object> searchResponseAsMap = search(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, null, modelId, null);
        }
    }

    /**
     * Tests bool should query with query text when two phase enabled:
     * {
     *     "query": {
     *         "bool" : {
     *             "should": [
     *                "neural_sparse": {
     *                  "field1": {
     *                      "query_text": "Hello world a b",
     *                      "model_id": "dcsdcasd"
     *                    }
     *                 },
     *                "neural_sparse": {
     *                  "field2": {
     *                      "query_text": "Hello world a b",
     *                      "model_id": "dcsdcasd"
     *                    }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBooleanQuery_withMultipleSparseEncodingQueries_whenTwoPhaseEnabled() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 2.0f, 0.4f));
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_2)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 2.0f, 0.4f));

            boolQueryBuilder.should(sparseEncodingQueryBuilder1).should(sparseEncodingQueryBuilder2);

            Map<String, Object> searchResponseAsMap = search(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, null, modelId, null);
        }
    }

    /**
     * Tests bool should query with query text:
     * {
     *     "query": {
     *         "bool" : {
     *             "should": [
     *                "neural_sparse": {
     *                  "field1": {
     *                      "query_text": "Hello world a b",
     *                      "model_id": "dcsdcasd"
     *                    }
     *                 },
     *                "match": {
     *                  "field2": "Hello world a b",
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBooleanQuery_withSparseEncodingAndBM25Queries() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);
            MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);
            boolQueryBuilder.should(sparseEncodingQueryBuilder).should(matchQueryBuilder);

            Map<String, Object> searchResponseAsMap = search(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float minExpectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertTrue(minExpectedScore < objectToFloat(firstInnerHit.get("_score")));
        } finally {
            wipeOfTestResources(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    public void testBasicQueryUsingQueryText_whenQueryWrongFieldType_thenFail() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_TEXT_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);

            expectThrows(
                ResponseException.class,
                () -> search(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, sparseEncodingQueryBuilder, 1)
            );
        } finally {
            wipeOfTestResources(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, null, modelId, null);
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
     *                         "query_text": "TEST_QUERY_TEXT",
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0,
     *                         "neural_sparse_two_phase": {
     *                             "enable": true,
     *                             "window_size_expansion": 2.0,
     *                             "pruning_ratio": 0.4
     *                         }
     *                     }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryText_whenTwoPhaseEnabled_thenGetExpectedScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 2.0f, 0.4f));
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    public void testBasicQueryUsingQueryText_whenTwoPhaseEnabledAndDisabled_thenGetSameScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float scoreWithoutTwoPhase = objectToFloat(firstInnerHit.get("_score"));

            sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(
                    new NeuralSparseTwoPhaseParameters().enabled(true).pruning_ratio(0.3f).window_size_expansion(6.0f)
                );
            searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float scoreWithTwoPhase = objectToFloat(firstInnerHit.get("_score"));
            assertEquals(scoreWithTwoPhase, scoreWithoutTwoPhase, DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    public void testUpdateTwoPhaseSettings_whenTwoPhasedSettingsOverEdge_thenFail() {
        expectThrows(ResponseException.class, () -> updateTwoPhaseClusterSettings(true, 50000f, 1.4f, 10000));
        expectThrows(ResponseException.class, () -> updateTwoPhaseClusterSettings(true, 50000f, -1f, 10000));
        expectThrows(ResponseException.class, () -> updateTwoPhaseClusterSettings(true, -10f, 1.4f, 10000));
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
     *                             "query_text": "Hello world a b",
     *                             "model_id": "dcsdcasd",
     *                             "boost": 2.0,
     *                             "neural_sparse_two_phase": {
     *                                 "enable": true,
     *                                 "window_size_expansion": 4.0,
     *                                 "pruning_ratio": 0.5
     *                             }
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
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 4.0f, 0.5f));
            QueryBuilder queryBuilder = new MatchAllQueryBuilder();
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, queryBuilder, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
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
     *                         "boost": 2.0,
     *                         "neural_sparse_two_phase": {
     *                             "enable": true,
     *                             "window_size_expansion": 4.0,
     *                             "pruning_ratio": 0.2
     *                         }
     *                     }
     *                 },
     *                 {
     *                     "neural_sparse": {
     *                         "field": "test-sparse-encoding-1",
     *                         "query_text": "Hello world a b",
     *                         "model_id": "dcsdcasd",
     *                         "boost": 2.0,
     *                         "neural_sparse_two_phase": {
     *                             "enable": true,
     *                             "window_size_expansion": 4.0,
     *                             "pruning_ratio": 0.2
     *                         }
     *                     }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhase_thenGetExpectedScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 4.0f, 0.2f));
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 4 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhaseAndFilter_thenGetExpectedScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.8f));
            boolQueryBuilder.should(sparseEncodingQueryBuilder);
            boolQueryBuilder.filter(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, boolQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    public void testMultiNeuralSparseQuery_whenTwoPhaseAndMultiBoolean_thenGetExpectedScore() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(1.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
            boolQueryBuilder.should(sparseEncodingQueryBuilder1);
            boolQueryBuilder.should(sparseEncodingQueryBuilder1);
            BoolQueryBuilder subBoolQueryBuilder = new BoolQueryBuilder();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder3 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(3.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
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
            Map<String, Float> queryTokens = new HashMap<>();
            queryTokens.put("hello", 10.0f);
            queryTokens.put("world", 10.0f);
            queryTokens.put("a", 10.0f);
            queryTokens.put("b", 10.0f);
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryTokensSupplier(() -> queryTokens)
                .boost(2.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, sparseEncodingQueryBuilder.queryTokensSupplier().get());
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, null, null);
        }
    }

    /**
     * Tests constantScoreQuery with query text:
     * {
     *     "query": {
     *         "constant_score" : {
     *             "filter": [
     *                "neural_sparse": {
     *                  "field1": {
     *                      "query_text": "Hello world a b",
     *                      "model_id": "dcsdcasd"
     *                    }
     *                 }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testNeuralSParseQuery_whenTwoPhaseAndNestedInConstantScoreQuery_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(1.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
            ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(sparseEncodingQueryBuilder);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, constantScoreQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            assertEquals(1.0f, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    /**
     * Tests disjunctionMaxQuery with query text:
     * {
     *     "query": {
     *         "dis_max" : {
     *             "queries": [
     *                  {
     *                      "neural_sparse": {
     *                          "field1": {
     *                              "query_text": "Hello world a b",
     *                              "model_id": "dcsdcasd"
     *                          }
     *                      }
     *                  },
     *                  {
     *                      "match_all":{}
     *                  }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testNeuralSParseQuery_whenTwoPhaseAndNestedInDisjunctionMaxQuery_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(5.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
            DisMaxQueryBuilder disMaxQueryBuilder = new DisMaxQueryBuilder();
            disMaxQueryBuilder.add(sparseEncodingQueryBuilder);
            disMaxQueryBuilder.add(new MatchAllQueryBuilder());
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, disMaxQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 5f * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }

    /**
     * Tests functionScoreQuery with query text:
     * {
     *     "query": {
     *         "function_score" : {
     *             "query":{
     *                  "neural_sparse": {
     *                      "field1": {
     *                          "query_text": "Hello world a b",
     *                          "model_id": "dcsdcasd"
     *                      }
     *                  }
     *              }
     *          }
     *     }
     * }
     */
    @SneakyThrows
    public void testNeuralSParseQuery_whenTwoPhaseAndNestedInFunctionScoreQuery_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareSparseEncodingModel();
            NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(5.0f)
                .neuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameter(true, 5.0f, 0.6f));
            FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(sparseEncodingQueryBuilder);
            functionScoreQueryBuilder.boost(2.0f);
            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, functionScoreQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = 10f * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
    }
}
