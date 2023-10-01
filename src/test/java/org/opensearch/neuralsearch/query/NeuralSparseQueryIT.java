/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.TestUtils.objectToFloat;

import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.TestUtils;
import org.opensearch.neuralsearch.common.BaseSparseEncodingIT;

public class NeuralSparseQueryIT extends BaseSparseEncodingIT {
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

    private static final Float DELTA = 1e-5f;
    private final Map<String, Float> testRankFeaturesDoc = TestUtils.createRandomTokenWeightMap(TEST_TOKENS);

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
        findDeployedModels().forEach(this::deleteModel);
    }

    /**
     * Tests basic query:
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_text": "Hello world a b",
     *                 "model_id": "dcsdcasd"
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryText() {
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
    }

    /**
     * Tests basic query:
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_text": "Hello world a b",
     *                 "model_id": "dcsdcasd",
     *                 "max_token_score": float
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryWithMaxTokenScore() {
        float maxTokenScore = 0.00001f;
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .maxTokenScore(maxTokenScore);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        Map<String, Float> queryTokens = runSparseModelInference(modelId, TEST_QUERY_TEXT);
        float expectedScore = 0f;
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            if (testRankFeaturesDoc.containsKey(entry.getKey())) {
                expectedScore += entry.getValue() * Math.min(
                    getFeatureFieldCompressedNumber(testRankFeaturesDoc.get(entry.getKey())),
                    maxTokenScore
                );
            }
        }
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
    }

    /**
     * Tests basic query:
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
    public void testBoostQuery() {
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .boost(2.0f);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
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
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
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
        initializeIndexIfNotExist(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();
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
    public void testBooleanQuery_withSparseEncodingAndBM25Queries() {
        initializeIndexIfNotExist(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();
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
    }

    @SneakyThrows
    public void testBasicQueryUsingQueryText_whenQueryWrongFieldType_thenFail() {
        initializeIndexIfNotExist(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();

        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_TEXT_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId);

        expectThrows(ResponseException.class, () -> search(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, sparseEncodingQueryBuilder, 1));
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
}
