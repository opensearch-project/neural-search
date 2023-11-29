/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.TestUtils.TEST_BASIC_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_IMAGE_TEXT;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_2;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_NESTED;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_DOC_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_VECTOR_FIELD_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_NESTED_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT8;
import static org.opensearch.neuralsearch.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_FIELD_NAME_3;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;
import static org.opensearch.neuralsearch.TestUtils.testVector;

import java.util.Map;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

public class NeuralQueryIT extends BaseNeuralSearchIT {

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
        /* this is required to minimize chance of model not being deployed due to open memory CB,
         * this happens in case we leave model from previous test case. We use new model for every test, and old model
         * can be undeployed and deleted to free resources after each test case execution.
         */
        findDeployedModels().forEach(this::deleteModel);
    }

    /**
     * Tests basic query:
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "model_id": "dcsdcasd",
     *                 "k": 1
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQuery() {
        initializeBasicIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests basic query with boost parameter:
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "model_id": "dcsdcasd",
     *                 "k": 1,
     *                 "boost": 2.0
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBoostQuery() {
        initializeBasicIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );

        final float boost = 2.0f;
        neuralQueryBuilder.boost(boost);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
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
     *                  "neural": {
     *                      "text_knn": {
     *                          "query_text": "Hello world",
     *                          "model_id": "dcsdcasd",
     *                          "k": 1
     *                      }
     *                  }
     *              }
     *          }
     *    }
     */
    @SneakyThrows
    public void testRescoreQuery() {
        initializeBasicIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        NeuralQueryBuilder rescoreNeuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );

        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, rescoreNeuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests bool should query with vectors:
     * {
     *     "query": {
     *         "bool" : {
     *             "should": [
     *                 "neural": {
     *                     "field_1": {
     *                         "query_text": "Hello world",
     *                         "model_id": "dcsdcasd",
     *                         "k": 1
     *                     },
     *                  },
     *                  "neural": {
     *                     "field_2": {
     *                         "query_text": "Hello world",
     *                         "model_id": "dcsdcasd",
     *                         "k": 1
     *                     }
     *                  }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBooleanQuery_withMultipleNeuralQueries() {
        initializeBasicIndexIfNotExist(TEST_MULTI_VECTOR_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralQueryBuilder neuralQueryBuilder1 = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );
        NeuralQueryBuilder neuralQueryBuilder2 = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_2,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );

        boolQueryBuilder.should(neuralQueryBuilder1).should(neuralQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_VECTOR_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests bool should with BM25 and neural query:
     * {
     *     "query": {
     *         "bool" : {
     *             "should": [
     *                 "neural": {
     *                     "field_1": {
     *                         "query_text": "Hello world",
     *                         "model_id": "dcsdcasd",
     *                         "k": 1
     *                     },
     *                  },
     *                  "match": {
     *                     "field_2": {
     *                          "query": "Hello world"
     *                     }
     *                  }
     *             ]
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBooleanQuery_withNeuralAndBM25Queries() {
        initializeBasicIndexIfNotExist(TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME_3, TEST_QUERY_TEXT8);

        boolQueryBuilder.should(neuralQueryBuilder).should(matchQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float minExpectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
        assertTrue(minExpectedScore < objectToFloat(firstInnerHit.get("_score")));
    }

    /**
     * Tests nested query:
     * {
     *     "query": {
     *         "nested" : {
     *             "query": {
     *                 "neural": {
     *                     "field_1": {
     *                         "query_text": "Hello world",
     *                         "model_id": "dcsdcasd",
     *                         "k": 1
     *                     },
     *                  }
     *              }
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testNestedQuery() {
        initializeBasicIndexIfNotExist(TEST_NESTED_INDEX_NAME);
        String modelId = getDeployedModelId();

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_NESTED,
            TEST_QUERY_TEXT8,
            "",
            modelId,
            1,
            null,
            null
        );

        Map<String, Object> searchResponseAsMap = search(TEST_NESTED_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT8);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests filter query:
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "model_id": "dcsdcasd",
     *                 "k": 1,
     *                 "filter": {
     *                     "match": {
     *                         "_id": {
     *                             "query": "3"
     *                         }
     *                     }
     *                 }
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testFilterQuery() {
        initializeMultiDocIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            "",
            modelId,
            1,
            null,
            new MatchQueryBuilder("_id", "3")
        );
        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_DOC_INDEX_NAME, neuralQueryBuilder, 3);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("3", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests basic query for multimodal:
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "query_image": "base64_1234567890",
     *                 "model_id": "dcsdcasd",
     *                 "k": 1
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testMultimodalQuery() {
        initializeBasicIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            TEST_IMAGE_TEXT,
            modelId,
            1,
            null,
            null
        );
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }
}
