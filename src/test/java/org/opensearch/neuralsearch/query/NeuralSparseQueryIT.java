/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.TestUtils.DELTA;
import static org.opensearch.neuralsearch.TestUtils.TEST_BASIC_SPARSE_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_NEURAL_SPARSE_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.TEST_NEURAL_SPARSE_FIELD_NAME_2;
import static org.opensearch.neuralsearch.TestUtils.TEST_QUERY_TEXT9;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_TEXT_FIELD_NAME_3;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;
import static org.opensearch.neuralsearch.TestUtils.testRankFeaturesDoc;

import java.util.Map;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseSparseEncodingIT;

public class NeuralSparseQueryIT extends BaseSparseEncodingIT {

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
        initializeSparseIndexIfNotExist(TEST_BASIC_SPARSE_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_SPARSE_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT9);
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
        initializeSparseIndexIfNotExist(TEST_BASIC_SPARSE_INDEX_NAME);
        String modelId = getDeployedModelId();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId)
            .boost(2.0f);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_SPARSE_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT9);
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
        initializeSparseIndexIfNotExist(TEST_BASIC_SPARSE_INDEX_NAME);
        String modelId = getDeployedModelId();
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_SPARSE_INDEX_NAME, matchAllQueryBuilder, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT9);
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
        initializeSparseIndexIfNotExist(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralSparseQueryBuilder sparseEncodingQueryBuilder1 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId);
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder2 = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_2)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId);

        boolQueryBuilder.should(sparseEncodingQueryBuilder1).should(sparseEncodingQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_NEURAL_SPARSE_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT9);
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
        initializeSparseIndexIfNotExist(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_NEURAL_SPARSE_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME_3, TEST_QUERY_TEXT9);
        boolQueryBuilder.should(sparseEncodingQueryBuilder).should(matchQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float minExpectedScore = computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT9);
        assertTrue(minExpectedScore < objectToFloat(firstInnerHit.get("_score")));
    }

    @SneakyThrows
    public void testBasicQueryUsingQueryText_whenQueryWrongFieldType_thenFail() {
        initializeSparseIndexIfNotExist(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME);
        String modelId = getDeployedModelId();

        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_TEXT_FIELD_NAME_3)
            .queryText(TEST_QUERY_TEXT9)
            .modelId(modelId);

        expectThrows(ResponseException.class, () -> search(TEST_TEXT_AND_NEURAL_SPARSE_FIELD_INDEX_NAME, sparseEncodingQueryBuilder, 1));
    }
}
