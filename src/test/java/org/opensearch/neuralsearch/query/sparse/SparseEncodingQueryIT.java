/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query.sparse;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.TestUtils;
import org.opensearch.neuralsearch.common.BaseSparseEncodingIT;

import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.TestUtils.objectToFloat;

public class SparseEncodingQueryIT extends BaseSparseEncodingIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-sparse-basic-index";
    private static final String TEST_MULTI_VECTOR_FIELD_INDEX_NAME = "test-sparse-multi-field-index";
    private static final String TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME = "test-sparse-text-and-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-sparse-nested-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-sparse-multi-doc-index";
    private static final String TEST_QUERY_TEXT = "Hello world a b";
    private static final String TEST_SPARSE_ENCODING_FIELD_NAME_1 = "test-sparse-encoding-1";
    private static final String TEST_SPARSE_ENCODING_FIELD_NAME_2 = "test-sparse-encoding-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field";
    private static final String TEST_SPARSE_ENCODING_FIELD_NAME_NESTED = "nested.sparse_encoding.field";

    private static final List<String> TEST_TOKENS = List.of("hello", "world", "a", "b", "c");
    private final Map<String, Float> testTokenWeightMap = TestUtils.createRandomTokenWeightMap(TEST_TOKENS);

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
     *         "sparse_encoding": {
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
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(TEST_SPARSE_ENCODING_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testTokenWeightMap, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests basic query:
     * {
     *     "query": {
     *         "sparse_encoding": {
     *             "text_sparse": {
     *                 "query_tokens": {
     *                     "hello": float,
     *                     "a": float,
     *                     "c": float
     *                 }
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testBasicQueryUsingQueryTokens() {
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        Map<String, Float> queryTokens = TestUtils.createRandomTokenWeightMap(List.of("hello","a","b"));
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(TEST_SPARSE_ENCODING_FIELD_NAME_1)
                .queryTokens(queryTokens);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(testTokenWeightMap, queryTokens);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    /**
     * Tests basic query:
     * {
     *     "query": {
     *         "sparse_encoding": {
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
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(TEST_SPARSE_ENCODING_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId)
                .boost(2.0f);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, sparseEncodingQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testTokenWeightMap, TEST_QUERY_TEXT);
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
     *                  "sparse_encoding": {
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
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder()
                .fieldName(TEST_SPARSE_ENCODING_FIELD_NAME_1)
                .queryText(TEST_QUERY_TEXT)
                .modelId(modelId);
        Map<String, Object> searchResponseAsMap = search(
                TEST_BASIC_INDEX_NAME,
                matchAllQueryBuilder,
                sparseEncodingQueryBuilder,
                1
        );
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testTokenWeightMap, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    @SneakyThrows
    protected void initializeIndexIfNotExist(String indexName) {
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareSparseEncodingIndex(
                    TEST_BASIC_INDEX_NAME,
                    List.of(TEST_SPARSE_ENCODING_FIELD_NAME_1)
            );
            addSparseEncodingDoc(
                    TEST_BASIC_INDEX_NAME,
                    "1",
                    List.of(TEST_SPARSE_ENCODING_FIELD_NAME_1),
                    List.of(testTokenWeightMap)
            );
            assertEquals(1, getDocCount(TEST_BASIC_INDEX_NAME));
        }
    }
}
