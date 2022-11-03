/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.TestUtils.objectToFloat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;

public class NeuralQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-neural-basic-index";
    private static final String TEST_MULTI_VECTOR_FIELD_INDEX_NAME = "test-neural-multi-vector-field-index";
    private static final String TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME = "test-neural-text-and-vector-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-neural-nested-index";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_NESTED = "nested.knn.field";

    private static final int TEST_DIMENSION = 768;
    private static final SpaceType TEST_SPACE_TYPE = SpaceType.L2;
    private final float[] testVector = createRandomVector(TEST_DIMENSION);
    private final AtomicReference<String> modelId = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelId.compareAndSet(null, prepareModel());
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
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
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
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        final float boost = 2.0f;
        neuralQueryBuilder.boost(boost);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
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
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        NeuralQueryBuilder rescoreNeuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, rescoreNeuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
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
        initializeIndexIfNotExist(TEST_MULTI_VECTOR_FIELD_INDEX_NAME);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralQueryBuilder neuralQueryBuilder1 = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );
        NeuralQueryBuilder neuralQueryBuilder2 = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_2,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        boolQueryBuilder.should(neuralQueryBuilder1).should(neuralQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_VECTOR_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
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
        initializeIndexIfNotExist(TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);

        boolQueryBuilder.should(neuralQueryBuilder).should(matchQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float minExpectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
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
        initializeIndexIfNotExist(TEST_NESTED_INDEX_NAME);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_NESTED,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        Map<String, Object> searchResponseAsMap = search(TEST_NESTED_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    private void initializeIndexIfNotExist(String indexName) throws IOException {
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_BASIC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(TEST_BASIC_INDEX_NAME));
        }

        if (TEST_MULTI_VECTOR_FIELD_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_VECTOR_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_VECTOR_FIELD_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                )
            );
            addKnnDoc(
                TEST_MULTI_VECTOR_FIELD_INDEX_NAME,
                "1",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector).toArray(), Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(TEST_MULTI_VECTOR_FIELD_INDEX_NAME));
        }

        if (TEST_NESTED_INDEX_NAME.equals(indexName) && !indexExists(TEST_NESTED_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_NESTED_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_NESTED, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_NESTED_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_NESTED),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(TEST_NESTED_INDEX_NAME));
        }

        if (TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME.equals(indexName) && !indexExists(TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_QUERY_TEXT)
            );
            assertEquals(1, getDocCount(TEST_TEXT_AND_VECTOR_FIELD_INDEX_NAME));
        }
    }
}
