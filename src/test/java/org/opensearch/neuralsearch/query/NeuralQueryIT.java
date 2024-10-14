/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.util.TestUtils.objectToFloat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

public class NeuralQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-neural-basic-index";
    private static final String TEST_MULTI_VECTOR_FIELD_INDEX_NAME = "test-neural-multi-vector-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-neural-nested-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-neural-multi-doc-index";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_NESTED = "nested.knn.field";
    private final float[] testVector = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
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
     * and query with image query part
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
     * and query with radial search max distance and min score:
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "model_id": "dcsdcasd",
     *                 "max_distance": 100.0f,
     *             }
     *         }
     *     }
     * }
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "model_id": "dcsdcasd",
     *                 "min_score": 0.01f,
     *             }
     *         }
     *     }
     * }
     *
     */
    @SneakyThrows
    public void testQueryWithBoostAndImageQueryAndRadialQuery() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareModel();
            NeuralQueryBuilder neuralQueryBuilderTextQuery = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                null,
                null,
                null
            );

            final float boost = 2.0f;
            neuralQueryBuilderTextQuery.boost(boost);
            Map<String, Object> searchResponseAsMapTextQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilderTextQuery, 1);
            Map<String, Object> firstInnerHitTextQuery = getFirstInnerHit(searchResponseAsMapTextQuery);

            assertEquals("1", firstInnerHitTextQuery.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHitTextQuery.get("_score")), DELTA_FOR_SCORE_ASSERTION);

            NeuralQueryBuilder neuralQueryBuilderMultimodalQuery = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                TEST_IMAGE_TEXT,
                modelId,
                1,
                null,
                null,
                null,
                null,
                Map.of("ef_search", 10),
                RescoreContext.getDefault()
            );
            Map<String, Object> searchResponseAsMapMultimodalQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilderMultimodalQuery, 1);
            Map<String, Object> firstInnerHitMultimodalQuery = getFirstInnerHit(searchResponseAsMapMultimodalQuery);

            assertEquals("1", firstInnerHitMultimodalQuery.get("_id"));
            float expectedScoreMultimodalQuery = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(
                expectedScoreMultimodalQuery,
                objectToFloat(firstInnerHitMultimodalQuery.get("_score")),
                DELTA_FOR_SCORE_ASSERTION
            );

            // To save test resources, IT tests for radial search are added below.
            // Context: https://github.com/opensearch-project/neural-search/pull/697#discussion_r1571549776

            // Test radial search max distance query
            NeuralQueryBuilder neuralQueryWithMaxDistanceBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                null,
                100.0f,
                null,
                null,
                null,
                null,
                null
            );

            Map<String, Object> searchResponseAsMapWithMaxDistanceQuery = search(
                TEST_BASIC_INDEX_NAME,
                neuralQueryWithMaxDistanceBuilder,
                1
            );
            Map<String, Object> firstInnerHitWithMaxDistanceQuery = getFirstInnerHit(searchResponseAsMapWithMaxDistanceQuery);

            assertEquals("1", firstInnerHitWithMaxDistanceQuery.get("_id"));
            float expectedScoreWithMaxDistanceQuery = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(
                expectedScoreWithMaxDistanceQuery,
                objectToFloat(firstInnerHitWithMaxDistanceQuery.get("_score")),
                DELTA_FOR_SCORE_ASSERTION
            );

            // Test radial search min score query
            NeuralQueryBuilder neuralQueryWithMinScoreBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                null,
                null,
                0.01f,
                null,
                null,
                null,
                null
            );

            Map<String, Object> searchResponseAsMapWithMinScoreQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryWithMinScoreBuilder, 1);
            Map<String, Object> firstInnerHitWithMinScoreQuery = getFirstInnerHit(searchResponseAsMapWithMinScoreQuery);

            assertEquals("1", firstInnerHitWithMinScoreQuery.get("_id"));
            float expectedScoreWithMinScoreQuery = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(
                expectedScoreWithMinScoreQuery,
                objectToFloat(firstInnerHitWithMinScoreQuery.get("_score")),
                DELTA_FOR_SCORE_ASSERTION
            );
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
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
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
            modelId = prepareModel();
            MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
            NeuralQueryBuilder rescoreNeuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, rescoreNeuralQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_BASIC_INDEX_NAME, null, modelId, null);
        }
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
     * and bool should with BM25 and neural query:
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
    public void testBooleanQuery_withMultipleNeuralQueries() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_VECTOR_FIELD_INDEX_NAME);
            modelId = prepareModel();
            // verify two neural queries wrapped into bool
            BoolQueryBuilder boolQueryBuilderTwoNeuralQueries = new BoolQueryBuilder();
            NeuralQueryBuilder neuralQueryBuilder1 = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                null,
                null,
                null
            );
            NeuralQueryBuilder neuralQueryBuilder2 = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_2,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                null,
                null,
                null
            );

            boolQueryBuilderTwoNeuralQueries.should(neuralQueryBuilder1).should(neuralQueryBuilder2);

            Map<String, Object> searchResponseAsMapTwoNeuralQueries = search(
                TEST_MULTI_VECTOR_FIELD_INDEX_NAME,
                boolQueryBuilderTwoNeuralQueries,
                1
            );
            Map<String, Object> firstInnerHitTwoNeuralQueries = getFirstInnerHit(searchResponseAsMapTwoNeuralQueries);

            assertEquals("1", firstInnerHitTwoNeuralQueries.get("_id"));
            float expectedScore = 2 * computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHitTwoNeuralQueries.get("_score")), DELTA_FOR_SCORE_ASSERTION);

            // verify bool with one neural and one bm25 query
            BoolQueryBuilder boolQueryBuilderMixOfNeuralAndBM25 = new BoolQueryBuilder();
            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                null,
                null,
                null
            );

            MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);

            boolQueryBuilderMixOfNeuralAndBM25.should(neuralQueryBuilder).should(matchQueryBuilder);

            Map<String, Object> searchResponseAsMapMixOfNeuralAndBM25 = search(
                TEST_MULTI_VECTOR_FIELD_INDEX_NAME,
                boolQueryBuilderMixOfNeuralAndBM25,
                1
            );
            Map<String, Object> firstInnerHitMixOfNeuralAndBM25 = getFirstInnerHit(searchResponseAsMapMixOfNeuralAndBM25);

            assertEquals("1", firstInnerHitMixOfNeuralAndBM25.get("_id"));
            float minExpectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertTrue(minExpectedScore < objectToFloat(firstInnerHitMixOfNeuralAndBM25.get("_score")));
        } finally {
            wipeOfTestResources(TEST_MULTI_VECTOR_FIELD_INDEX_NAME, null, modelId, null);
        }
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
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_NESTED_INDEX_NAME);
            modelId = prepareModel();
            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_NESTED,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Map<String, Object> searchResponseAsMap = search(TEST_NESTED_INDEX_NAME, neuralQueryBuilder, 1);
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

            assertEquals("1", firstInnerHit.get("_id"));
            float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_NESTED_INDEX_NAME, null, modelId, null);
        }
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
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
            modelId = prepareModel();
            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_QUERY_TEXT,
                "",
                modelId,
                1,
                null,
                null,
                null,
                new MatchQueryBuilder("_id", "3"),
                null,
                null
            );
            Map<String, Object> searchResponseAsMap = search(TEST_MULTI_DOC_INDEX_NAME, neuralQueryBuilder, 3);
            assertEquals(1, getHitCount(searchResponseAsMap));
            Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
            assertEquals("3", firstInnerHit.get("_id"));
            float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
            assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_NAME, null, modelId, null);
        }
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
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
                List.of(Floats.asList(testVector).toArray(), Floats.asList(testVector).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_QUERY_TEXT)
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

        if (TEST_MULTI_DOC_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_NAME,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_NAME,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(3, getDocCount(TEST_MULTI_DOC_INDEX_NAME));
        }
    }
}
