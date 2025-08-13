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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import joptsimple.internal.Strings;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.neuralsearch.stats.events.EventStatName;

public class NeuralQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-neural-basic-index";
    private static final String TEST_MULTI_VECTOR_FIELD_INDEX_NAME = "test-neural-multi-vector-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-neural-nested-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-neural-multi-doc-index";
    private static final String TEST_SEMANTIC_INDEX_SPARSE_NAME = "test-neural-sparse-semantic-index";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final String TEST_QUERY_TEXT_SPARSE = "Hello world a b";
    private static final String TEST_IMAGE_TEXT = "/9j/4AAQSkZJRgABAQAASABIAAD";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_SEMANTIC_TEXT_FIELD = "test_field";
    private static final String SEMANTIC_INFO_FIELD = "semantic_info";
    private static final String SEMANTIC_EMBEDDING_FIELD = "embedding";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_NESTED = "nested.knn.field";
    private final float[] testVector = createRandomVector(TEST_DIMENSION);
    private static final List<String> TEST_TOKENS = List.of("hello", "world", "a", "b", "c");
    private final Map<String, Float> testRankFeaturesDoc = TestUtils.createRandomTokenWeightMap(TEST_TOKENS);

    private static final Float DELTA = 1e-5f;

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
        // Enable stats for the test
        enableStats();
        String modelId = null;
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        modelId = prepareModel();
        NeuralQueryBuilder neuralQueryBuilderTextQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

        final float boost = 2.0f;
        neuralQueryBuilderTextQuery.boost(boost);
        Map<String, Object> searchResponseAsMapTextQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilderTextQuery, 1);
        Map<String, Object> firstInnerHitTextQuery = getFirstInnerHit(searchResponseAsMapTextQuery);

        assertEquals("1", firstInnerHitTextQuery.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHitTextQuery.get("_score")), DELTA_FOR_SCORE_ASSERTION);

        NeuralQueryBuilder neuralQueryBuilderMultimodalQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .queryImage(TEST_IMAGE_TEXT)
            .modelId(modelId)
            .k(1)
            .methodParameters(Map.of("ef_search", 10))
            .rescoreContext(RescoreContext.getDefault())
            .build();

        Map<String, Object> searchResponseAsMapMultimodalQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilderMultimodalQuery, 1);
        Map<String, Object> firstInnerHitMultimodalQuery = getFirstInnerHit(searchResponseAsMapMultimodalQuery);

        assertEquals("1", firstInnerHitMultimodalQuery.get("_id"));
        float expectedScoreMultimodalQuery = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScoreMultimodalQuery, objectToFloat(firstInnerHitMultimodalQuery.get("_score")), DELTA_FOR_SCORE_ASSERTION);

        // To save test resources, IT tests for radial search are added below.
        // Context: https://github.com/opensearch-project/neural-search/pull/697#discussion_r1571549776

        // Test radial search max distance query
        NeuralQueryBuilder neuralQueryWithMaxDistanceBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .maxDistance(100.0f)
            .build();

        Map<String, Object> searchResponseAsMapWithMaxDistanceQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryWithMaxDistanceBuilder, 1);
        Map<String, Object> firstInnerHitWithMaxDistanceQuery = getFirstInnerHit(searchResponseAsMapWithMaxDistanceQuery);

        assertEquals("1", firstInnerHitWithMaxDistanceQuery.get("_id"));
        float expectedScoreWithMaxDistanceQuery = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(
            expectedScoreWithMaxDistanceQuery,
            objectToFloat(firstInnerHitWithMaxDistanceQuery.get("_score")),
            DELTA_FOR_SCORE_ASSERTION
        );

        // Test radial search min score query
        NeuralQueryBuilder neuralQueryWithMinScoreBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .minScore(0.01f)
            .build();

        Map<String, Object> searchResponseAsMapWithMinScoreQuery = search(TEST_BASIC_INDEX_NAME, neuralQueryWithMinScoreBuilder, 1);
        Map<String, Object> firstInnerHitWithMinScoreQuery = getFirstInnerHit(searchResponseAsMapWithMinScoreQuery);

        assertEquals("1", firstInnerHitWithMinScoreQuery.get("_id"));
        float expectedScoreWithMinScoreQuery = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(
            expectedScoreWithMinScoreQuery,
            objectToFloat(firstInnerHitWithMinScoreQuery.get("_score")),
            DELTA_FOR_SCORE_ASSERTION
        );

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(
            "Stats should contain the expected number of neural_query_against_knn_requests",
            4,
            getNestedValue(allNodesStats, EventStatName.NEURAL_QUERY_AGAINST_KNN_REQUESTS)
        );
        assertEquals(
            "Stats should contain the expected number of neural_query_requests",
            4,
            getNestedValue(allNodesStats, EventStatName.NEURAL_QUERY_REQUESTS)
        );
        // Disable stats to not impact other tests
        disableStats();
    }

    /**
     * Test basic query with Match Query Builder
     * {
     *     "query": {
     *         "neural": {
     *             "text_knn": {
     *                 "query_text": "Hello world",
     *                 "model_id": "dcsdcasd",
     *                 "k": 2,
     *                 "boost": 2.0
     *             }
     *             "filter": {
     *                 "match": {
     *                     "_id": {
     *                         "query": "3"
     *                     }
     *                 }
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testQueryWithBoostAndFilterApplied() {
        String modelId = null;
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        modelId = prepareModel();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(3)
            .build();

        // Test with a Filter Applied
        neuralQueryBuilder.queryfilter(new MatchQueryBuilder("_id", "3"));
        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_DOC_INDEX_NAME, neuralQueryBuilder, 3);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("3", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
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
        initializeIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        modelId = prepareModel();
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        NeuralQueryBuilder rescoreNeuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, rescoreNeuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
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
        initializeIndexIfNotExist(TEST_MULTI_VECTOR_FIELD_INDEX_NAME);
        modelId = prepareModel();
        // verify two neural queries wrapped into bool
        BoolQueryBuilder boolQueryBuilderTwoNeuralQueries = new BoolQueryBuilder();
        NeuralQueryBuilder neuralQueryBuilder1 = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

        NeuralQueryBuilder neuralQueryBuilder2 = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_2)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

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
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

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
        initializeIndexIfNotExist(TEST_NESTED_INDEX_NAME);
        modelId = prepareModel();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_NESTED)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

        Map<String, Object> searchResponseAsMap = search(TEST_NESTED_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals("1", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
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
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        modelId = prepareModel();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .filter(new MatchQueryBuilder("_id", "3"))
            .build();

        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_DOC_INDEX_NAME, neuralQueryBuilder, 3);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("3", firstInnerHit.get("_id"));
        float expectedScore = computeExpectedScore(modelId, testVector, TEST_SPACE_TYPE, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA_FOR_SCORE_ASSERTION);
    }

    /**
     * Tests basic query with boost in semantic index:
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
    public void testNeuralQuery_InSemanticField_WithSparseModel() {
        String modelId = prepareSparseEncodingModel();
        initializeIndexIfNotExist(TEST_SEMANTIC_INDEX_SPARSE_NAME, modelId, null);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_SEMANTIC_TEXT_FIELD)
            .queryText(TEST_QUERY_TEXT)
            .boost(2.0f)
            .build();

        Map<String, Object> searchResponseAsMap = search(TEST_SEMANTIC_INDEX_SPARSE_NAME, neuralQueryBuilder, 3);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("4", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(modelId, testRankFeaturesDoc, TEST_QUERY_TEXT);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
    }

    /**
     * Tests basic query with boost and search analyzer in query time
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_text": "Hello world a b",
     *                 "semantic_field_search_analyzer": "standard",
     *                 "boost": 2
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testNeuralQuery_InSemanticField_WithSparseModelAndSearchAnalyzer() {
        String modelId = prepareSparseEncodingModel();
        initializeIndexIfNotExist(TEST_SEMANTIC_INDEX_SPARSE_NAME, modelId, null);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_SEMANTIC_TEXT_FIELD)
            .queryText(TEST_QUERY_TEXT_SPARSE)
            .boost(2.0f)
            .searchAnalyzer("standard")
            .build();

        Map<String, Object> searchResponseAsMap = search(TEST_SEMANTIC_INDEX_SPARSE_NAME, neuralQueryBuilder, 3);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("4", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, Map.of("hello", 1f, "world", 1f, "a", 1f, "b", 1f));
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
    }

    /**
     * Tests basic query with boost and search analyzer in semantic index creation
     * {
     *     "query": {
     *         "neural_sparse": {
     *             "text_sparse": {
     *                 "query_text": "Hello world a b",
     *                 "boost": 2
     *             }
     *         }
     *     }
     * }
     */
    @SneakyThrows
    public void testNeuralQuery_InSemanticField_WithSparseModelAndSearchAnalyzerAtIndexCreation() {
        String modelId = prepareSparseEncodingModel();
        initializeIndexIfNotExist(TEST_SEMANTIC_INDEX_SPARSE_NAME, modelId, "standard");
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(TEST_SEMANTIC_TEXT_FIELD)
            .queryText(TEST_QUERY_TEXT_SPARSE)
            .boost(2.0f)
            .build();

        Map<String, Object> searchResponseAsMap = search(TEST_SEMANTIC_INDEX_SPARSE_NAME, neuralQueryBuilder, 3);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);
        assertEquals("4", firstInnerHit.get("_id"));
        float expectedScore = 2 * computeExpectedScore(testRankFeaturesDoc, Map.of("hello", 1f, "world", 1f, "a", 1f, "b", 1f));
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), DELTA);
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        initializeIndexIfNotExist(indexName, Strings.EMPTY, null);
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName, String modelId, String semanticFieldSearchAnalyzer) {
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

        if (TEST_SEMANTIC_INDEX_SPARSE_NAME.equals(indexName) && !indexExists(TEST_SEMANTIC_INDEX_SPARSE_NAME)) {
            prepareSemanticIndex(
                TEST_SEMANTIC_INDEX_SPARSE_NAME,
                Collections.singletonList(new SemanticFieldConfig(TEST_SEMANTIC_TEXT_FIELD)),
                modelId,
                semanticFieldSearchAnalyzer
            );
            addSemanticDoc(
                indexName,
                "4",
                String.format(LOCALE, "%s_%s", TEST_SEMANTIC_TEXT_FIELD, SEMANTIC_INFO_FIELD),
                List.of(SEMANTIC_EMBEDDING_FIELD),
                List.of(testRankFeaturesDoc)
            );
            assertEquals(1, getDocCount(TEST_SEMANTIC_INDEX_SPARSE_NAME));
        }
    }
}
