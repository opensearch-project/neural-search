/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.opensearch.client.ResponseException;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

public class HybridQueryJVectorIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-hybrid-basic-index";
    private static final String TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME = "test-hybrid-vector-doc-field-index";
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME = "test-hybrid-multi-doc-nested-fields-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-hybrid-multi-doc-index";
    private static final String TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD = "test-hybrid-multi-doc-single-shard-index";
    private static final String TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD =
        "test-hybrid-multi-doc-nested-type-single-shard-index";
    private static final String TEST_INDEX_WITH_KEYWORDS_ONE_SHARD = "test-hybrid-keywords-single-shard-index";
    private static final String TEST_INDEX_DOC_QTY_ONE_SHARD = "test-hybrid-doc-qty-single-shard-index";
    private static final String TEST_INDEX_DOC_QTY_MULTIPLE_SHARDS = "test-hybrid-doc-qty-multiple-shards-index";
    private static final String TEST_INDEX_WITH_KEYWORDS_THREE_SHARDS = "test-hybrid-keywords-three-shards-index";
    private static final String TEST_QUERY_TEXT = "greetings";
    private static final String TEST_QUERY_TEXT2 = "salute";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    private static final String NESTED_FIELD_1 = "firstname";
    private static final String NESTED_FIELD_2 = "lastname";
    private static final String NESTED_FIELD_1_VALUE = "john";
    private static final String NESTED_FIELD_2_VALUE = "black";
    private static final String KEYWORD_FIELD_1 = "doc_keyword";
    private static final String KEYWORD_FIELD_1_VALUE = "workable";
    private static final String KEYWORD_FIELD_2_VALUE = "angry";
    private static final String KEYWORD_FIELD_3_VALUE = "likeable";
    private static final String KEYWORD_FIELD_4_VALUE = "entire";
    private static final String INTEGER_FIELD_PRICE = "doc_price";
    private static final int INTEGER_FIELD_PRICE_1_VALUE = 130;
    private static final int INTEGER_FIELD_PRICE_2_VALUE = 100;
    private static final int INTEGER_FIELD_PRICE_3_VALUE = 200;
    private static final int INTEGER_FIELD_PRICE_4_VALUE = 25;
    private static final int INTEGER_FIELD_PRICE_5_VALUE = 30;
    private static final int INTEGER_FIELD_PRICE_6_VALUE = 350;
    protected static final int SINGLE_SHARD = 1;
    protected static final int MULTIPLE_SHARDS = 3;
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-pipeline";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * Tests complex query with multiple nested sub-queries:
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "bool": {
     *                          "should": [
     *                              {
     *                                  "term": {
     *                                      "text": "word1"
     *                                  }
     *                             },
     *                             {
     *                                  "term": {
     *                                      "text": "word2"
     *                                   }
     *                              }
     *                         ]
     *                      }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testComplexQuery_whenMultipleSubqueries_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
        TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(termQueryBuilder2).should(termQueryBuilder3);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(boolQueryBuilder);

        Map<String, Object> searchResponseAsMap1 = search(
            TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
            hybridQueryBuilderNeuralThenTerm,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap1);
        assertNotNull(total.get("value"));
        assertEquals(3, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testTotalHits_whenResultSizeIsLessThenDefaultSize_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
        TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(termQueryBuilder2).should(termQueryBuilder3);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(boolQueryBuilder);
        Map<String, Object> searchResponseAsMap = search(
            TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
            hybridQueryBuilderNeuralThenTerm,
            null,
            1,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(3, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testMaxScoreCalculation_whenMaxScoreIsTrackedAtCollectorLevel_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
        TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(termQueryBuilder2).should(termQueryBuilder3);

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(termQueryBuilder1);
        hybridQueryBuilderNeuralThenTerm.add(boolQueryBuilder);
        Map<String, Object> searchResponseAsMap = search(
            TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
            hybridQueryBuilderNeuralThenTerm,
            null,
            10,
            null
        );

        double maxScore = getMaxScore(searchResponseAsMap).get();
        List<Map<String, Object>> hits = getNestedHits(searchResponseAsMap);
        double maxScoreExpected = 0.0;
        for (Map<String, Object> hit : hits) {
            double score = (double) hit.get("_score");
            maxScoreExpected = Math.max(score, maxScoreExpected);
        }
        assertEquals(maxScoreExpected, maxScore, 0.0000001);
    }

    /**
     * Tests complex query with multiple nested sub-queries, where some sub-queries are same
     * {
     *     "query": {
     *         "hybrid": {
     *              "queries": [
     *                  {
     *                      "term": {
     *                         "text": "word1"
     *                       }
     *                  },
     *                  {
     *                      "term": {
     *                         "text": "word2"
     *                       }
     *                  },
     *                  {
     *                      "term": {
     *                          "text": "word3"
     *                      }
     *                  }
     *              ]
     *          }
     *      }
     * }
     */
    @SneakyThrows
    public void testComplexQuery_whenMultipleIdenticalSubQueries_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
        TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

        HybridQueryBuilder hybridQueryBuilderThreeTerms = new HybridQueryBuilder();
        hybridQueryBuilderThreeTerms.add(termQueryBuilder1);
        hybridQueryBuilderThreeTerms.add(termQueryBuilder2);
        hybridQueryBuilderThreeTerms.add(termQueryBuilder3);

        Map<String, Object> searchResponseAsMap1 = search(
            TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
            hybridQueryBuilderThreeTerms,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(2, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap1);
        assertNotNull(total.get("value"));
        assertEquals(2, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testNoMatchResults_whenOnlyTermSubQueryWithoutMatch_thenEmptyResult() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);
        TermQueryBuilder termQuery2Builder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT2);
        HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
        hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
        hybridQueryBuilderOnlyTerm.add(termQuery2Builder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME,
            hybridQueryBuilderOnlyTerm,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(0, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        assertEquals(0.0f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(0, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testNestedQuery_whenHybridQueryIsWrappedIntoOtherQuery_thenFail() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        MatchQueryBuilder matchQuery2Builder = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
        HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
        hybridQueryBuilderOnlyTerm.add(matchQueryBuilder);
        hybridQueryBuilderOnlyTerm.add(matchQuery2Builder);
        MatchQueryBuilder matchQuery3Builder = QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().should(hybridQueryBuilderOnlyTerm).should(matchQuery3Builder);

        ResponseException exceptionNoNestedTypes = expectThrows(
            ResponseException.class,
            () -> search(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD, boolQueryBuilder, null, 10, Map.of("search_pipeline", SEARCH_PIPELINE))
        );

        org.hamcrest.MatcherAssert.assertThat(
            exceptionNoNestedTypes.getMessage(),
            allOf(
                containsString("hybrid query must be a top level query and cannot be wrapped into other queries"),
                containsString("illegal_argument_exception")
            )
        );

        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);

        ResponseException exceptionQWithNestedTypes = expectThrows(
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
                boolQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            exceptionQWithNestedTypes.getMessage(),
            allOf(
                containsString("hybrid query must be a top level query and cannot be wrapped into other queries"),
                containsString("illegal_argument_exception")
            )
        );
    }

    @SneakyThrows
    public void testIndexWithNestedFields_whenHybridQuery_thenSuccess() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        TermQueryBuilder termQuery2Builder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT2);
        HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
        hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
        hybridQueryBuilderOnlyTerm.add(termQuery2Builder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
            hybridQueryBuilderOnlyTerm,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(1, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        assertEquals(0.5f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(1, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testIndexWithNestedFields_whenHybridQueryIncludesNested_thenSuccess() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT);
        NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery(
            TEST_NESTED_TYPE_FIELD_NAME_1,
            matchQuery(TEST_NESTED_TYPE_FIELD_NAME_1 + "." + NESTED_FIELD_1, NESTED_FIELD_1_VALUE),
            ScoreMode.Total
        );
        HybridQueryBuilder hybridQueryBuilderOnlyTerm = new HybridQueryBuilder();
        hybridQueryBuilderOnlyTerm.add(termQueryBuilder);
        hybridQueryBuilderOnlyTerm.add(nestedQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
            hybridQueryBuilderOnlyTerm,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(1, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        assertEquals(0.5f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(1, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testRequestCache_whenOneShardAndQueryReturnResults_thenSuccessful() {
        initializeIndexIfNotExist(TEST_INDEX_WITH_KEYWORDS_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_2_VALUE);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_PRICE).gte(10).lte(1000);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(rangeQueryBuilder);

        // first query with cache flag executed normally by reading documents from index
        Map<String, Object> firstSearchResponseAsMap = search(
            TEST_INDEX_WITH_KEYWORDS_ONE_SHARD,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE, "request_cache", Boolean.TRUE.toString())
        );

        int firstQueryHitCount = getHitCount(firstSearchResponseAsMap);
        assertTrue(firstQueryHitCount > 0);

        List<Map<String, Object>> hitsNestedList = getNestedHits(firstSearchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(firstSearchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(firstQueryHitCount, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // second query is served from the cache
        Map<String, Object> secondSearchResponseAsMap = search(
            TEST_INDEX_WITH_KEYWORDS_ONE_SHARD,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE, "request_cache", Boolean.TRUE.toString())
        );

        assertEquals(firstQueryHitCount, getHitCount(secondSearchResponseAsMap));

        List<Map<String, Object>> hitsNestedListSecondQuery = getNestedHits(secondSearchResponseAsMap);
        List<String> idsSecondQuery = new ArrayList<>();
        List<Double> scoresSecondQuery = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedListSecondQuery) {
            idsSecondQuery.add((String) oneHit.get("_id"));
            scoresSecondQuery.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(
            IntStream.range(0, scoresSecondQuery.size() - 1).noneMatch(idx -> scoresSecondQuery.get(idx) < scoresSecondQuery.get(idx + 1))
        );
        // verify that all ids are unique
        assertEquals(Set.copyOf(idsSecondQuery).size(), idsSecondQuery.size());

        Map<String, Object> totalSecondQuery = getTotalHits(secondSearchResponseAsMap);
        assertNotNull(totalSecondQuery.get("value"));
        assertEquals(firstQueryHitCount, totalSecondQuery.get("value"));
        assertNotNull(totalSecondQuery.get("relation"));
        assertEquals(RELATION_EQUAL_TO, totalSecondQuery.get("relation"));
    }

    @SneakyThrows
    public void testRequestCache_whenMultipleShardsQueryReturnResults_thenSuccessful() {
        initializeIndexIfNotExist(TEST_INDEX_WITH_KEYWORDS_THREE_SHARDS);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(KEYWORD_FIELD_1, KEYWORD_FIELD_2_VALUE);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_PRICE).gte(10).lte(1000);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(rangeQueryBuilder);

        // first query with cache flag executed normally by reading documents from index
        Map<String, Object> firstSearchResponseAsMap = search(
            TEST_INDEX_WITH_KEYWORDS_THREE_SHARDS,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE, "request_cache", Boolean.TRUE.toString())
        );

        int firstQueryHitCount = getHitCount(firstSearchResponseAsMap);
        assertTrue(firstQueryHitCount > 0);

        List<Map<String, Object>> hitsNestedList = getNestedHits(firstSearchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(firstSearchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(firstQueryHitCount, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // second query is served from the cache
        Map<String, Object> secondSearchResponseAsMap = search(
            TEST_INDEX_WITH_KEYWORDS_THREE_SHARDS,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE, "request_cache", Boolean.TRUE.toString())
        );

        assertEquals(firstQueryHitCount, getHitCount(secondSearchResponseAsMap));

        List<Map<String, Object>> hitsNestedListSecondQuery = getNestedHits(secondSearchResponseAsMap);
        List<String> idsSecondQuery = new ArrayList<>();
        List<Double> scoresSecondQuery = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedListSecondQuery) {
            idsSecondQuery.add((String) oneHit.get("_id"));
            scoresSecondQuery.add((Double) oneHit.get("_score"));
        }

        // verify that scores are in desc order
        assertTrue(
            IntStream.range(0, scoresSecondQuery.size() - 1).noneMatch(idx -> scoresSecondQuery.get(idx) < scoresSecondQuery.get(idx + 1))
        );
        // verify that all ids are unique
        assertEquals(Set.copyOf(idsSecondQuery).size(), idsSecondQuery.size());

        Map<String, Object> totalSecondQuery = getTotalHits(secondSearchResponseAsMap);
        assertNotNull(totalSecondQuery.get("value"));
        assertEquals(firstQueryHitCount, totalSecondQuery.get("value"));
        assertNotNull(totalSecondQuery.get("relation"));
        assertEquals(RELATION_EQUAL_TO, totalSecondQuery.get("relation"));
    }

    @SneakyThrows
    public void testWrappedQueryWithFilter_whenIndexAliasHasFilterAndIndexWithNestedFields_thenSuccess() {
        String alias = "alias_with_filter";
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            // create alias for index
            QueryBuilder aliasFilter = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));
            createIndexAlias(TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME, alias, aliasFilter);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(QueryBuilders.existsQuery(TEST_TEXT_FIELD_NAME_1));

            Map<String, Object> searchResponseAsMap = search(
                alias,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(2, getHitCount(searchResponseAsMap));
            assertTrue(getMaxScore(searchResponseAsMap).isPresent());
            assertEquals(1.0f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> total = getTotalHits(searchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(2, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            deleteIndexAlias(TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME, alias);
        }
    }

    @SneakyThrows
    public void testWrappedQueryWithFilter_whenIndexAliasHasFilters_thenSuccess() {
        String alias = "alias_with_filter";
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            // create alias for index
            QueryBuilder aliasFilter = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.matchQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));
            createIndexAlias(TEST_MULTI_DOC_INDEX_NAME, alias, aliasFilter);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(QueryBuilders.existsQuery(TEST_TEXT_FIELD_NAME_1));

            Map<String, Object> searchResponseAsMap = search(
                alias,
                hybridQueryBuilder,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertEquals(2, getHitCount(searchResponseAsMap));
            assertTrue(getMaxScore(searchResponseAsMap).isPresent());
            assertEquals(1.0f, getMaxScore(searchResponseAsMap).get(), DELTA_FOR_SCORE_ASSERTION);

            Map<String, Object> total = getTotalHits(searchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(2, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            deleteIndexAlias(TEST_MULTI_DOC_INDEX_NAME, alias);
        }
    }

    @SneakyThrows
    public void testConcurrentSearchWithMultipleSlices_whenSingleShardIndex_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
            int numberOfDocumentsInIndex = 1_000;
            initializeIndexIfNotExist(TEST_INDEX_DOC_QTY_ONE_SHARD, SINGLE_SHARD, numberOfDocumentsInIndex);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(QueryBuilders.matchAllQuery());

            // first query with cache flag executed normally by reading documents from index
            Map<String, Object> firstSearchResponseAsMap = search(
                TEST_INDEX_DOC_QTY_ONE_SHARD,
                hybridQueryBuilder,
                null,
                numberOfDocumentsInIndex,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            int queryHitCount = getHitCount(firstSearchResponseAsMap);
            assertEquals(numberOfDocumentsInIndex, queryHitCount);

            List<Map<String, Object>> hitsNestedList = getNestedHits(firstSearchResponseAsMap);
            List<String> ids = new ArrayList<>();
            List<Double> scores = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                ids.add((String) oneHit.get("_id"));
                scores.add((Double) oneHit.get("_score"));
            }

            // verify that scores are in desc order
            assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
            // verify that all ids are unique
            assertEquals(Set.copyOf(ids).size(), ids.size());

            Map<String, Object> total = getTotalHits(firstSearchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(numberOfDocumentsInIndex, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        }
    }

    @SneakyThrows
    public void testConcurrentSearchWithMultipleSlices_whenMultipleShardsIndex_thenSuccessful() {
        try {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
            int numberOfDocumentsInIndex = 2_000;
            initializeIndexIfNotExist(TEST_INDEX_DOC_QTY_MULTIPLE_SHARDS, MULTIPLE_SHARDS, numberOfDocumentsInIndex);
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(QueryBuilders.matchAllQuery());
            hybridQueryBuilder.add(QueryBuilders.rangeQuery(INTEGER_FIELD_PRICE).gte(0).lte(1000));

            // first query with cache flag executed normally by reading documents from index
            Map<String, Object> firstSearchResponseAsMap = search(
                TEST_INDEX_DOC_QTY_MULTIPLE_SHARDS,
                hybridQueryBuilder,
                null,
                numberOfDocumentsInIndex,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            int queryHitCount = getHitCount(firstSearchResponseAsMap);
            assertEquals(numberOfDocumentsInIndex, queryHitCount);

            List<Map<String, Object>> hitsNestedList = getNestedHits(firstSearchResponseAsMap);
            List<String> ids = new ArrayList<>();
            List<Double> scores = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                ids.add((String) oneHit.get("_id"));
                scores.add((Double) oneHit.get("_score"));
            }

            // verify that scores are in desc order
            assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
            // verify that all ids are unique
            assertEquals(Set.copyOf(ids).size(), ids.size());

            Map<String, Object> total = getTotalHits(firstSearchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(numberOfDocumentsInIndex, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        } finally {
            updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        }
    }

    @SneakyThrows
    public void testPaginationOnSingleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        testHybridQuery_whenFromAndPaginationDepthIsGreaterThanZero_thenSuccessful(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
    }

    @SneakyThrows
    public void testPaginationOnSingleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        testHybridQuery_whenFromAndPaginationDepthIsGreaterThanZero_thenSuccessful(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
    }

    @SneakyThrows
    public void testPaginationOnMultipleShard_whenConcurrentSearchEnabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, true);
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        testHybridQuery_whenFromAndPaginationDepthIsGreaterThanZero_thenSuccessful(TEST_MULTI_DOC_INDEX_NAME);
    }

    @SneakyThrows
    public void testPaginationOnMultipleShard_whenConcurrentSearchDisabled_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        testHybridQuery_whenFromAndPaginationDepthIsGreaterThanZero_thenSuccessful(TEST_MULTI_DOC_INDEX_NAME);
    }

    @SneakyThrows
    public void testHybridQuery_whenFromAndPaginationDepthIsGreaterThanZero_thenSuccessful(String indexName) {
        HybridQueryBuilder hybridQueryBuilderOnlyMatchAll = new HybridQueryBuilder();
        hybridQueryBuilderOnlyMatchAll.add(new MatchAllQueryBuilder());
        hybridQueryBuilderOnlyMatchAll.paginationDepth(10);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilderOnlyMatchAll,
            null,
            10,
            Map.of("search_pipeline", SEARCH_PIPELINE),
            null,
            null,
            null,
            false,
            null,
            2
        );

        assertEquals(2, getHitCount(searchResponseAsMap));
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(4, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    @SneakyThrows
    public void testHybridQuery_whenFromIsGreaterThanTotalResultCount_thenFail() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        HybridQueryBuilder hybridQueryBuilderOnlyMatchAll = new HybridQueryBuilder();
        hybridQueryBuilderOnlyMatchAll.add(new MatchAllQueryBuilder());
        hybridQueryBuilderOnlyMatchAll.paginationDepth(10);

        ResponseException responseException = assertThrows(
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD,
                hybridQueryBuilderOnlyMatchAll,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                null,
                false,
                null,
                5
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            responseException.getMessage(),
            allOf(containsString("Reached end of search result, increase pagination_depth value to see more results"))
        );
    }

    @SneakyThrows
    public void testHybridQuery_whenPaginationDepthIsOutOfRange_thenFail() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
        HybridQueryBuilder hybridQueryBuilderOnlyMatchAll = new HybridQueryBuilder();
        hybridQueryBuilderOnlyMatchAll.add(new MatchAllQueryBuilder());
        hybridQueryBuilderOnlyMatchAll.paginationDepth(100001);

        ResponseException responseException = assertThrows(
            ResponseException.class,
            () -> search(
                TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD,
                hybridQueryBuilderOnlyMatchAll,
                null,
                10,
                Map.of("search_pipeline", SEARCH_PIPELINE),
                null,
                null,
                null,
                false,
                null,
                0
            )
        );

        org.hamcrest.MatcherAssert.assertThat(
            responseException.getMessage(),
            allOf(containsString("pagination_depth should be less than or equal to index.max_result_window setting"))
        );
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) throws Exception {
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_BASIC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray())
            );
            assertEquals(1, getDocCount(TEST_BASIC_INDEX_NAME));
        }
        if (TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                )
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "1",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector1).toArray(), Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "2",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector2).toArray(), Floats.asList(testVector2).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            addKnnDoc(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                "3",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector3).toArray(), Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            assertEquals(3, getDocCount(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME));
        }

        if (TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                    Collections.singletonMap(TEST_NESTED_TYPE_FIELD_NAME_1, Map.of()),
                    1
                ),
                ""
            );
            addDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                    Map.of(),
                    1
                ),
                ""
            );
            addDocsToIndex(TEST_MULTI_DOC_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD.equals(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                1
            );
            addDocsToIndex(TEST_MULTI_DOC_INDEX_NAME_ONE_SHARD);
        }

        if (TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                    Map.of(TEST_NESTED_TYPE_FIELD_NAME_1, Map.of()),
                    SINGLE_SHARD
                ),
                ""
            );

            addDocsToIndex(TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD);
            List<Map<String, String>> nestedFields = new ArrayList<>();
            nestedFields.add(
                Map.ofEntries(Map.entry(NESTED_FIELD_1, NESTED_FIELD_1_VALUE), Map.entry(NESTED_FIELD_2, NESTED_FIELD_2_VALUE))
            );

            addKnnDoc(
                TEST_MULTI_DOC_INDEX_WITH_NESTED_TYPE_NAME_ONE_SHARD,
                "4",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                List.of(),
                List.of(),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                Map.of(TEST_NESTED_TYPE_FIELD_NAME_1, nestedFields)
            );
        }

        if (TEST_INDEX_WITH_KEYWORDS_ONE_SHARD.equals(indexName) && !indexExists(TEST_INDEX_WITH_KEYWORDS_ONE_SHARD)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    List.of(),
                    Collections.emptyMap(),
                    List.of(INTEGER_FIELD_PRICE),
                    List.of(KEYWORD_FIELD_1),
                    List.of(),
                    SINGLE_SHARD
                ),
                ""
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "1",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_1_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_1_VALUE
            );
            addDocWithKeywordsAndIntFields(indexName, "2", INTEGER_FIELD_PRICE, INTEGER_FIELD_PRICE_2_VALUE, null, null);
            addDocWithKeywordsAndIntFields(
                indexName,
                "3",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_3_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_2_VALUE
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "4",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_4_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_3_VALUE
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "5",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_5_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_4_VALUE
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "6",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_6_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_4_VALUE
            );
        }

        if (TEST_INDEX_WITH_KEYWORDS_THREE_SHARDS.equals(indexName) && !indexExists(TEST_INDEX_WITH_KEYWORDS_THREE_SHARDS)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    List.of(),
                    Collections.emptyMap(),
                    List.of(INTEGER_FIELD_PRICE),
                    List.of(KEYWORD_FIELD_1),
                    List.of(),
                    3
                ),
                ""
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "1",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_1_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_1_VALUE
            );
            addDocWithKeywordsAndIntFields(indexName, "2", INTEGER_FIELD_PRICE, INTEGER_FIELD_PRICE_2_VALUE, null, null);
            addDocWithKeywordsAndIntFields(
                indexName,
                "3",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_3_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_2_VALUE
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "4",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_4_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_3_VALUE
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "5",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_5_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_4_VALUE
            );
            addDocWithKeywordsAndIntFields(
                indexName,
                "6",
                INTEGER_FIELD_PRICE,
                INTEGER_FIELD_PRICE_6_VALUE,
                KEYWORD_FIELD_1,
                KEYWORD_FIELD_4_VALUE
            );
        }
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName, int numberOfShards, int numberOfDocuments) throws Exception {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    List.of(),
                    Collections.emptyMap(),
                    List.of(INTEGER_FIELD_PRICE),
                    List.of(KEYWORD_FIELD_1),
                    List.of(),
                    numberOfShards
                ),
                ""
            );
            for (int i = 0; i < numberOfDocuments; i++) {
                addDocWithKeywordsAndIntFields(
                    indexName,
                    String.valueOf(i),
                    INTEGER_FIELD_PRICE,
                    RandomUtils.nextInt(1000),
                    KEYWORD_FIELD_1,
                    RandomStringUtils.randomAlphabetic(10)
                );
            }
        }
    }

    private void addDocsToIndex(final String testMultiDocIndexName) {
        addKnnDoc(
            testMultiDocIndexName,
            "1",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
            Collections.singletonList(Floats.asList(testVector1).toArray()),
            Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
            Collections.singletonList(TEST_DOC_TEXT1)
        );
        addKnnDoc(
            testMultiDocIndexName,
            "2",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
            Collections.singletonList(Floats.asList(testVector2).toArray())
        );
        addKnnDoc(
            testMultiDocIndexName,
            "3",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
            Collections.singletonList(Floats.asList(testVector3).toArray()),
            Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
            Collections.singletonList(TEST_DOC_TEXT2)
        );
        addKnnDoc(
            testMultiDocIndexName,
            "4",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
            Collections.singletonList(TEST_DOC_TEXT3)
        );
        assertEquals(4, getDocCount(testMultiDocIndexName));
    }

    private List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    private Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }

    private Optional<Float> getMaxScore(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return hitsMap.get("max_score") == null ? Optional.empty() : Optional.of(((Double) hitsMap.get("max_score")).floatValue());
    }

    private void addDocWithKeywordsAndIntFields(
        final String indexName,
        final String docId,
        final String integerField,
        final Integer integerFieldValue,
        final String keywordField,
        final String keywordFieldValue
    ) {
        List<String> intFields = integerField == null ? List.of() : List.of(integerField);
        List<Integer> intValues = integerFieldValue == null ? List.of() : List.of(integerFieldValue);
        List<String> keywordFields = keywordField == null ? List.of() : List.of(keywordField);
        List<String> keywordValues = keywordFieldValue == null ? List.of() : List.of(keywordFieldValue);

        indexTheDocument(
            indexName,
            docId,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            intFields,
            intValues,
            keywordFields,
            keywordValues,
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            null
        );
    }

}
