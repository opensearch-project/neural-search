/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.opensearch.test.OpenSearchTestCase.randomFloat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.lang3.Range;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;

public class TestUtils {

    public static final String RELATION_EQUAL_TO = "eq";
    public static final float DELTA_FOR_SCORE_ASSERTION = 0.001f;
    public static final float DELTA_FOR_FLOATS_ASSERTION = 0.001f;
    public static final String RESTART_UPGRADE_OLD_CLUSTER = "tests.is_old_cluster";
    public static final String BWC_VERSION = "tests.plugin_bwc_version";
    public static final String NEURAL_SEARCH_BWC_PREFIX = "neuralsearch-bwc-";
    public static final String OLD_CLUSTER = "old_cluster";
    public static final String MIXED_CLUSTER = "mixed_cluster";
    public static final String UPGRADED_CLUSTER = "upgraded_cluster";
    public static final String ROLLING_UPGRADE_FIRST_ROUND = "tests.rest.first_round";
    public static final String BWCSUITE_CLUSTER = "tests.rest.bwcsuite_cluster";
    public static final String NODES_BWC_CLUSTER = "3";
    public static final int TEST_DIMENSION = 768;
    public static final SpaceType TEST_SPACE_TYPE = SpaceType.L2;
    public static final String CLIENT_TIMEOUT_VALUE = "90s";
    public static final String OPENDISTRO_SECURITY = ".opendistro_security";
    public static final String SKIP_DELETE_MODEL_INDEX = "tests.skip_delete_model_index";
    public static final String SECURITY_AUDITLOG_PREFIX = "security-auditlog";
    public static final String OPENSEARCH_SYSTEM_INDEX_PREFIX = ".opensearch";
    public static final String TEXT_EMBEDDING_PROCESSOR = "text_embedding";
    public static final String TEXT_IMAGE_EMBEDDING_PROCESSOR = "text_image_embedding";
    public static final int MAX_TASK_RESULT_QUERY_TIME_IN_SECOND = 60 * 5;
    public static final int DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND = 1000;
    public static final String DEFAULT_USER_AGENT = "Kibana";
    public static final String DEFAULT_NORMALIZATION_METHOD = "min_max";
    public static final String DEFAULT_COMBINATION_METHOD = "arithmetic_mean";
    public static final String PARAM_NAME_WEIGHTS = "weights";
    public static final String SPARSE_ENCODING_PROCESSOR = "sparse_encoding";
    public static final String TEXT_CHUNKING_PROCESSOR = "text_chunking";
    public static final int MAX_TIME_OUT_INTERVAL = 3000;
    public static final int MAX_RETRY = 5;

    /**
     * Convert an xContentBuilder to a map
     * @param xContentBuilder to produce map from
     * @return Map from xContentBuilder
     */
    public static Map<String, Object> xContentBuilderToMap(XContentBuilder xContentBuilder) {
        return XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2();
    }

    /**
     * Utility method to convert an object to a float
     *
     * @param obj object to be converted to float
     * @return object as float
     */
    public static Float objectToFloat(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).floatValue();
        }

        throw new IllegalArgumentException("Object provided must be of type Number");
    }

    /**
     * Create a random vector of provided dimension
     *
     * @param dimension of vector to be created
     * @return dimension-dimensional floating point array with random content
     */
    public static float[] createRandomVector(int dimension) {
        float[] vector = new float[dimension];
        for (int j = 0; j < dimension; j++) {
            vector[j] = randomFloat();
        }
        return vector;
    }

    /**
     * Create a map of provided tokens, the values will be random float numbers
     *
     * @param tokens of the created map keys
     * @return token weight map with random weight > 0
     */
    public static Map<String, Float> createRandomTokenWeightMap(Collection<String> tokens) {
        Map<String, Float> resultMap = new HashMap<>();
        for (String token : tokens) {
            // use a small shift to ensure value > 0
            resultMap.put(token, Math.abs(randomFloat()) + 1e-3f);
        }
        return resultMap;
    }

    /**
     * Assert results of hybrid query after score normalization and combination
     * @param querySearchResults collection of query search results after they processed by normalization processor
     */
    public static void assertQueryResultScores(List<QuerySearchResult> querySearchResults) {
        assertNotNull(querySearchResults);
        float maxScore = querySearchResults.stream()
            .map(searchResult -> searchResult.topDocs().maxScore)
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, maxScore, DELTA_FOR_SCORE_ASSERTION);
        float totalMaxScore = querySearchResults.stream()
            .map(searchResult -> searchResult.getMaxScore())
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, totalMaxScore, DELTA_FOR_SCORE_ASSERTION);
        float maxScoreScoreFromScoreDocs = querySearchResults.stream()
            .map(
                searchResult -> Arrays.stream(searchResult.topDocs().topDocs.scoreDocs)
                    .map(scoreDoc -> scoreDoc.score)
                    .max(Float::compare)
                    .orElse(Float.MAX_VALUE)
            )
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, maxScoreScoreFromScoreDocs, DELTA_FOR_SCORE_ASSERTION);
        float minScoreScoreFromScoreDocs = querySearchResults.stream()
            .map(
                searchResult -> Arrays.stream(searchResult.topDocs().topDocs.scoreDocs)
                    .map(scoreDoc -> scoreDoc.score)
                    .min(Float::compare)
                    .orElse(Float.MAX_VALUE)
            )
            .min(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(0.001f, minScoreScoreFromScoreDocs, DELTA_FOR_SCORE_ASSERTION);
    }

    /**
     * Assert results of hybrid query after score normalization and combination
     * @param querySearchResults collection of query search results after they processed by normalization processor
     */
    public static void assertQueryResultScoresWithNoMatches(List<QuerySearchResult> querySearchResults) {
        assertNotNull(querySearchResults);
        float maxScore = querySearchResults.stream()
            .map(searchResult -> searchResult.topDocs().maxScore)
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(0.0f, maxScore, DELTA_FOR_SCORE_ASSERTION);
        float totalMaxScore = querySearchResults.stream().map(QuerySearchResult::getMaxScore).max(Float::compare).orElse(Float.MAX_VALUE);
        assertEquals(0.0f, totalMaxScore, DELTA_FOR_SCORE_ASSERTION);
        float maxScoreScoreFromScoreDocs = querySearchResults.stream()
            .map(
                searchResult -> Arrays.stream(searchResult.topDocs().topDocs.scoreDocs)
                    .map(scoreDoc -> scoreDoc.score)
                    .max(Float::compare)
                    .orElse(0.0f)
            )
            .max(Float::compare)
            .orElse(0.0f);
        assertEquals(0.0f, maxScoreScoreFromScoreDocs, DELTA_FOR_SCORE_ASSERTION);
        float minScoreScoreFromScoreDocs = querySearchResults.stream()
            .map(
                searchResult -> Arrays.stream(searchResult.topDocs().topDocs.scoreDocs)
                    .map(scoreDoc -> scoreDoc.score)
                    .min(Float::compare)
                    .orElse(0.0f)
            )
            .min(Float::compare)
            .orElse(0.0f);
        assertEquals(0.001f, minScoreScoreFromScoreDocs, DELTA_FOR_SCORE_ASSERTION);

        assertFalse(
            querySearchResults.stream()
                .map(searchResult -> searchResult.topDocs().topDocs.totalHits)
                .filter(totalHits -> Objects.isNull(totalHits.relation))
                .filter(totalHits -> TotalHits.Relation.EQUAL_TO != totalHits.relation)
                .anyMatch(totalHits -> 0 != totalHits.value)
        );
    }

    /**
     * Assert results of hybrid query after score normalization and combination
     * @param searchResponseWithWeightsAsMap collection of query search results after they processed by normalization processor
     * @param expectedMaxScore expected maximum score
     * @param expectedMaxMinusOneScore second highest expected score
     * @param expectedMinScore expected minimal score
     */
    public static void assertWeightedScores(
        Map<String, Object> searchResponseWithWeightsAsMap,
        double expectedMaxScore,
        double expectedMaxMinusOneScore,
        double expectedMinScore
    ) {
        assertNotNull(searchResponseWithWeightsAsMap);
        Map<String, Object> totalWeights = getTotalHits(searchResponseWithWeightsAsMap);
        assertNotNull(totalWeights.get("value"));
        assertEquals(4, totalWeights.get("value"));
        assertNotNull(totalWeights.get("relation"));
        assertEquals(RELATION_EQUAL_TO, totalWeights.get("relation"));
        assertTrue(getMaxScore(searchResponseWithWeightsAsMap).isPresent());
        assertEquals(expectedMaxScore, getMaxScore(searchResponseWithWeightsAsMap).get(), 0.001f);

        List<Double> scoresWeights = new ArrayList<>();
        for (Map<String, Object> oneHit : getNestedHits(searchResponseWithWeightsAsMap)) {
            scoresWeights.add((Double) oneHit.get("_score"));
        }
        // verify scores order
        assertTrue(IntStream.range(0, scoresWeights.size() - 1).noneMatch(idx -> scoresWeights.get(idx) < scoresWeights.get(idx + 1)));
        // verify the scores are normalized with inclusion of weights
        assertEquals(expectedMaxScore, scoresWeights.get(0), 0.001);
        assertEquals(expectedMaxMinusOneScore, scoresWeights.get(1), 0.001);
        assertEquals(expectedMinScore, scoresWeights.get(scoresWeights.size() - 1), 0.001);
    }

    /**
     * Assert results of hybrid query after score normalization and combination
     * @param searchResponseAsMap collection of query search results after they processed by normalization processor
     * @param totalExpectedDocQty expected total document quantity
     * @param minMaxScoreRange range of scores from min to max inclusive
     */
    public static void assertHybridSearchResults(
        Map<String, Object> searchResponseAsMap,
        int totalExpectedDocQty,
        float[] minMaxScoreRange
    ) {
        assertNotNull(searchResponseAsMap);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(totalExpectedDocQty, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        assertTrue(Range.between(minMaxScoreRange[0], minMaxScoreRange[1]).contains(getMaxScore(searchResponseAsMap).get()));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }
        // verify scores order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify the scores are normalized. for l2 scores max score will not be 1.0 so we're checking on a range
        assertTrue(
            Range.between(minMaxScoreRange[0], minMaxScoreRange[1])
                .contains(scores.stream().map(Double::floatValue).max(Double::compare).get())
        );

        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());
    }

    /**
     * Assert results of a fetch phase for hybrid query
     * @param fetchSearchResult results produced by fetch phase
     * @param expectedNumberOfHits expected number of hits that should be in the fetch result object
     */
    public static void assertFetchResultScores(FetchSearchResult fetchSearchResult, int expectedNumberOfHits) {
        assertNotNull(fetchSearchResult);
        assertNotNull(fetchSearchResult.hits());
        SearchHits searchHits = fetchSearchResult.hits();
        float maxScore = searchHits.getMaxScore();
        assertEquals(1.0f, maxScore, DELTA_FOR_SCORE_ASSERTION);
        TotalHits totalHits = searchHits.getTotalHits();
        assertNotNull(totalHits);
        assertEquals(expectedNumberOfHits, totalHits.value);
        assertNotNull(searchHits.getHits());
        assertEquals(expectedNumberOfHits, searchHits.getHits().length);
        float maxScoreScoreFromScoreDocs = Arrays.stream(searchHits.getHits())
            .map(SearchHit::getScore)
            .max(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(1.0f, maxScoreScoreFromScoreDocs, DELTA_FOR_SCORE_ASSERTION);
        float minScoreScoreFromScoreDocs = Arrays.stream(searchHits.getHits())
            .map(SearchHit::getScore)
            .min(Float::compare)
            .orElse(Float.MAX_VALUE);
        assertEquals(0.001f, minScoreScoreFromScoreDocs, DELTA_FOR_SCORE_ASSERTION);
    }

    public static void assertHitResultsFromQuery(int expected, Map<String, Object> searchResponseAsMap) {
        assertHitResultsFromQuery(expected, expected, searchResponseAsMap);
    }

    public static void assertHitResultsFromQuery(int expected, int expectedTotal, Map<String, Object> searchResponseAsMap) {
        assertEquals(expected, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
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

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(expectedTotal, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    public static void assertHitResultsFromQueryWhenSortIsEnabled(
        int expectedCollectedHits,
        int expectedTotalHits,
        Map<String, Object> searchResponseAsMap
    ) {
        assertEquals(expectedCollectedHits, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
        }
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(expectedTotalHits, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    public static List<Map<String, Object>> getNestedHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (List<Map<String, Object>>) hitsMap.get("hits");
    }

    public static Map<String, Object> getTotalHits(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return (Map<String, Object>) hitsMap.get("total");
    }

    public static Optional<Float> getMaxScore(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        return hitsMap.get("max_score") == null ? Optional.empty() : Optional.of(((Double) hitsMap.get("max_score")).floatValue());
    }

    @SuppressWarnings("unchecked")
    private static int getHitCount(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hitsList = (List<Object>) hitsMap.get("hits");
        return hitsList.size();
    }

    public static String getModelId(Map<String, Object> pipeline, String processor) {
        assertNotNull(pipeline);
        ArrayList<Map<String, Object>> processors = (ArrayList<Map<String, Object>>) pipeline.get("processors");
        Map<String, Object> textEmbeddingProcessor = (Map<String, Object>) processors.get(0).get(processor);
        String modelId = (String) textEmbeddingProcessor.get("model_id");
        assertNotNull(modelId);
        return modelId;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getValueByKey(Map<String, Object> map, String key) {
        assertNotNull(map);
        Object value = map.get(key);
        return (T) value;
    }

    public static String generateModelId() {
        return "public_model_" + RandomizedTest.randomAsciiAlphanumOfLength(8);
    }
}
