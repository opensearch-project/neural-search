/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.util.TestUtils.getMaxScore;
import static org.opensearch.neuralsearch.util.TestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;
import static org.opensearch.neuralsearch.util.TestUtils.getValueByKey;

public class HybridQueryExplainIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME = "test-hybrid-index-explain";
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_INDEX_NAME = "test-hybrid-index-explain";
    private static final String TEST_MULTI_DOC_INDEX_NAME = "test-hybrid-multi-doc-index-explain";
    private static final String TEST_LARGE_DOCS_INDEX_NAME = "test-hybrid-large-docs-index-explain";

    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_TEXT_FIELD_NAME_2 = "test-text-field-2";
    private static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    private static final String NORMALIZATION_TECHNIQUE_L2 = "l2";
    private static final int MAX_NUMBER_OF_DOCS_IN_LARGE_INDEX = 2_000;
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private static final String NORMALIZATION_SEARCH_PIPELINE = "normalization-search-pipeline";
    private static final String RRF_SEARCH_PIPELINE = "rrf-search-pipeline";

    static final Supplier<float[]> TEST_VECTOR_SUPPLIER = () -> new float[768];

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @SneakyThrows
    public void testExplain_whenMultipleSubqueriesAndOneShard_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        // create search pipeline with both normalization processor and explain response processor
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            false,
            true
        );

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
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );
        // Assert
        // search hits
        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap1);
        assertNotNull(total.get("value"));
        assertEquals(3, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> topLevelExplanationsHit1 = getValueByKey(searchHit1, "_explanation");
        assertExplanation(topLevelExplanationsHit1, searchHit1, hitsNestedList, false);
    }

    @SneakyThrows
    public void testExplain_whenMultipleSubqueriesAndMultipleShards_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            NORMALIZATION_TECHNIQUE_L2,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
            false,
            true
        );

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        KNNQueryBuilder knnQueryBuilder = KNNQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .vector(createRandomVector(TEST_DIMENSION))
            .k(10)
            .build();
        hybridQueryBuilder.add(QueryBuilders.existsQuery(TEST_TEXT_FIELD_NAME_1));
        hybridQueryBuilder.add(knnQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );
        // Assert
        // basic sanity check for search hits
        assertEquals(4, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        float actualMaxScore = getMaxScore(searchResponseAsMap).get();
        assertTrue(actualMaxScore > 0);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(4, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain, hit 1
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> explanationForHit1 = getValueByKey(searchHit1, "_explanation");
        assertNotNull(explanationForHit1);
        assertEquals((double) searchHit1.get("_score"), (double) explanationForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        String expectedTopLevelDescription = "arithmetic_mean, weights [0.3, 0.7] combination of:";
        assertEquals(expectedTopLevelDescription, explanationForHit1.get("description"));
        List<Map<String, Object>> hit1Details = getListOfValues(explanationForHit1, "details");
        assertEquals(2, hit1Details.size());
        // two sub-queries meaning we do have two detail objects with separate query level details
        Map<String, Object> hit1DetailsForHit1 = hit1Details.get(0);
        assertTrue((double) hit1DetailsForHit1.get("value") > 0.5f);
        assertEquals("l2 normalization of:", hit1DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit1 = getListOfValues(hit1DetailsForHit1, "details").get(0);
        assertEquals("ConstantScore(FieldExistsQuery [field=test-text-field-1])", explanationsHit1.get("description"));
        assertTrue((double) explanationsHit1.get("value") > 0.5f);
        assertEquals(0, ((List) explanationsHit1.get("details")).size());

        Map<String, Object> hit1DetailsForHit2 = hit1Details.get(1);
        assertTrue((double) hit1DetailsForHit2.get("value") > 0.0f);
        assertEquals("l2 normalization of:", hit1DetailsForHit2.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit2.get("details")).size());

        Map<String, Object> explanationsHit2 = getListOfValues(hit1DetailsForHit2, "details").get(0);
        assertEquals("within top 3 docs", explanationsHit2.get("description"));
        assertTrue((double) explanationsHit2.get("value") > 0.0f);
        assertEquals(0, ((List) explanationsHit2.get("details")).size());

        // hit 2
        Map<String, Object> searchHit2 = hitsNestedList.get(1);
        Map<String, Object> explanationForHit2 = getValueByKey(searchHit2, "_explanation");
        assertNotNull(explanationForHit2);
        assertEquals((double) searchHit2.get("_score"), (double) explanationForHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, explanationForHit2.get("description"));
        List<Map<String, Object>> hit2Details = getListOfValues(explanationForHit2, "details");
        assertEquals(2, hit2Details.size());

        Map<String, Object> hit2DetailsForHit1 = hit2Details.get(0);
        assertTrue((double) hit2DetailsForHit1.get("value") > 0.5f);
        assertEquals("l2 normalization of:", hit2DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit2DetailsForHit1.get("details")).size());

        Map<String, Object> hit2DetailsForHit2 = hit2Details.get(1);
        assertTrue((double) hit2DetailsForHit2.get("value") > 0.0f);
        assertEquals("l2 normalization of:", hit2DetailsForHit2.get("description"));
        assertEquals(1, ((List) hit2DetailsForHit2.get("details")).size());

        // hit 3
        Map<String, Object> searchHit3 = hitsNestedList.get(2);
        Map<String, Object> explanationForHit3 = getValueByKey(searchHit3, "_explanation");
        assertNotNull(explanationForHit3);
        assertEquals((double) searchHit3.get("_score"), (double) explanationForHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, explanationForHit3.get("description"));
        List<Map<String, Object>> hit3Details = getListOfValues(explanationForHit3, "details");
        assertEquals(1, hit3Details.size());

        Map<String, Object> hit3DetailsForHit1 = hit3Details.get(0);
        assertTrue((double) hit3DetailsForHit1.get("value") > 0.5f);
        assertEquals("l2 normalization of:", hit3DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit3DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit3 = getListOfValues(hit3DetailsForHit1, "details").get(0);
        assertEquals("within top 3 docs", explanationsHit3.get("description"));
        assertEquals(0, getListOfValues(explanationsHit3, "details").size());
        assertTrue((double) explanationsHit3.get("value") > 0.0f);

        // hit 4
        Map<String, Object> searchHit4 = hitsNestedList.get(3);
        Map<String, Object> explanationForHit4 = getValueByKey(searchHit4, "_explanation");
        assertNotNull(explanationForHit4);
        assertEquals((double) searchHit4.get("_score"), (double) explanationForHit4.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, explanationForHit4.get("description"));
        List<Map<String, Object>> hit4Details = getListOfValues(explanationForHit4, "details");
        assertEquals(1, hit4Details.size());

        Map<String, Object> hit4DetailsForHit1 = hit4Details.get(0);
        assertTrue((double) hit4DetailsForHit1.get("value") > 0.5f);
        assertEquals("l2 normalization of:", hit4DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit4DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit4 = getListOfValues(hit4DetailsForHit1, "details").get(0);
        assertEquals("ConstantScore(FieldExistsQuery [field=test-text-field-1])", explanationsHit4.get("description"));
        assertEquals(0, getListOfValues(explanationsHit4, "details").size());
        assertTrue((double) explanationsHit4.get("value") > 0.0f);
    }

    @SneakyThrows
    public void testExplanationResponseProcessor_whenProcessorIsNotConfigured_thenResponseHasQueryExplanations() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        // create search pipeline with normalization processor, no explanation response processor
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            false,
            false
        );

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
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );
        // Assert
        // search hits
        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap1);
        assertNotNull(total.get("value"));
        assertEquals(3, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> topLevelExplanationsHit1 = getValueByKey(searchHit1, "_explanation");
        assertNotNull(topLevelExplanationsHit1);
        assertEquals(0.343f, (double) topLevelExplanationsHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        String expectedTopLevelDescription = "combined score of:";
        assertEquals(expectedTopLevelDescription, topLevelExplanationsHit1.get("description"));
        List<Map<String, Object>> normalizationExplanationHit1 = getListOfValues(topLevelExplanationsHit1, "details");
        assertEquals(2, normalizationExplanationHit1.size());

        Map<String, Object> noMatchDetailsForHit1 = normalizationExplanationHit1.get(0);
        assertEquals(0.0f, (double) noMatchDetailsForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("no matching term", noMatchDetailsForHit1.get("description"));
        assertEquals(0, ((List) noMatchDetailsForHit1.get("details")).size());

        Map<String, Object> hit1DetailsForHit1 = normalizationExplanationHit1.get(1);
        assertEquals(0.343f, (double) hit1DetailsForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("sum of:", hit1DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit1 = getListOfValues(hit1DetailsForHit1, "details").get(0);
        assertEquals("weight(test-text-field-1:place in 0) [PerFieldSimilarity], result of:", explanationsHit1.get("description"));
        assertEquals(0.343f, (double) explanationsHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, ((List) explanationsHit1.get("details")).size());

        Map<String, Object> explanationsHit1Details = getListOfValues(explanationsHit1, "details").get(0);
        assertEquals(0.343f, (double) explanationsHit1Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("score(freq=1.0), computed as boost * idf * tf from:", explanationsHit1Details.get("description"));
        assertEquals(2, getListOfValues(explanationsHit1Details, "details").size());

        Map<String, Object> explanationsDetails2Hit1Details = getListOfValues(explanationsHit1Details, "details").get(0);
        assertEquals(0.693f, (double) explanationsDetails2Hit1Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:", explanationsDetails2Hit1Details.get("description"));
        assertFalse(getListOfValues(explanationsDetails2Hit1Details, "details").isEmpty());

        Map<String, Object> explanationsDetails3Hit1Details = getListOfValues(explanationsHit1Details, "details").get(1);
        assertEquals(0.495f, (double) explanationsDetails3Hit1Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(
            "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
            explanationsDetails3Hit1Details.get("description")
        );
        assertFalse(getListOfValues(explanationsDetails3Hit1Details, "details").isEmpty());

        // search hit 2
        Map<String, Object> searchHit2 = hitsNestedList.get(1);
        Map<String, Object> topLevelExplanationsHit2 = getValueByKey(searchHit2, "_explanation");
        assertNotNull(topLevelExplanationsHit2);
        assertEquals(0.13f, (double) topLevelExplanationsHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, topLevelExplanationsHit2.get("description"));
        List<Map<String, Object>> normalizationExplanationHit2 = getListOfValues(topLevelExplanationsHit2, "details");
        assertEquals(2, normalizationExplanationHit2.size());

        Map<String, Object> hit1DetailsForHit2 = normalizationExplanationHit2.get(0);
        assertEquals(0.13f, (double) hit1DetailsForHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("weight(test-text-field-1:hello in 0) [PerFieldSimilarity], result of:", hit1DetailsForHit2.get("description"));
        assertEquals(1, getListOfValues(hit1DetailsForHit2, "details").size());

        Map<String, Object> explanationsHit2 = getListOfValues(hit1DetailsForHit2, "details").get(0);
        assertEquals(0.13f, (double) explanationsHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("score(freq=1.0), computed as boost * idf * tf from:", explanationsHit2.get("description"));
        assertEquals(2, getListOfValues(explanationsHit2, "details").size());

        Map<String, Object> explanationsHit2Details = getListOfValues(explanationsHit2, "details").get(1);
        assertEquals(0.454f, (double) explanationsHit2Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:", explanationsHit2Details.get("description"));
        assertEquals(5, getListOfValues(explanationsHit2Details, "details").size());

        Map<String, Object> hit1DetailsForHit2NoMatch = normalizationExplanationHit2.get(0);
        assertEquals(0.13f, (double) hit1DetailsForHit2NoMatch.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("weight(test-text-field-1:hello in 0) [PerFieldSimilarity], result of:", hit1DetailsForHit2NoMatch.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit2NoMatch.get("details")).size());

        // search hit 3
        Map<String, Object> searchHit3 = hitsNestedList.get(1);
        Map<String, Object> topLevelExplanationsHit3 = getValueByKey(searchHit3, "_explanation");
        assertNotNull(topLevelExplanationsHit3);
        assertEquals(0.13f, (double) topLevelExplanationsHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, topLevelExplanationsHit3.get("description"));
        List<Map<String, Object>> normalizationExplanationHit3 = getListOfValues(topLevelExplanationsHit3, "details");
        assertEquals(2, normalizationExplanationHit3.size());

        Map<String, Object> hit1DetailsForHit3 = normalizationExplanationHit3.get(0);
        assertEquals(0.13f, (double) hit1DetailsForHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("weight(test-text-field-1:hello in 0) [PerFieldSimilarity], result of:", hit1DetailsForHit3.get("description"));
        assertEquals(1, getListOfValues(hit1DetailsForHit3, "details").size());

        Map<String, Object> explanationsHit3 = getListOfValues(hit1DetailsForHit3, "details").get(0);
        assertEquals(0.13f, (double) explanationsHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("score(freq=1.0), computed as boost * idf * tf from:", explanationsHit3.get("description"));
        assertEquals(2, getListOfValues(explanationsHit3, "details").size());

        Map<String, Object> explanationsHit3Details = getListOfValues(explanationsHit3, "details").get(0);
        assertEquals(0.287f, (double) explanationsHit3Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:", explanationsHit3Details.get("description"));
        assertEquals(2, getListOfValues(explanationsHit3Details, "details").size());

        Map<String, Object> hit1DetailsForHit3NoMatch = normalizationExplanationHit2.get(1);
        assertEquals(0.0f, (double) hit1DetailsForHit3NoMatch.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("No matching clauses", hit1DetailsForHit3NoMatch.get("description"));
        assertEquals(0, ((List) hit1DetailsForHit3NoMatch.get("details")).size());
    }

    @SneakyThrows
    public void testExplain_whenLargeNumberOfDocuments_thenSuccessful() {
        initializeIndexIfNotExist(TEST_LARGE_DOCS_INDEX_NAME);
        // create search pipeline with both normalization processor and explain response processor
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            false,
            true
        );

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(termQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_LARGE_DOCS_INDEX_NAME,
            hybridQueryBuilder,
            null,
            MAX_NUMBER_OF_DOCS_IN_LARGE_INDEX,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        assertNotNull(hitsNestedList);
        assertFalse(hitsNestedList.isEmpty());

        // Verify total hits
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertTrue((int) total.get("value") > 0);
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // Sanity checks for each hit's explanation
        for (Map<String, Object> hit : hitsNestedList) {
            // Verify score is positive
            double score = (double) hit.get("_score");
            assertTrue("Score should be positive", score > 0.0);

            // Basic explanation structure checks
            Map<String, Object> explanation = getValueByKey(hit, "_explanation");
            assertNotNull(explanation);
            assertEquals("arithmetic_mean combination of:", explanation.get("description"));
            Map<String, Object> hitDetailsForHit = getListOfValues(explanation, "details").get(0);
            assertTrue((double) hitDetailsForHit.get("value") > 0.0f);
            assertEquals("min_max normalization of:", hitDetailsForHit.get("description"));
            Map<String, Object> subQueryDetailsForHit = getListOfValues(hitDetailsForHit, "details").get(0);
            assertTrue((double) subQueryDetailsForHit.get("value") > 0.0f);
            assertFalse(subQueryDetailsForHit.get("description").toString().isEmpty());
            assertEquals(1, getListOfValues(subQueryDetailsForHit, "details").size());
        }
        // Verify scores are properly ordered
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> hit : hitsNestedList) {
            scores.add((Double) hit.get("_score"));
        }
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(i -> scores.get(i) < scores.get(i + 1)));
    }

    @SneakyThrows
    public void testSpecificQueryTypes_whenMultiMatchAndKnn_thenSuccessful() {
        initializeIndexIfNotExist(TEST_LARGE_DOCS_INDEX_NAME);
        // create search pipeline with both normalization processor and explain response processor
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            false,
            true
        );

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.multiMatchQuery(TEST_QUERY_TEXT3, TEST_TEXT_FIELD_NAME_1, TEST_TEXT_FIELD_NAME_2));
        hybridQueryBuilder.add(
            KNNQueryBuilder.builder().k(10).fieldName(TEST_KNN_VECTOR_FIELD_NAME_1).vector(TEST_VECTOR_SUPPLIER.get()).build()
        );

        Map<String, Object> searchResponseAsMap = search(
            TEST_LARGE_DOCS_INDEX_NAME,
            hybridQueryBuilder,
            null,
            MAX_NUMBER_OF_DOCS_IN_LARGE_INDEX,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        assertNotNull(hitsNestedList);
        assertFalse(hitsNestedList.isEmpty());

        // Verify total hits
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertTrue((int) total.get("value") > 0);
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // Sanity checks for each hit's explanation
        for (Map<String, Object> hit : hitsNestedList) {
            // Verify score is positive
            double score = (double) hit.get("_score");
            assertTrue("Score should be positive", score > 0.0);

            // Basic explanation structure checks
            Map<String, Object> explanation = getValueByKey(hit, "_explanation");
            assertNotNull(explanation);
            assertEquals("arithmetic_mean combination of:", explanation.get("description"));
            Map<String, Object> hitDetailsForHit = getListOfValues(explanation, "details").get(0);
            org.hamcrest.MatcherAssert.assertThat((double) hitDetailsForHit.get("value"), greaterThanOrEqualTo(0.0));
            assertEquals("min_max normalization of:", hitDetailsForHit.get("description"));
            Map<String, Object> subQueryDetailsForHit = getListOfValues(hitDetailsForHit, "details").get(0);
            org.hamcrest.MatcherAssert.assertThat((double) subQueryDetailsForHit.get("value"), greaterThanOrEqualTo(0.0));
            assertFalse(subQueryDetailsForHit.get("description").toString().isEmpty());
            assertNotNull(getListOfValues(subQueryDetailsForHit, "details"));
        }
        // Verify scores are properly ordered
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> hit : hitsNestedList) {
            scores.add((Double) hit.get("_score"));
        }
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(i -> scores.get(i) < scores.get(i + 1)));
    }

    @SneakyThrows
    public void testExplain_whenRRFProcessor_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        createRRFSearchPipeline(RRF_SEARCH_PIPELINE, Arrays.asList(), true);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        KNNQueryBuilder knnQueryBuilder = KNNQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .vector(createRandomVector(TEST_DIMENSION))
            .k(10)
            .build();
        hybridQueryBuilder.add(QueryBuilders.existsQuery(TEST_TEXT_FIELD_NAME_1));
        hybridQueryBuilder.add(knnQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", RRF_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );
        // Assert
        // basic sanity check for search hits
        assertEquals(4, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        float actualMaxScore = getMaxScore(searchResponseAsMap).get();
        assertTrue(actualMaxScore > 0);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(4, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain, hit 1
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> explanationForHit1 = getValueByKey(searchHit1, "_explanation");
        assertNotNull(explanationForHit1);
        assertEquals((double) searchHit1.get("_score"), (double) explanationForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        String expectedTopLevelDescription = "rrf combination of:";
        assertEquals(expectedTopLevelDescription, explanationForHit1.get("description"));
        List<Map<String, Object>> hit1Details = getListOfValues(explanationForHit1, "details");
        assertEquals(2, hit1Details.size());
        // two sub-queries meaning we do have two detail objects with separate query level details
        Map<String, Object> hit1DetailsForHit1 = hit1Details.get(0);
        assertTrue((double) hit1DetailsForHit1.get("value") > DELTA_FOR_SCORE_ASSERTION);
        assertEquals("rrf, rank_constant [60] normalization of:", hit1DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit1 = getListOfValues(hit1DetailsForHit1, "details").get(0);
        assertEquals("ConstantScore(FieldExistsQuery [field=test-text-field-1])", explanationsHit1.get("description"));
        assertTrue((double) explanationsHit1.get("value") > 0.5f);
        assertEquals(0, ((List) explanationsHit1.get("details")).size());

        Map<String, Object> hit1DetailsForHit2 = hit1Details.get(1);
        assertTrue((double) hit1DetailsForHit2.get("value") > 0.0f);
        assertEquals("rrf, rank_constant [60] normalization of:", hit1DetailsForHit2.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit2.get("details")).size());

        Map<String, Object> explanationsHit2 = getListOfValues(hit1DetailsForHit2, "details").get(0);
        assertEquals("within top 3 docs", explanationsHit2.get("description"));
        assertTrue((double) explanationsHit2.get("value") > 0.0f);
        assertEquals(0, ((List) explanationsHit2.get("details")).size());

        // hit 2
        Map<String, Object> searchHit2 = hitsNestedList.get(1);
        Map<String, Object> explanationForHit2 = getValueByKey(searchHit2, "_explanation");
        assertNotNull(explanationForHit2);
        assertEquals((double) searchHit2.get("_score"), (double) explanationForHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, explanationForHit2.get("description"));
        List<Map<String, Object>> hit2Details = getListOfValues(explanationForHit2, "details");
        assertEquals(2, hit2Details.size());

        Map<String, Object> hit2DetailsForHit1 = hit2Details.get(0);
        assertTrue((double) hit2DetailsForHit1.get("value") > DELTA_FOR_SCORE_ASSERTION);
        assertEquals("rrf, rank_constant [60] normalization of:", hit2DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit2DetailsForHit1.get("details")).size());

        Map<String, Object> hit2DetailsForHit2 = hit2Details.get(1);
        assertTrue((double) hit2DetailsForHit2.get("value") > DELTA_FOR_SCORE_ASSERTION);
        assertEquals("rrf, rank_constant [60] normalization of:", hit2DetailsForHit2.get("description"));
        assertEquals(1, ((List) hit2DetailsForHit2.get("details")).size());

        // hit 3
        Map<String, Object> searchHit3 = hitsNestedList.get(2);
        Map<String, Object> explanationForHit3 = getValueByKey(searchHit3, "_explanation");
        assertNotNull(explanationForHit3);
        assertEquals((double) searchHit3.get("_score"), (double) explanationForHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, explanationForHit3.get("description"));
        List<Map<String, Object>> hit3Details = getListOfValues(explanationForHit3, "details");
        assertEquals(1, hit3Details.size());

        Map<String, Object> hit3DetailsForHit1 = hit3Details.get(0);
        assertTrue((double) hit3DetailsForHit1.get("value") > .0f);
        assertEquals("rrf, rank_constant [60] normalization of:", hit3DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit3DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit3 = getListOfValues(hit3DetailsForHit1, "details").get(0);
        assertEquals("within top 3 docs", explanationsHit3.get("description"));
        assertEquals(0, getListOfValues(explanationsHit3, "details").size());
        assertTrue((double) explanationsHit3.get("value") > 0.0f);

        // hit 4
        Map<String, Object> searchHit4 = hitsNestedList.get(3);
        Map<String, Object> explanationForHit4 = getValueByKey(searchHit4, "_explanation");
        assertNotNull(explanationForHit4);
        assertEquals((double) searchHit4.get("_score"), (double) explanationForHit4.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, explanationForHit4.get("description"));
        List<Map<String, Object>> hit4Details = getListOfValues(explanationForHit4, "details");
        assertEquals(1, hit4Details.size());

        Map<String, Object> hit4DetailsForHit1 = hit4Details.get(0);
        assertTrue((double) hit4DetailsForHit1.get("value") > DELTA_FOR_SCORE_ASSERTION);
        assertEquals("rrf, rank_constant [60] normalization of:", hit4DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit4DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit4 = getListOfValues(hit4DetailsForHit1, "details").get(0);
        assertEquals("ConstantScore(FieldExistsQuery [field=test-text-field-1])", explanationsHit4.get("description"));
        assertEquals(0, getListOfValues(explanationsHit4, "details").size());
        assertTrue((double) explanationsHit4.get("value") > 0.0f);
    }

    @SneakyThrows
    public void testExplain_whenMinMaxNormalizationWithLowerBounds_thenSuccessful() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        // create search pipeline with both normalization processor and explain response processor
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(
                "lower_bounds",
                List.of(
                    Map.of("mode", "apply", "min_score", Float.toString(0.01f)),
                    Map.of("mode", "clip", "min_score", Float.toString(0.0f))
                )
            ),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            false,
            true
        );

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
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", "true")
        );
        // Assert
        // search hits
        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap1);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap1);
        assertNotNull(total.get("value"));
        assertEquals(3, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> topLevelExplanationsHit1 = getValueByKey(searchHit1, "_explanation");
        assertExplanation(topLevelExplanationsHit1, searchHit1, hitsNestedList, true);
    }

    @SneakyThrows
    public void testExplainWithSubQueryScoresEnabled_whenMultipleSubqueriesAndOneShard_thenNull() {
        initializeIndexIfNotExist(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME);
        // create search pipeline with both normalization processor and explain response processor
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            true,
            true
        );

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
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );
        // Assert
        // search hits
        assertEquals(3, getHitCount(searchResponseAsMap1));

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap1);
        // No fields added to the hits when explain is enabled
        for (Map<String, Object> hit : hitsNestedList) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            assertNull(fields);
        }
    }

    @SneakyThrows
    public void testExplainWithSubQueryScoresEnabled_whenMultipleSubqueriesAndMultipleShards_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_NAME);
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            NORMALIZATION_TECHNIQUE_L2,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f })),
            true,
            true
        );

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        KNNQueryBuilder knnQueryBuilder = KNNQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .vector(createRandomVector(TEST_DIMENSION))
            .k(10)
            .build();
        hybridQueryBuilder.add(QueryBuilders.existsQuery(TEST_TEXT_FIELD_NAME_1));
        hybridQueryBuilder.add(knnQueryBuilder);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );
        // Assert
        // basic sanity check for search hits
        assertEquals(4, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        float actualMaxScore = getMaxScore(searchResponseAsMap).get();
        assertTrue(actualMaxScore > 0);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(4, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain, hit 1
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        // No fields added to the hits when explain is enabled
        for (Map<String, Object> hit : hitsNestedList) {
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            assertNull(fields);
        }
    }

    private void assertExplanation(
        Map<String, Object> topLevelExplanationsHit1,
        Map<String, Object> searchHit1,
        List<Map<String, Object>> hitsNestedList,
        boolean withLowerBounds
    ) {
        assertNotNull(topLevelExplanationsHit1);
        assertEquals((double) searchHit1.get("_score"), (double) topLevelExplanationsHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        String expectedTopLevelDescription = "arithmetic_mean combination of:";
        assertEquals(expectedTopLevelDescription, topLevelExplanationsHit1.get("description"));
        List<Map<String, Object>> normalizationExplanationHit1 = getListOfValues(topLevelExplanationsHit1, "details");
        assertEquals(1, normalizationExplanationHit1.size());
        Map<String, Object> hit1DetailsForHit1 = normalizationExplanationHit1.get(0);
        assertEquals(1.0, hit1DetailsForHit1.get("value"));
        if (withLowerBounds) {
            assertEquals("min_max, lower bounds [(apply, 0.01), (clip, 0.0)] normalization of:", hit1DetailsForHit1.get("description"));
        } else {
            assertEquals("min_max normalization of:", hit1DetailsForHit1.get("description"));
        }
        assertEquals(1, ((List) hit1DetailsForHit1.get("details")).size());

        Map<String, Object> explanationsHit1 = getListOfValues(hit1DetailsForHit1, "details").get(0);
        assertEquals("sum of:", explanationsHit1.get("description"));
        assertEquals(0.343f, (double) explanationsHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(1, ((List) explanationsHit1.get("details")).size());

        // search hit 2
        Map<String, Object> searchHit2 = hitsNestedList.get(1);
        Map<String, Object> topLevelExplanationsHit2 = getValueByKey(searchHit2, "_explanation");
        assertNotNull(topLevelExplanationsHit2);
        assertEquals((double) searchHit2.get("_score"), (double) topLevelExplanationsHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, topLevelExplanationsHit2.get("description"));
        List<Map<String, Object>> normalizationExplanationHit2 = getListOfValues(topLevelExplanationsHit2, "details");
        assertEquals(1, normalizationExplanationHit2.size());

        Map<String, Object> hit1DetailsForHit2 = normalizationExplanationHit2.get(0);
        assertEquals(1.0, hit1DetailsForHit2.get("value"));
        if (withLowerBounds) {
            assertEquals("min_max, lower bounds [(apply, 0.01), (clip, 0.0)] normalization of:", hit1DetailsForHit2.get("description"));
        } else {
            assertEquals("min_max normalization of:", hit1DetailsForHit2.get("description"));
        }
        assertEquals(1, getListOfValues(hit1DetailsForHit2, "details").size());

        Map<String, Object> explanationsHit2 = getListOfValues(hit1DetailsForHit2, "details").get(0);
        assertEquals(0.13f, (double) explanationsHit2.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("weight(test-text-field-1:hello in 0) [PerFieldSimilarity], result of:", explanationsHit2.get("description"));
        assertEquals(1, getListOfValues(explanationsHit2, "details").size());

        Map<String, Object> explanationsHit2Details = getListOfValues(explanationsHit2, "details").get(0);
        assertEquals(0.13f, (double) explanationsHit2Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("score(freq=1.0), computed as boost * idf * tf from:", explanationsHit2Details.get("description"));
        assertEquals(2, getListOfValues(explanationsHit2Details, "details").size());

        // search hit 3
        Map<String, Object> searchHit3 = hitsNestedList.get(1);
        Map<String, Object> topLevelExplanationsHit3 = getValueByKey(searchHit3, "_explanation");
        assertNotNull(topLevelExplanationsHit3);
        assertEquals((double) searchHit2.get("_score"), (double) topLevelExplanationsHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);

        assertEquals(expectedTopLevelDescription, topLevelExplanationsHit3.get("description"));
        List<Map<String, Object>> normalizationExplanationHit3 = getListOfValues(topLevelExplanationsHit3, "details");
        assertEquals(1, normalizationExplanationHit3.size());

        Map<String, Object> hit1DetailsForHit3 = normalizationExplanationHit3.get(0);
        assertEquals(1.0, hit1DetailsForHit3.get("value"));
        if (withLowerBounds) {
            assertEquals("min_max, lower bounds [(apply, 0.01), (clip, 0.0)] normalization of:", hit1DetailsForHit3.get("description"));
        } else {
            assertEquals("min_max normalization of:", hit1DetailsForHit3.get("description"));
        }
        assertEquals(1, getListOfValues(hit1DetailsForHit3, "details").size());

        Map<String, Object> explanationsHit3 = getListOfValues(hit1DetailsForHit3, "details").get(0);
        assertEquals(0.13f, (double) explanationsHit3.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("weight(test-text-field-1:hello in 0) [PerFieldSimilarity], result of:", explanationsHit3.get("description"));
        assertEquals(1, getListOfValues(explanationsHit3, "details").size());

        Map<String, Object> explanationsHit3Details = getListOfValues(explanationsHit3, "details").get(0);
        assertEquals(0.13f, (double) explanationsHit3Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("score(freq=1.0), computed as boost * idf * tf from:", explanationsHit3Details.get("description"));
        assertEquals(2, getListOfValues(explanationsHit3Details, "details").size());
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
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
                    Map.of(TEST_NESTED_TYPE_FIELD_NAME_1, Map.of()),
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

        if (TEST_LARGE_DOCS_INDEX_NAME.equals(indexName) && !indexExists(TEST_LARGE_DOCS_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_LARGE_DOCS_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                )
            );

            // Index large number of documents
            for (int i = 0; i < MAX_NUMBER_OF_DOCS_IN_LARGE_INDEX; i++) {
                String docText;
                if (i % 5 == 0) {
                    docText = TEST_DOC_TEXT1;  // "Hello world"
                } else if (i % 7 == 0) {
                    docText = TEST_DOC_TEXT2;  // "Hi to this place"
                } else if (i % 11 == 0) {
                    docText = TEST_DOC_TEXT3;  // "We would like to welcome everyone"
                } else {
                    docText = String.format(Locale.ROOT, "Document %d with random content", i);
                }

                addKnnDoc(
                    TEST_LARGE_DOCS_INDEX_NAME,
                    String.valueOf(i),
                    List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                    List.of(
                        Floats.asList(createRandomVector(TEST_DIMENSION)).toArray(),
                        Floats.asList(createRandomVector(TEST_DIMENSION)).toArray()
                    ),
                    Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                    Collections.singletonList(docText)
                );
            }
            assertEquals(MAX_NUMBER_OF_DOCS_IN_LARGE_INDEX, getDocCount(TEST_LARGE_DOCS_INDEX_NAME));
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
}
