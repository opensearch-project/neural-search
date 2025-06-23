/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.lucene.search.join.ScoreMode;
import org.junit.Before;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.join.query.HasChildQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.search.sort.SortOrder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.getMaxScore;
import static org.opensearch.neuralsearch.util.TestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;
import static org.opensearch.neuralsearch.util.TestUtils.getValueByKey;

public class HybridQueryInnerHitsIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME = "test-hybrid-index-nested-field-single-shard";
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME =
        "test-hybrid-index-nested-field-multiple-shard";
    private static final String TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME = "test-hybrid-index-parent-child-field";
    private static final String TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME = "test-hybrid-index-nested-parent-child-field";

    private static final String TEST_NESTED_FIELD_NAME_1 = "user";
    private static final String TEST_USER_INNER_NAME_NESTED_FIELD = "name";
    private static final String TEST_USER_INNER_AGE_NESTED_FIELD = "age";
    private static final String TEST_NESTED_FIELD_NAME_2 = "location";
    private static final String TEST_LOCATION_INNER_STATE_NESTED_FIELD = "state";
    private static final String TEST_LOCATION_INNER_PLACE_NESTED_FIELD = "place";
    private static final String TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD = "my_join_field";
    private static final String TEST_PARENT_CHILD_TYPE_JOIN = "join";
    private static final String TEST_PARENT_CHILD_RELATION_FIELD_NAME_1 = "parent";
    private static final String TEST_PARENT_CHILD_RELATION_FIELD_NAME_2 = "child";
    private static final String TEST_PARENT_CHILD_TEXT_FIELD_NAME = "text";
    private static final String TEST_PARENT_CHILD_TEXT_FIELD_VALUE_1 = "This is a parent document";
    private static final String TEST_PARENT_CHILD_TEXT_FIELD_VALUE_2 = "This is a child document";
    private static final String TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME = "child";
    private static final String NORMALIZATION_SEARCH_PIPELINE = "normalization-search-pipeline";

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
    public void testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful() {
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
    }

    private void testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(String indexName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of(), false);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        nestedQueryBuilder1.innerHit(new InnerHitBuilder());
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Avg
        );
        nestedQueryBuilder2.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(nestedQueryBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            indexName,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE)
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(2, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );
        assertEquals(2, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(0).intValue());
        assertEquals(3, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("total").get(0).intValue());
        assertEquals(1, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(1).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("total").get(1).intValue());
    }

    public void testInnerHits_whenMultipleSubqueriesOnParentChildFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of(), false);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(
            "child",
            new MatchQueryBuilder("text", "child"),
            ScoreMode.Avg
        );
        hasChildQueryBuilder.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(hasChildQueryBuilder);
        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE)
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME)
        );
        assertEquals(1, innerHitCountPerFieldName.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME).get("total").get(0).intValue());
    }

    @SneakyThrows
    public void testInnerHits_whenMultipleSubqueriesOnNestedAndParentChildFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of(), false);
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);
        nestedQueryBuilder.innerHit(new InnerHitBuilder());
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder("child", nestedQueryBuilder, ScoreMode.Avg);
        hasChildQueryBuilder.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(hasChildQueryBuilder);
        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE)
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(1, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME)
        );
        assertEquals(1, innerHitCountPerFieldName.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME).get("total").get(0).intValue());
        Map<String, Object> childInnerHit = (Map<String, Object>) hitsNestedList.get(0);
        Map<String, Object> childHit = (Map<String, Object>) childInnerHit.get(TEST_PARENT_CHILD_INNER_HITS_FIELD_NAME);
        List<Object> childInnerHits = getInnerHitsFromSearchHits(childHit);
        Map<String, Map<String, ArrayList<Integer>>> childInnerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            childInnerHits,
            List.of(TEST_NESTED_FIELD_NAME_1)
        );
        assertEquals(1, childInnerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(0).intValue());
    }

    @SneakyThrows
    public void testInnerHits_withSortingAndPagination_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of(), false);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Avg);

        InnerHitBuilder innerHitBuilder = new InnerHitBuilder();
        innerHitBuilder.setFrom(1);
        nestedQueryBuilder1.innerHit(innerHitBuilder);
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Avg
        );
        InnerHitBuilder innerHitBuilder1 = new InnerHitBuilder();
        innerHitBuilder1.setSorts(createSortBuilders(Map.of("_doc", SortOrder.DESC), false));
        nestedQueryBuilder2.innerHit(innerHitBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE)
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(2, getHitCount(searchResponseAsMap));
        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );
        assertEquals(1, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("actual").get(0).intValue());
        assertEquals(3, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("actual").get(0).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("actual").get(1).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("actual").get(1).intValue());

        Map<String, ArrayList<List<Object>>> sortsPerField = getInnerHitsSortValueOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_FIELD_NAME_2)
        );

        ArrayList<List<Object>> locationSorts = sortsPerField.get(TEST_NESTED_FIELD_NAME_2);
        assertTrue(
            IntStream.range(0, locationSorts.size() - 1)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(locationSorts.get(i).get(0), locationSorts.get(i + 1).get(0)))
                .allMatch(pair -> ((Comparable<Object>) pair.getKey()).compareTo(pair.getValue()) > 0)
        );
    }

    @SneakyThrows
    public void testInnerHitsWithExplain_whenMultipleSubqueriesOnNestedFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        createSearchPipeline(
            NORMALIZATION_SEARCH_PIPELINE,
            DEFAULT_NORMALIZATION_METHOD,
            Map.of(),
            DEFAULT_COMBINATION_METHOD,
            Map.of(),
            true,
            false
        );
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("user", new MatchQueryBuilder("user.name", "John"), ScoreMode.Max);
        nestedQueryBuilder1.innerHit(new InnerHitBuilder());
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder(
            "location",
            new MatchQueryBuilder("location.state", "California"),
            ScoreMode.Max
        );
        nestedQueryBuilder2.innerHit(new InnerHitBuilder());
        hybridQueryBuilder.add(nestedQueryBuilder1);
        hybridQueryBuilder.add(nestedQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE, "explain", Boolean.TRUE.toString())
        );

        List<Object> nestedHitsList = getInnerHitsFromSearchHits(searchResponseAsMap);
        Map<String, ArrayList<Double>> scoreOfInnerHits = getInnerHitsScoresPerFieldList(
            nestedHitsList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );

        Map<String, Map<String, ArrayList<Integer>>> innerHitCountPerFieldName = getInnerHitsCountsOfNestedField(
            nestedHitsList,
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2)
        );
        // Assert
        // basic sanity check for search hits
        assertEquals(2, getHitCount(searchResponseAsMap));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        float actualMaxScore = getMaxScore(searchResponseAsMap).get();
        assertTrue(actualMaxScore > 0);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(2, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));

        // explain, hit 1
        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        Map<String, Object> searchHit1 = hitsNestedList.get(0);
        Map<String, Object> explanationForHit1 = getValueByKey(searchHit1, "_explanation");
        assertNotNull(explanationForHit1);
        assertEquals((double) searchHit1.get("_score"), (double) explanationForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);

        // top level explanation
        String expectedTopLevelDescription = "arithmetic_mean combination of:";
        assertEquals(expectedTopLevelDescription, explanationForHit1.get("description"));

        // Normalization explanation
        List<Map<String, Object>> hit1Details = getListOfValues(explanationForHit1, "details");
        assertEquals(1, hit1Details.size());
        Map<String, Object> hit1DetailsForHit1 = hit1Details.get(0);
        assertEquals(0.001f, (double) hit1DetailsForHit1.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("min_max normalization of:", hit1DetailsForHit1.get("description"));
        assertEquals(1, ((List) hit1DetailsForHit1.get("details")).size());

        // Combination explanation
        List<Map<String, Object>> hit1CombinationDetails = getListOfValues(hit1DetailsForHit1, "details");
        assertEquals(1, hit1CombinationDetails.size());
        Map<String, Object> internalCombinationDetailsForHit1 = hit1CombinationDetails.get(0);
        assertEquals(
            scoreOfInnerHits.get(TEST_NESTED_FIELD_NAME_1).get(0),
            (double) internalCombinationDetailsForHit1.get("value"),
            DELTA_FOR_SCORE_ASSERTION
        );
        assertEquals("combined score of:", internalCombinationDetailsForHit1.get("description"));
        assertEquals(2, ((List) internalCombinationDetailsForHit1.get("details")).size());

        // InnerHitsChild explanation
        List<Map<String, Object>> hit1ChildDetails = getListOfValues(internalCombinationDetailsForHit1, "details");
        assertEquals(2, hit1ChildDetails.size());

        Map<String, Object> child1Details = hit1ChildDetails.get(0);
        assertEquals(scoreOfInnerHits.get(TEST_NESTED_FIELD_NAME_1).get(0), (double) child1Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(
            "Score based on "
                + innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_1).get("total").get(0)
                + " child docs in range from 0 to 11, using score mode Max",
            child1Details.get("description")
        );
        assertEquals(1, ((List) child1Details.get("details")).size());

        Map<String, Object> child2Details = hit1ChildDetails.get(1);
        assertEquals(scoreOfInnerHits.get(TEST_NESTED_FIELD_NAME_2).get(0), (double) child2Details.get("value"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(
            "Score based on "
                + innerHitCountPerFieldName.get(TEST_NESTED_FIELD_NAME_2).get("total").get(0)
                + " child docs in range from 0 to 11, using score mode Max",
            child2Details.get("description")
        );
        assertEquals(1, ((List) child2Details.get("details")).size());
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        Map<String, Map<String, String>> nestedFields = new HashMap<>();
        nestedFields.put(
            TEST_NESTED_FIELD_NAME_1,
            Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "text", TEST_USER_INNER_AGE_NESTED_FIELD, "integer")
        );
        nestedFields.put(
            TEST_NESTED_FIELD_NAME_2,
            Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "text", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "text")
        );
        if ((TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME))) {
            createIndexWithConfiguration(indexName, buildIndexConfiguration(Collections.emptyList(), nestedFields, 1), "");
            addNestedDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        }

        if ((TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME))) {
            createIndexWithConfiguration(indexName, buildIndexConfiguration(Collections.emptyList(), nestedFields, 3), "");
            addNestedDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    List.of(List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD, TEST_PARENT_CHILD_TYPE_JOIN)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    3
                ),
                ""
            );
            addParentChildDocsToIndex(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    nestedFields,
                    List.of(List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD, TEST_PARENT_CHILD_TYPE_JOIN)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    3
                ),
                ""
            );
            addNestedAndParentChildDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME);
        }
    }

    private void addNestedDocsToIndex(final String testMultiDocIndexName) {
        addKnnDoc(
            testMultiDocIndexName,
            "1",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Alder", TEST_USER_INNER_AGE_NESTED_FIELD, "50"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John snow", TEST_USER_INNER_AGE_NESTED_FIELD, "23"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Harry Styles", TEST_USER_INNER_AGE_NESTED_FIELD, "20"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Michael Jackson", TEST_USER_INNER_AGE_NESTED_FIELD, "67"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Marry Jane", TEST_USER_INNER_AGE_NESTED_FIELD, "90"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Tom Hanks", TEST_USER_INNER_AGE_NESTED_FIELD, "5")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "San Diego"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "North Carolina", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Charlotte"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Los Angeles"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "New York", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "New York"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Oregon", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Portland"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Fresno")
                )
            )
        );
        addKnnDoc(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Carry", TEST_USER_INNER_AGE_NESTED_FIELD, "34"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Dwayne Rock", TEST_USER_INNER_AGE_NESTED_FIELD, "28"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Leonardo Di Caprio", TEST_USER_INNER_AGE_NESTED_FIELD, "22"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Jack Sparrow", TEST_USER_INNER_AGE_NESTED_FIELD, "47"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Will Smith", TEST_USER_INNER_AGE_NESTED_FIELD, "45"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Brad Pitt", TEST_USER_INNER_AGE_NESTED_FIELD, "39")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Illinois", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Chicago"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Texas", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Dallas"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Arizona", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Phoenix"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Florida", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Orlando"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Virginia", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Redmond"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Washington", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Seattle")
                )
            )
        );

        assertEquals(2, getDocCount(testMultiDocIndexName));
    }

    private void addParentChildDocsToIndex(final String testMultiDocIndexName) {
        indexTheDocument(
            testMultiDocIndexName,
            "1",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_1),
            Collections.emptyList(),
            Map.of(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_1),
            null
        );

        indexTheDocument(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_2),
            Collections.emptyList(),
            Map.of(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_2),
            "1"
        );
    }

    private void addNestedAndParentChildDocsToIndex(final String testMultiDocIndexName) {
        indexTheDocument(
            testMultiDocIndexName,
            "1",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_1),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Alder", TEST_USER_INNER_AGE_NESTED_FIELD, "50"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John snow", TEST_USER_INNER_AGE_NESTED_FIELD, "23"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Harry Styles", TEST_USER_INNER_AGE_NESTED_FIELD, "20"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Michael Jackson", TEST_USER_INNER_AGE_NESTED_FIELD, "67"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Marry Jane", TEST_USER_INNER_AGE_NESTED_FIELD, "90"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Tom Hanks", TEST_USER_INNER_AGE_NESTED_FIELD, "5")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "San Diego"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "North Carolina", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Charlotte"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Los Angeles"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "New York", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "New York"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Oregon", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Portland"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "California", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Fresno")
                )
            ),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_1),
            null
        );

        indexTheDocument(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_NAME),
            List.of(TEST_PARENT_CHILD_TEXT_FIELD_VALUE_2),
            List.of(TEST_NESTED_FIELD_NAME_1, TEST_NESTED_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "John Carry", TEST_USER_INNER_AGE_NESTED_FIELD, "34"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Dwayne Rock", TEST_USER_INNER_AGE_NESTED_FIELD, "28"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Leonardo Di Caprio", TEST_USER_INNER_AGE_NESTED_FIELD, "22"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Jack Sparrow", TEST_USER_INNER_AGE_NESTED_FIELD, "47"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Will Smith", TEST_USER_INNER_AGE_NESTED_FIELD, "45"),
                    Map.of(TEST_USER_INNER_NAME_NESTED_FIELD, "Brad Pitt", TEST_USER_INNER_AGE_NESTED_FIELD, "39")
                ),
                TEST_NESTED_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Illinois", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Chicago"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Texas", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Dallas"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Arizona", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Phoenix"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Florida", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Orlando"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Virginia", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Redmond"),
                    Map.of(TEST_LOCATION_INNER_STATE_NESTED_FIELD, "Washington", TEST_LOCATION_INNER_PLACE_NESTED_FIELD, "Seattle")
                )
            ),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_MY_JOIN_FIELD_FIELD),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_2),
            "1"
        );
    }

    @SneakyThrows
    public void testInnerHits_whenMultipleSubqueriesOnNestedFields_statsEnabled_thenSuccessful() {
        enableStats();

        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.HYBRID_QUERY_REQUESTS));
        assertEquals(2, getNestedValue(allNodesStats, EventStatName.HYBRID_QUERY_INNER_HITS_REQUESTS));

        disableStats();
    }
}
