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
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;

public class HybridQueryInnerHitsIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME = "test-hybrid-index-nested-field-single-shard";
    private static final String TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME =
        "test-hybrid-index-nested-field-multiple-shard";
    private static final String TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME = "test-hybrid-index-parent-child-field";
    private static final String TEST_MULTI_DOC_WITH_NESTED_PARENT_CHILD_INDEX_NAME = "test-hybrid-index-nested-parent-child-field";

    private static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    private static final String TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1 = "name";
    private static final String TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2 = "age";
    private static final String TEST_NESTED_TYPE_FIELD_NAME_2 = "location";
    private static final String TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1 = "state";
    private static final String TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2 = "place";
    private static final String TEST_PARENT_CHILD_FIELD_NAME_1 = "my_join_field";
    private static final String TEST_PARENT_CHILD_TYPE_FIELD_NAME_1 = "join";
    private static final String TEST_PARENT_CHILD_RELATION_FIELD_NAME_1 = "parent";
    private static final String TEST_PARENT_CHILD_RELATION_FIELD_NAME_2 = "child";
    private static final String NORMALIZATION_SEARCH_PIPELINE = "normalization-search-pipeline";

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
    public void testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful() {
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
    }

    @SneakyThrows
    private void testInnerHits_whenMultipleSubqueriesOnNestedFields_thenSuccessful(String indexName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
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
        Map<String, ArrayList<Integer>> innerHitCountPerFieldName = getInnerHitsCountOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_TYPE_FIELD_NAME_1, TEST_NESTED_TYPE_FIELD_NAME_2)
        );
        assertEquals(2, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_1).get(0).intValue());
        assertEquals(3, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_2).get(0).intValue());
        assertEquals(1, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_1).get(1).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_2).get(1).intValue());
    }

    @SneakyThrows
    public void testInnerHits_whenMultipleSubqueriesOnParentChildFields_thenSuccessful() {
        initializeIndexIfNotExist(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME);
        createSearchPipeline(NORMALIZATION_SEARCH_PIPELINE, DEFAULT_NORMALIZATION_METHOD, DEFAULT_COMBINATION_METHOD, Map.of());
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();

        Map<String, Object> searchResponseAsMap = search(
            TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of("search_pipeline", NORMALIZATION_SEARCH_PIPELINE)
        );

        List<Object> hitsNestedList = getInnerHitsFromSearchHits(searchResponseAsMap);
        assertEquals(2, getHitCount(searchResponseAsMap));
        Map<String, ArrayList<Integer>> innerHitCountPerFieldName = getInnerHitsCountOfNestedField(
            hitsNestedList,
            List.of(TEST_NESTED_TYPE_FIELD_NAME_1, TEST_NESTED_TYPE_FIELD_NAME_2)
        );
        assertEquals(2, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_1).get(0).intValue());
        assertEquals(3, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_2).get(0).intValue());
        assertEquals(1, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_1).get(1).intValue());
        assertEquals(0, innerHitCountPerFieldName.get(TEST_NESTED_TYPE_FIELD_NAME_2).get(1).intValue());
    }

    @SneakyThrows
    public void testInnerHits_whenMultipleSubqueriesAndMultipleShards_thenSuccessful() {}

    @SneakyThrows
    public void testInnerHitsSort_whenMultipleSubqueriesAndMultipleShards_thenSuccessful() {}

    @SneakyThrows
    public void testPaginationWithInnerHits_whenMultipleSubqueriesAndMultipleShards_thenSuccessful() {

    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {

        if ((TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME))) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    List.of(
                        List.of(
                            TEST_NESTED_TYPE_FIELD_NAME_1,
                            TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1,
                            TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2
                        ),
                        List.of(
                            TEST_NESTED_TYPE_FIELD_NAME_2,
                            TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                            TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2
                        )
                    ),
                    1
                ),
                ""
            );
            addNestedDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_SINGLE_SHARD_INDEX_NAME);
        }

        if ((TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME.equals(indexName)
            && !indexExists(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME))) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    List.of(
                        List.of(
                            TEST_NESTED_TYPE_FIELD_NAME_1,
                            TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1,
                            TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2
                        ),
                        List.of(
                            TEST_NESTED_TYPE_FIELD_NAME_2,
                            TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                            TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2
                        )
                    ),
                    3
                ),
                ""
            );
            addNestedDocsToIndex(TEST_MULTI_DOC_WITH_NESTED_FIELDS_MULTIPLE_SHARD_INDEX_NAME);
        }

        if (TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_DOC_WITH_PARENT_CHILD_INDEX_NAME)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    List.of(List.of(TEST_PARENT_CHILD_TYPE_FIELD_NAME_1, TEST_PARENT_CHILD_TYPE_FIELD_NAME_1)),
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
                    List.of(
                        List.of(
                            TEST_NESTED_TYPE_FIELD_NAME_1,
                            TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1,
                            TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2
                        ),
                        List.of(
                            TEST_NESTED_TYPE_FIELD_NAME_2,
                            TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                            TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2
                        )
                    ),
                    List.of(List.of(TEST_PARENT_CHILD_TYPE_FIELD_NAME_1, TEST_PARENT_CHILD_TYPE_FIELD_NAME_1)),
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
            List.of(TEST_NESTED_TYPE_FIELD_NAME_1, TEST_NESTED_TYPE_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_TYPE_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "John Alder", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "50"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "John snow", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "23"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Harry Styles", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "20"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Michael Jackson", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "67"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Marry Jane", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "90"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Tom Hanks", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "5")
                ),
                TEST_NESTED_TYPE_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "California",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "San Diego"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "North Carolina",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Charlotte"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "California",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Los Angeles"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "New York",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "New York"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Oregon",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Portland"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "California",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Fresno"
                    )
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
            List.of(TEST_NESTED_TYPE_FIELD_NAME_1, TEST_NESTED_TYPE_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_TYPE_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "John Carry", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "34"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Dwayne Rock", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "28"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Leonardo Di Caprio", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "22"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Jack Sparrow", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "47"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Will Smith", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "45"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Brad Pitt", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "39")
                ),
                TEST_NESTED_TYPE_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Illinois",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Chicago"
                    ),
                    Map.of(TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1, "Texas", TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2, "Dallas"),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Arizona",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Phoenix"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Florida",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Orlando"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Virginia",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Redmond"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Washington",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Seattle"
                    )
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
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Map.of(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_FIELD_NAME_1),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_1),
            null
        );

        indexTheDocument(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Map.of(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_FIELD_NAME_1),
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
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_NESTED_TYPE_FIELD_NAME_1, TEST_NESTED_TYPE_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_TYPE_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "John Alder", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "50"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "John snow", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "23"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Harry Styles", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "20"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Michael Jackson", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "67"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Marry Jane", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "90"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Tom Hanks", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "5")
                ),
                TEST_NESTED_TYPE_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "California",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "San Diego"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "North Carolina",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Charlotte"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "California",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Los Angeles"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "New York",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "New York"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Oregon",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Portland"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "California",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Fresno"
                    )
                )
            ),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_FIELD_NAME_1),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_1),
            null
        );

        indexTheDocument(
            testMultiDocIndexName,
            "2",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_NESTED_TYPE_FIELD_NAME_1, TEST_NESTED_TYPE_FIELD_NAME_2),
            Map.of(
                TEST_NESTED_TYPE_FIELD_NAME_1,
                Arrays.asList(
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "John Carry", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "34"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Dwayne Rock", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "28"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Leonardo Di Caprio", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "22"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Jack Sparrow", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "47"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Will Smith", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "45"),
                    Map.of(TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_1, "Brad Pitt", TEST_USER_INNER_NESTED_TYPE_FIELD_NAME_2, "39")
                ),
                TEST_NESTED_TYPE_FIELD_NAME_2,
                Arrays.asList(
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Illinois",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Chicago"
                    ),
                    Map.of(TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1, "Texas", TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2, "Dallas"),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Arizona",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Phoenix"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Florida",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Orlando"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Virginia",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Redmond"
                    ),
                    Map.of(
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_1,
                        "Washington",
                        TEST_LOCATION_INNER_NESTED_TYPE_FIELD_NAME_2,
                        "Seattle"
                    )
                )
            ),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(TEST_PARENT_CHILD_FIELD_NAME_1),
            List.of(TEST_PARENT_CHILD_RELATION_FIELD_NAME_2),
            "1"
        );
    }
}
