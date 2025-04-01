/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.aggregation;

import lombok.SneakyThrows;
import org.junit.BeforeClass;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getNestedHits;
import static org.opensearch.neuralsearch.util.AggregationsTestUtils.getTotalHits;

public class BaseAggregationsWithHybridQueryIT extends BaseNeuralSearchIT {
    protected static final String TEST_DOC_TEXT1 = "Hello world";
    protected static final String TEST_DOC_TEXT2 = "Hi to this place";
    protected static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    protected static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    protected static final String TEST_DOC_TEXT5 = "People keep telling me orange but I still prefer pink";
    protected static final String TEST_DOC_TEXT6 = "She traveled because it cost the same as therapy and was a lot more enjoyable";
    protected static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    protected static final String TEST_QUERY_TEXT3 = "hello";
    protected static final String TEST_QUERY_TEXT4 = "cost";
    protected static final String TEST_QUERY_TEXT5 = "welcome";
    protected static final String NESTED_TYPE_FIELD_USER = "user";
    protected static final String NESTED_FIELD_FIRSTNAME = "firstname";
    protected static final String NESTED_FIELD_LASTNAME = "lastname";
    protected static final String NESTED_FIELD_FIRSTNAME_JOHN = "john";
    protected static final String NESTED_FIELD_LASTNAME_BLACK = "black";
    protected static final String NESTED_FIELD_FIRSTNAME_FRODO = "frodo";
    protected static final String NESTED_FIELD_LASTNAME_BAGGINS = "baggins";
    protected static final String NESTED_FIELD_FIRSTNAME_MOHAMMED = "mohammed";
    protected static final String NESTED_FIELD_LASTNAME_EZAB = "ezab";
    protected static final String NESTED_FIELD_FIRSTNAME_SUN = "sun";
    protected static final String NESTED_FIELD_LASTNAME_WUKONG = "wukong";
    protected static final String NESTED_FIELD_FIRSTNAME_VASILISA = "vasilisa";
    protected static final String NESTED_FIELD_LASTNAME_WISE = "the wise";
    protected static final String INTEGER_FIELD_DOCINDEX = "doc_index";
    protected static final int INTEGER_FIELD_DOCINDEX_1234 = 1234;
    protected static final int INTEGER_FIELD_DOCINDEX_2345 = 2345;
    protected static final int INTEGER_FIELD_DOCINDEX_3456 = 3456;
    protected static final int INTEGER_FIELD_DOCINDEX_4567 = 4567;
    protected static final String KEYWORD_FIELD_DOCKEYWORD = "doc_keyword";
    protected static final String KEYWORD_FIELD_DOCKEYWORD_WORKABLE = "workable";
    protected static final String KEYWORD_FIELD_DOCKEYWORD_ANGRY = "angry";
    protected static final String KEYWORD_FIELD_DOCKEYWORD_LIKABLE = "likeable";
    protected static final String KEYWORD_FIELD_DOCKEYWORD_ENTIRE = "entire";
    protected static final String DATE_FIELD = "doc_date";
    protected static final String DATE_FIELD_01031995 = "01/03/1995";
    protected static final String DATE_FIELD_05022015 = "05/02/2015";
    protected static final String DATE_FIELD_07232007 = "07/23/2007";
    protected static final String DATE_FIELD_08212012 = "08/21/2012";
    protected static final String INTEGER_FIELD_PRICE = "doc_price";
    protected static final int INTEGER_FIELD_PRICE_130 = 130;
    protected static final int INTEGER_FIELD_PRICE_100 = 100;
    protected static final int INTEGER_FIELD_PRICE_200 = 200;
    protected static final int INTEGER_FIELD_PRICE_25 = 25;
    protected static final int INTEGER_FIELD_PRICE_30 = 30;
    protected static final int INTEGER_FIELD_PRICE_350 = 350;
    protected static final String BUCKET_AGG_DOC_COUNT_FIELD = "doc_count";
    protected static final String BUCKETS_AGGREGATION_NAME_1 = "date_buckets_1";
    protected static final String BUCKETS_AGGREGATION_NAME_2 = "date_buckets_2";
    protected static final String BUCKETS_AGGREGATION_NAME_3 = "date_buckets_3";
    protected static final String BUCKETS_AGGREGATION_NAME_4 = "date_buckets_4";
    protected static final String KEY = "key";
    protected static final String BUCKET_AGG_KEY_AS_STRING = "key_as_string";
    protected static final String SUM_AGGREGATION_NAME = "sum_aggs";
    protected static final String SUM_AGGREGATION_NAME_2 = "sum_aggs_2";
    protected static final String AVG_AGGREGATION_NAME = "avg_field";
    protected static final String GENERIC_AGGREGATION_NAME = "my_aggregation";
    protected static final String DATE_AGGREGATION_NAME = "date_aggregation";

    @BeforeClass
    @SneakyThrows
    public static void setUpCluster() {
        // we need new instance because we're calling non-static methods from static method.
        // main purpose is to minimize network calls, initialization is only needed once
        BaseAggregationsWithHybridQueryIT instance = new BaseAggregationsWithHybridQueryIT();
        instance.initClient();
        instance.updateClusterSettings();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected void prepareResources(String indexName, String pipelineName) {
        initializeIndexIfNotExist(indexName);
        createSearchPipelineWithResultsPostProcessor(pipelineName);
    }

    @SneakyThrows
    protected void initializeIndexIfNotExist(String indexName) {
        if (!indexExists(indexName)) {
            createIndexWithConfiguration(
                indexName,
                buildIndexConfiguration(
                    List.of(),
                    Map.of(NESTED_TYPE_FIELD_USER, Map.of(NESTED_FIELD_FIRSTNAME, "keyword", NESTED_FIELD_LASTNAME, "keyword")),
                    List.of(INTEGER_FIELD_DOCINDEX),
                    List.of(KEYWORD_FIELD_DOCKEYWORD),
                    List.of(DATE_FIELD),
                    3
                ),
                ""
            );

            indexTheDocument(
                indexName,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(NESTED_TYPE_FIELD_USER),
                Map.of(
                    NESTED_TYPE_FIELD_USER,
                    List.of(Map.of(NESTED_FIELD_FIRSTNAME, NESTED_FIELD_FIRSTNAME_JOHN, NESTED_FIELD_LASTNAME, NESTED_FIELD_LASTNAME_BLACK))
                ),
                List.of(INTEGER_FIELD_DOCINDEX, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_DOCINDEX_1234, INTEGER_FIELD_PRICE_130),
                List.of(KEYWORD_FIELD_DOCKEYWORD),
                List.of(KEYWORD_FIELD_DOCKEYWORD_WORKABLE),
                List.of(DATE_FIELD),
                List.of(DATE_FIELD_01031995),
                List.of(),
                List.of(),
                null
            );
            indexTheDocument(
                indexName,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(NESTED_TYPE_FIELD_USER),
                Map.of(
                    NESTED_TYPE_FIELD_USER,
                    List.of(
                        Map.of(NESTED_FIELD_FIRSTNAME, NESTED_FIELD_FIRSTNAME_FRODO, NESTED_FIELD_LASTNAME, NESTED_FIELD_LASTNAME_BAGGINS)
                    )
                ),
                List.of(INTEGER_FIELD_DOCINDEX, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_DOCINDEX_2345, INTEGER_FIELD_PRICE_100),
                List.of(),
                List.of(),
                List.of(DATE_FIELD),
                List.of(DATE_FIELD_05022015),
                List.of(),
                List.of(),
                null
            );
            indexTheDocument(
                indexName,
                "3",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2),
                List.of(NESTED_TYPE_FIELD_USER),
                Map.of(
                    NESTED_TYPE_FIELD_USER,
                    List.of(
                        Map.of(NESTED_FIELD_FIRSTNAME, NESTED_FIELD_FIRSTNAME_MOHAMMED, NESTED_FIELD_LASTNAME, NESTED_FIELD_LASTNAME_EZAB)
                    )
                ),
                List.of(INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_PRICE_200),
                List.of(KEYWORD_FIELD_DOCKEYWORD),
                List.of(KEYWORD_FIELD_DOCKEYWORD_ANGRY),
                List.of(DATE_FIELD),
                List.of(DATE_FIELD_07232007),
                List.of(),
                List.of(),
                null
            );
            indexTheDocument(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4),
                List.of(NESTED_TYPE_FIELD_USER),
                Map.of(
                    NESTED_TYPE_FIELD_USER,
                    List.of(Map.of(NESTED_FIELD_FIRSTNAME, NESTED_FIELD_FIRSTNAME_SUN, NESTED_FIELD_LASTNAME, NESTED_FIELD_LASTNAME_WUKONG))
                ),
                List.of(INTEGER_FIELD_DOCINDEX, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_DOCINDEX_3456, INTEGER_FIELD_PRICE_25),
                List.of(KEYWORD_FIELD_DOCKEYWORD),
                List.of(KEYWORD_FIELD_DOCKEYWORD_LIKABLE),
                List.of(DATE_FIELD),
                List.of(DATE_FIELD_05022015),
                List.of(),
                List.of(),
                null
            );
            indexTheDocument(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_DOCINDEX, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_DOCINDEX_3456, INTEGER_FIELD_PRICE_30),
                List.of(KEYWORD_FIELD_DOCKEYWORD),
                List.of(KEYWORD_FIELD_DOCKEYWORD_ENTIRE),
                List.of(DATE_FIELD),
                List.of(DATE_FIELD_08212012),
                List.of(),
                List.of(),
                null
            );
            indexTheDocument(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT6),
                List.of(NESTED_TYPE_FIELD_USER),
                Map.of(
                    NESTED_TYPE_FIELD_USER,
                    List.of(
                        Map.of(NESTED_FIELD_FIRSTNAME, NESTED_FIELD_FIRSTNAME_VASILISA, NESTED_FIELD_LASTNAME, NESTED_FIELD_LASTNAME_WISE)
                    )
                ),
                List.of(INTEGER_FIELD_DOCINDEX, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_DOCINDEX_4567, INTEGER_FIELD_PRICE_350),
                List.of(KEYWORD_FIELD_DOCKEYWORD),
                List.of(KEYWORD_FIELD_DOCKEYWORD_ENTIRE),
                List.of(DATE_FIELD),
                List.of(DATE_FIELD_08212012),
                List.of(),
                List.of(),
                null
            );
        }
    }

    protected void assertHitResultsFromQuery(int expected, Map<String, Object> searchResponseAsMap) {
        assertEquals(expected, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap);
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

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(expected, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }
}
