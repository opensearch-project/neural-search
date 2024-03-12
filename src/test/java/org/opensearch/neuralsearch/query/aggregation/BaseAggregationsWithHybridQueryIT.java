/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.aggregation;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.opensearch.neuralsearch.TestUtils.RELATION_EQUAL_TO;
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
    protected static final String TEST_NESTED_TYPE_FIELD_NAME_1 = "user";
    protected static final String NESTED_FIELD_1 = "firstname";
    protected static final String NESTED_FIELD_2 = "lastname";
    protected static final String NESTED_FIELD_1_VALUE_1 = "john";
    protected static final String NESTED_FIELD_2_VALUE_1 = "black";
    protected static final String NESTED_FIELD_1_VALUE_2 = "frodo";
    protected static final String NESTED_FIELD_2_VALUE_2 = "baggins";
    protected static final String NESTED_FIELD_1_VALUE_3 = "mohammed";
    protected static final String NESTED_FIELD_2_VALUE_3 = "ezab";
    protected static final String NESTED_FIELD_1_VALUE_4 = "sun";
    protected static final String NESTED_FIELD_2_VALUE_4 = "wukong";
    protected static final String NESTED_FIELD_1_VALUE_5 = "vasilisa";
    protected static final String NESTED_FIELD_2_VALUE_5 = "the wise";
    protected static final String INTEGER_FIELD_1 = "doc_index";
    protected static final int INTEGER_FIELD_1_VALUE = 1234;
    protected static final int INTEGER_FIELD_2_VALUE = 2345;
    protected static final int INTEGER_FIELD_3_VALUE = 3456;
    protected static final int INTEGER_FIELD_4_VALUE = 4567;
    protected static final String KEYWORD_FIELD_1 = "doc_keyword";
    protected static final String KEYWORD_FIELD_1_VALUE = "workable";
    protected static final String KEYWORD_FIELD_2_VALUE = "angry";
    protected static final String KEYWORD_FIELD_3_VALUE = "likeable";
    protected static final String KEYWORD_FIELD_4_VALUE = "entire";
    protected static final String DATE_FIELD_1 = "doc_date";
    protected static final String DATE_FIELD_1_VALUE = "01/03/1995";
    private static final String DATE_FIELD_2_VALUE = "05/02/2015";
    protected static final String DATE_FIELD_3_VALUE = "07/23/2007";
    protected static final String DATE_FIELD_4_VALUE = "08/21/2012";
    protected static final String INTEGER_FIELD_PRICE = "doc_price";
    protected static final int INTEGER_FIELD_PRICE_1_VALUE = 130;
    protected static final int INTEGER_FIELD_PRICE_2_VALUE = 100;
    protected static final int INTEGER_FIELD_PRICE_3_VALUE = 200;
    protected static final int INTEGER_FIELD_PRICE_4_VALUE = 25;
    protected static final int INTEGER_FIELD_PRICE_5_VALUE = 30;
    protected static final int INTEGER_FIELD_PRICE_6_VALUE = 350;
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
    protected static final String CLUSTER_SETTING_CONCURRENT_SEGMENT_SEARCH = "search.concurrent_segment_search.enabled";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    @Override
    public boolean isUpdateClusterSettings() {
        return false;
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
                    List.of(TEST_NESTED_TYPE_FIELD_NAME_1, NESTED_FIELD_1, NESTED_FIELD_2),
                    List.of(INTEGER_FIELD_1),
                    List.of(KEYWORD_FIELD_1),
                    List.of(DATE_FIELD_1),
                    3
                ),
                ""
            );

            addKnnDoc(
                indexName,
                "1",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_1, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_1)),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_1_VALUE, INTEGER_FIELD_PRICE_1_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_1_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_1_VALUE)
            );
            addKnnDoc(
                indexName,
                "2",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_2, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_2)),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_2_VALUE, INTEGER_FIELD_PRICE_2_VALUE),
                List.of(),
                List.of(),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_2_VALUE)
            );
            addKnnDoc(
                indexName,
                "3",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_3, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_3)),
                List.of(INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_PRICE_3_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_2_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_3_VALUE)
            );
            addKnnDoc(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_4, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_4)),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_3_VALUE, INTEGER_FIELD_PRICE_4_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_3_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_2_VALUE)
            );
            addKnnDoc(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5),
                List.of(),
                List.of(),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_3_VALUE, INTEGER_FIELD_PRICE_5_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_4_VALUE)
            );
            addKnnDoc(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT6),
                List.of(TEST_NESTED_TYPE_FIELD_NAME_1),
                List.of(Map.of(NESTED_FIELD_1, NESTED_FIELD_1_VALUE_5, NESTED_FIELD_2, NESTED_FIELD_2_VALUE_5)),
                List.of(INTEGER_FIELD_1, INTEGER_FIELD_PRICE),
                List.of(INTEGER_FIELD_4_VALUE, INTEGER_FIELD_PRICE_6_VALUE),
                List.of(KEYWORD_FIELD_1),
                List.of(KEYWORD_FIELD_4_VALUE),
                List.of(DATE_FIELD_1),
                List.of(DATE_FIELD_4_VALUE)
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
