/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;
import org.junit.BeforeClass;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

public class HybridQueryFilterIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS =
        "test-hybrid-post-filter-multi-doc-index-multiple-shards";
    private static final String TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD =
        "test-hybrid-post-filter-multi-doc-index-single-shard";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-post-filter-pipeline";
    private static final String INTEGER_FIELD_1_STOCK = "stock";
    private static final String TEXT_FIELD_1_NAME = "name";
    private static final String KEYWORD_FIELD_2_CATEGORY = "category";
    private static final String TEXT_FIELD_VALUE_1_DUNES = "Dunes part 1";
    private static final String TEXT_FIELD_VALUE_2_DUNES = "Dunes part 2";
    private static final String TEXT_FIELD_VALUE_3_MI_1 = "Mission Impossible 1";
    private static final String TEXT_FIELD_VALUE_4_MI_2 = "Mission Impossible 2";
    private static final String TEXT_FIELD_VALUE_5_TERMINAL = "The Terminal";
    private static final String TEXT_FIELD_VALUE_6_AVENGERS = "Avengers";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final int INTEGER_FIELD_STOCK_1_25 = 25;
    private static final int INTEGER_FIELD_STOCK_2_22 = 22;
    private static final int INTEGER_FIELD_STOCK_3_256 = 256;
    private static final int INTEGER_FIELD_STOCK_4_25 = 25;
    private static final int INTEGER_FIELD_STOCK_5_20 = 20;
    private static final String KEYWORD_FIELD_CATEGORY_1_DRAMA = "Drama";
    private static final String KEYWORD_FIELD_CATEGORY_2_ACTION = "Action";
    private static final String KEYWORD_FIELD_CATEGORY_3_SCI_FI = "Sci-fi";
    private static final int SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER = 1;
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);

    @BeforeClass
    @SneakyThrows
    public static void setUpCluster() {
        // we need new instance because we're calling non-static methods from static method.
        // main purpose is to minimize network calls, initialization is only needed once
        HybridQueryFilterIT instance = new HybridQueryFilterIT();
        instance.initClient();
        instance.updateClusterSettings();
    }

    @SneakyThrows
    public void testFilterOnNeuralQueryFilterAndTermQueryFilter_thenSuccessful() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_ENABLED, false);
        prepareResourcesBeforeTestExecution(SHARDS_COUNT_IN_SINGLE_NODE_CLUSTER);
        testNeuralQueryBuilder(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD);
    }

    @SneakyThrows
    private void testNeuralQueryBuilder(String indexName) {
        String modelId = null;
        modelId = prepareModel();
        NeuralQueryBuilder neuralQueryBuilderTextQuery = NeuralQueryBuilder.builder()
            .fieldName(TEST_KNN_VECTOR_FIELD_NAME_1)
            .queryText(TEST_QUERY_TEXT)
            .modelId(modelId)
            .k(1)
            .build();

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEXT_FIELD_1_NAME, TEST_QUERY_TEXT);

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(neuralQueryBuilderTextQuery);
        hybridQueryBuilder.add(termQueryBuilder);

        hybridQueryBuilder.filter(new MatchQueryBuilder("_id", "1"));

        Map<String, Object> searchResponseAsMapTextQuery = search(indexName, hybridQueryBuilder, 1);
        assertEquals(1, getHitCount(searchResponseAsMapTextQuery));

        Map<String, Object> firstInnerHitTextQuery = getFirstInnerHit(searchResponseAsMapTextQuery);
        assertEquals("1", firstInnerHitTextQuery.get("_id"));
    }

    @SneakyThrows
    void prepareResourcesBeforeTestExecution(int numShards) {
        if (numShards == 1) {
            initializeIndexIfNotExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_SINGLE_SHARD, numShards);
        } else {
            initializeIndexIfNotExists(TEST_MULTI_DOC_INDEX_WITH_TEXT_AND_INT_MULTIPLE_SHARDS, numShards);
        }
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
    }

    @SneakyThrows
    private void initializeIndexIfNotExists(String indexName, int numShards) {
        if (!indexExists(indexName)) {
            prepareKnnIndex(
                indexName,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );

            indexTheDocument(
                indexName,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_2_DUNES),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_1_25),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector2).toArray()),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_1_DUNES),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_2_22),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_3_MI_1),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_3_256),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_2_ACTION),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "4",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_4_MI_2),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_4_25),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_2_ACTION),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "5",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_5_TERMINAL),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_5_20),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_1_DRAMA),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );

            indexTheDocument(
                indexName,
                "6",
                List.of(),
                List.of(),
                Collections.singletonList(TEXT_FIELD_1_NAME),
                Collections.singletonList(TEXT_FIELD_VALUE_6_AVENGERS),
                List.of(),
                Map.of(),
                List.of(INTEGER_FIELD_1_STOCK),
                List.of(INTEGER_FIELD_STOCK_5_20),
                List.of(KEYWORD_FIELD_2_CATEGORY),
                List.of(KEYWORD_FIELD_CATEGORY_3_SCI_FI),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null
            );
        }
    }

    private HybridQueryBuilder createHybridQueryBuilderWithMatchTermAndRangeQuery(String text, String value, int lte, int gte) {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(TEXT_FIELD_1_NAME, text);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEXT_FIELD_1_NAME, value);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder).add(termQueryBuilder).add(rangeQueryBuilder);
        return hybridQueryBuilder;
    }

    private QueryBuilder createQueryBuilderWithRangeQuery(int lte, int gte) {
        return QueryBuilders.rangeQuery(INTEGER_FIELD_1_STOCK).gte(gte).lte(lte);
    }

}
