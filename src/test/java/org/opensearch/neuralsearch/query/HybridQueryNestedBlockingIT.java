/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

import java.util.Collections;
import java.util.Map;

import org.junit.BeforeClass;
import org.opensearch.client.ResponseException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;

/**
 * Integration tests verifying that hybrid queries nested inside other compound queries
 * are correctly blocked with a clear error message. Hybrid query must always be the
 * top-level query — nesting it inside function_score, constant_score, bool, dis_max,
 * or any combination thereof is not supported.
 */
public class HybridQueryNestedBlockingIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX = "test-hybrid-nested-blocking-index";
    private static final String SEARCH_PIPELINE = "phase-results-hybrid-nested-blocking-pipeline";
    private static final String TEST_KNN_VECTOR_FIELD = "test-knn-vector-1";
    private static final String TEST_TEXT_FIELD = "test-text-field-1";
    private static final String QUERY_TEXT = "hello";
    private static final String QUERY_TEXT_2 = "world";
    private static final String DOC_TEXT_1 = "Hello world";
    private static final String DOC_TEXT_2 = "Hi to this place";
    private static final String EXPECTED_ERROR = "hybrid query must be a top level query and cannot be wrapped into other queries";

    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);

    @BeforeClass
    @SneakyThrows
    public static void setUpCluster() {
        HybridQueryNestedBlockingIT instance = new HybridQueryNestedBlockingIT();
        instance.initClient();
        instance.updateClusterSettings();
    }

    @SneakyThrows
    private void initializeIndexIfNotExist() {
        if (indexExists(TEST_INDEX)) {
            return;
        }
        prepareKnnIndex(
            TEST_INDEX,
            Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD, TEST_DIMENSION, TEST_SPACE_TYPE)),
            1
        );
        addKnnDoc(
            TEST_INDEX,
            "1",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD),
            Collections.singletonList(Floats.asList(testVector1).toArray()),
            Collections.singletonList(TEST_TEXT_FIELD),
            Collections.singletonList(DOC_TEXT_1)
        );
        addKnnDoc(
            TEST_INDEX,
            "2",
            Collections.singletonList(TEST_KNN_VECTOR_FIELD),
            Collections.singletonList(Floats.asList(testVector2).toArray()),
            Collections.singletonList(TEST_TEXT_FIELD),
            Collections.singletonList(DOC_TEXT_2)
        );
        assertEquals(2, getDocCount(TEST_INDEX));
    }

    private HybridQueryBuilder createSimpleHybridQuery() {
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(QueryBuilders.matchQuery(TEST_TEXT_FIELD, QUERY_TEXT));
        hybridQueryBuilder.add(QueryBuilders.matchQuery(TEST_TEXT_FIELD, QUERY_TEXT_2));
        return hybridQueryBuilder;
    }

    private void assertNestedHybridBlocked(QueryBuilder query) {
        initializeIndexIfNotExist();
        createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> search(TEST_INDEX, query, null, 10, Map.of("search_pipeline", SEARCH_PIPELINE), null)
        );

        org.hamcrest.MatcherAssert.assertThat(
            exception.getMessage(),
            allOf(containsString(EXPECTED_ERROR), containsString("illegal_argument_exception"))
        );
    }

    // Depth 1: direct wrapping

    @SneakyThrows
    public void testNestedHybrid_whenWrappedInFunctionScore_thenFail() {
        assertNestedHybridBlocked(QueryBuilders.functionScoreQuery(createSimpleHybridQuery()));
    }

    @SneakyThrows
    public void testNestedHybrid_whenWrappedInConstantScore_thenFail() {
        assertNestedHybridBlocked(QueryBuilders.constantScoreQuery(createSimpleHybridQuery()));
    }

    @SneakyThrows
    public void testNestedHybrid_whenWrappedInBoolShould_thenFail() {
        // bool with hybrid + another clause to prevent single-clause rewrite optimization
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .should(createSimpleHybridQuery())
            .should(QueryBuilders.matchQuery(TEST_TEXT_FIELD, QUERY_TEXT));
        assertNestedHybridBlocked(boolQuery);
    }

    @SneakyThrows
    public void testNestedHybrid_whenWrappedInDisMax_thenFail() {
        // dis_max with hybrid + another clause to prevent single-clause rewrite optimization
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery()
            .add(createSimpleHybridQuery())
            .add(QueryBuilders.matchQuery(TEST_TEXT_FIELD, QUERY_TEXT));
        assertNestedHybridBlocked(disMaxQuery);
    }

    // Depth 2: mixed compound query nesting

    @SneakyThrows
    public void testNestedHybrid_whenInsideFunctionScoreInsideBool_thenFail() {
        // bool(should: function_score(hybrid))
        QueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(createSimpleHybridQuery());
        assertNestedHybridBlocked(QueryBuilders.boolQuery().should(functionScoreQuery));
    }

    @SneakyThrows
    public void testNestedHybrid_whenInsideBoolInsideFunctionScore_thenFail() {
        // function_score(bool(must: hybrid))
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().must(createSimpleHybridQuery());
        assertNestedHybridBlocked(QueryBuilders.functionScoreQuery(boolQuery));
    }

    @SneakyThrows
    public void testNestedHybrid_whenInsideDisMaxInsideBool_thenFail() {
        // bool(must: dis_max(hybrid, match))
        DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery()
            .add(createSimpleHybridQuery())
            .add(QueryBuilders.matchQuery(TEST_TEXT_FIELD, QUERY_TEXT));
        assertNestedHybridBlocked(QueryBuilders.boolQuery().must(disMaxQuery));
    }

    @SneakyThrows
    public void testNestedHybrid_whenInsideConstantScoreInsideBool_thenFail() {
        // bool(filter: constant_score(hybrid))
        QueryBuilder constantScoreQuery = QueryBuilders.constantScoreQuery(createSimpleHybridQuery());
        assertNestedHybridBlocked(QueryBuilders.boolQuery().filter(constantScoreQuery));
    }

    // Depth 3: triple nesting across type boundaries

    @SneakyThrows
    public void testNestedHybrid_whenInsideBoolInsideFunctionScoreInsideBool_thenFail() {
        // bool(must: function_score(bool(should: hybrid)))
        BoolQueryBuilder innerBool = QueryBuilders.boolQuery().should(createSimpleHybridQuery());
        QueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(innerBool);
        assertNestedHybridBlocked(QueryBuilders.boolQuery().must(functionScoreQuery));
    }
}
