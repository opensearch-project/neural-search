/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.TestUtils.createRandomVector;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizationTechnique;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import com.google.common.primitives.Floats;

public class HybridQueryZScoreIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME = "test-neural-vector-doc-field-index";
    private static final String TEST_QUERY_TEXT = "greetings";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT5 = "welcome";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";

    private static final int TEST_DIMENSION = 768;
    private static final SpaceType TEST_SPACE_TYPE = SpaceType.L2;
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final static String RELATION_EQUAL_TO = "eq";
    private static final String SEARCH_PIPELINE = "phase-results-pipeline";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        prepareModel();
        createSearchPipeline(
            SEARCH_PIPELINE,
            ZScoreNormalizationTechnique.TECHNIQUE_NAME,
            DEFAULT_COMBINATION_METHOD,
            Map.of(PARAM_NAME_WEIGHTS, "[0.5,0.5]")
        );
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteSearchPipeline(SEARCH_PIPELINE);
        /* this is required to minimize chance of model not being deployed due to open memory CB,
         * this happens in case we leave model from previous test case. We use new model for every test, and old model
         * can be undeployed and deleted to free resources after each test case execution.
         */
        findDeployedModels().forEach(this::deleteModel);
    }

    @Override
    public boolean isUpdateClusterSettings() {
        return false;
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
    public void testComplexQuery_withZScoreNormalization() {
        initializeIndexIfNotExist();

        TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4);
        TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT5);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(termQueryBuilder2).should(termQueryBuilder3);

        String modelId = getDeployedModelId();
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            "",
            modelId,
            5,
            null,
            null
        );

        HybridQueryBuilder hybridQueryBuilderNeuralThenTerm = new HybridQueryBuilder();
        hybridQueryBuilderNeuralThenTerm.add(neuralQueryBuilder);
        hybridQueryBuilderNeuralThenTerm.add(boolQueryBuilder);

        final Map<String, Object> searchResponseAsMap = search(
            TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
            hybridQueryBuilderNeuralThenTerm,
            null,
            5,
            Map.of("search_pipeline", SEARCH_PIPELINE)
        );

        assertEquals(2, getHitCount(searchResponseAsMap));

        List<Map<String, Object>> hits1NestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hits1NestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }

        assertEquals(2, scores.size());
        // by design when there are only two results with z score since it's z-score normalized we would expect 1 , -1 to be the
        // corresponding score,
        // furthermore the combination logic with weights should make it doc1Score: (1 * w1 + 0.98 * w2)/(w1 + w2), doc2Score: -1 ~ 0
        assertEquals(0.9999, scores.get(0).floatValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0, scores.get(1).floatValue(), DELTA_FOR_SCORE_ASSERTION);

        // verify that scores are in desc order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());

        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(2, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
    }

    private void initializeIndexIfNotExist() throws IOException {
        if (!indexExists(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                ),
                1
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
            assertEquals(2, getDocCount(TEST_BASIC_VECTOR_DOC_FIELD_INDEX_NAME));
        }
    }
}
