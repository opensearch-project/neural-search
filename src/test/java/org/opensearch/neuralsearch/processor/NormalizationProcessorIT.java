/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.util.TestUtils.RELATION_EQUAL_TO;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.lang3.Range;
import org.junit.Before;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

public class NormalizationProcessorIT extends BaseNeuralSearchIT {
    private static final String TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME = "test-neural-multi-doc-one-shard-index";
    private static final String TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME = "test-neural-multi-doc-three-shards-index";
    private static final String TEST_QUERY_TEXT3 = "hello";
    private static final String TEST_QUERY_TEXT4 = "place";
    private static final String TEST_QUERY_TEXT6 = "notexistingword";
    private static final String TEST_QUERY_TEXT7 = "notexistingwordtwo";
    private static final String TEST_DOC_TEXT1 = "Hello world";
    private static final String TEST_DOC_TEXT2 = "Hi to this place";
    private static final String TEST_DOC_TEXT3 = "We would like to welcome everyone";
    private static final String TEST_DOC_TEXT4 = "Hello, I'm glad to you see you pal";
    private static final String TEST_DOC_TEXT5 = "Say hello and enter my friend";
    private static final String TEST_DOC_TEXT6 = "This tale grew in the telling";
    private static final String TEST_DOC_TEXT7 = "They do not and did not understand or like machines";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_TEXT_FIELD_NAME_1 = "test-text-field-1";
    private static final String TEST_TEXT_FIELD_NAME_2 = "test-text-field-2";
    private static final String SEARCH_PIPELINE = "phase-results-normalization-processor-pipeline";
    private final float[] testVector1 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector2 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector3 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector4 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector5 = createRandomVector(TEST_DIMENSION);
    private final float[] testVector6 = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    /**
     * Using search pipelines with result processor configs like below:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *                 "normalization": {
     *                     "technique": "min-max"
     *                 },
     *                 "combination": {
     *                     "technique": "arithmetic_mean"
     *                 }
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testResultProcessor_whenOneShardAndQueryMatches_thenSuccessful() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);

            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_DOC_TEXT1,
                "",
                modelId,
                5,
                null,
                null,
                null,
                null,
                null,
                null
            );
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(neuralQueryBuilder);
            hybridQueryBuilder.add(termQueryBuilder);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                hybridQueryBuilder,
                null,
                5,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );
            assertQueryResults(searchResponseAsMap, 5, false);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME, null, modelId, SEARCH_PIPELINE);
        }
    }

    /**
     * Using search pipelines with default result processor configs:
     * {
     *     "description": "Post processor for hybrid search",
     *     "phase_results_processors": [
     *         {
     *             "normalization-processor": {
     *             }
     *         }
     *     ]
     * }
     */
    @SneakyThrows
    public void testResultProcessor_whenDefaultProcessorConfigAndQueryMatches_thenSuccessful() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME);
            modelId = prepareModel();
            createSearchPipelineWithDefaultResultsPostProcessor(SEARCH_PIPELINE);

            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_DOC_TEXT1,
                "",
                modelId,
                5,
                null,
                null,
                null,
                null,
                null,
                null
            );
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(neuralQueryBuilder);
            hybridQueryBuilder.add(termQueryBuilder);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                hybridQueryBuilder,
                null,
                5,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );
            assertQueryResults(searchResponseAsMap, 5, false);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME, null, modelId, SEARCH_PIPELINE);
        }
    }

    @SneakyThrows
    public void testQueryMatches_whenMultipleShards_thenSuccessful() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME);
            modelId = prepareModel();
            createSearchPipelineWithResultsPostProcessor(SEARCH_PIPELINE);
            int totalExpectedDocQty = 6;

            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                TEST_KNN_VECTOR_FIELD_NAME_1,
                TEST_DOC_TEXT1,
                "",
                modelId,
                6,
                null,
                null,
                null,
                null,
                null,
                null
            );
            TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3);

            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(neuralQueryBuilder);
            hybridQueryBuilder.add(termQueryBuilder);

            Map<String, Object> searchResponseAsMap = search(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                hybridQueryBuilder,
                null,
                6,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );

            assertNotNull(searchResponseAsMap);
            Map<String, Object> total = getTotalHits(searchResponseAsMap);
            assertNotNull(total.get("value"));
            assertEquals(totalExpectedDocQty, total.get("value"));
            assertNotNull(total.get("relation"));
            assertEquals(RELATION_EQUAL_TO, total.get("relation"));
            assertTrue(getMaxScore(searchResponseAsMap).isPresent());
            assertTrue(Range.between(.5f, 1.0f).contains(getMaxScore(searchResponseAsMap).get()));
            List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
            List<String> ids = new ArrayList<>();
            List<Double> scores = new ArrayList<>();
            for (Map<String, Object> oneHit : hitsNestedList) {
                ids.add((String) oneHit.get("_id"));
                scores.add((Double) oneHit.get("_score"));
            }
            // verify scores order
            assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));

            // verify the scores are normalized. we need special assert logic because combined score may vary as neural search query
            // based on random vectors and return results for every doc. In some cases that may affect 1.0 score from term query and make it
            // lower.
            float highestScore = scores.stream().max(Double::compare).get().floatValue();
            assertTrue(Range.between(.5f, 1.0f).contains(highestScore));
            float lowestScore = scores.stream().min(Double::compare).get().floatValue();
            assertTrue(Range.between(.0f, .5f).contains(lowestScore));

            // verify that all ids are unique
            assertEquals(Set.copyOf(ids).size(), ids.size());

            // verify case when there are partial match
            HybridQueryBuilder hybridQueryBuilderPartialMatch = new HybridQueryBuilder();
            hybridQueryBuilderPartialMatch.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT3));
            hybridQueryBuilderPartialMatch.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT4));
            hybridQueryBuilderPartialMatch.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT7));

            Map<String, Object> searchResponseAsMapPartialMatch = search(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                hybridQueryBuilderPartialMatch,
                null,
                5,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );
            assertQueryResults(searchResponseAsMapPartialMatch, 4, true, Range.between(0.33f, 1.0f));

            // verify case when query doesn't have a match
            HybridQueryBuilder hybridQueryBuilderNoMatches = new HybridQueryBuilder();
            hybridQueryBuilderNoMatches.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT6));
            hybridQueryBuilderNoMatches.add(QueryBuilders.termQuery(TEST_TEXT_FIELD_NAME_1, TEST_QUERY_TEXT7));

            Map<String, Object> searchResponseAsMapNoMatches = search(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                hybridQueryBuilderNoMatches,
                null,
                5,
                Map.of("search_pipeline", SEARCH_PIPELINE)
            );
            assertQueryResults(searchResponseAsMapNoMatches, 0, true);
        } finally {
            wipeOfTestResources(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME, null, modelId, SEARCH_PIPELINE);
        }
    }

    private void initializeIndexIfNotExist(String indexName) throws IOException {
        if (TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME.equalsIgnoreCase(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                1
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT1)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector2).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector3).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT2)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "4",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME,
                "5",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector5).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4)
            );
            assertEquals(5, getDocCount(TEST_MULTI_DOC_INDEX_ONE_SHARD_NAME));
        }

        if (TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME.equalsIgnoreCase(indexName) && !indexExists(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE)),
                3
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector1).toArray()),
                List.of(TEST_TEXT_FIELD_NAME_1, TEST_TEXT_FIELD_NAME_2),
                List.of(TEST_DOC_TEXT1, TEST_DOC_TEXT6)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "2",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector2).toArray())
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "3",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector3).toArray()),
                List.of(TEST_TEXT_FIELD_NAME_1, TEST_TEXT_FIELD_NAME_2),
                List.of(TEST_DOC_TEXT2, TEST_DOC_TEXT7)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "4",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector4).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT3)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "5",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector5).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT4)
            );
            addKnnDoc(
                TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME,
                "6",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector6).toArray()),
                Collections.singletonList(TEST_TEXT_FIELD_NAME_1),
                Collections.singletonList(TEST_DOC_TEXT5)
            );
            assertEquals(6, getDocCount(TEST_MULTI_DOC_INDEX_THREE_SHARDS_NAME));
        }
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

    private void assertQueryResults(Map<String, Object> searchResponseAsMap, int totalExpectedDocQty, boolean assertMinScore) {
        assertQueryResults(searchResponseAsMap, totalExpectedDocQty, assertMinScore, Range.between(0.5f, 1.0f));
    }

    private void assertQueryResults(
        Map<String, Object> searchResponseAsMap,
        int totalExpectedDocQty,
        boolean assertMinScore,
        Range<Float> maxScoreRange
    ) {
        assertNotNull(searchResponseAsMap);
        Map<String, Object> total = getTotalHits(searchResponseAsMap);
        assertNotNull(total.get("value"));
        assertEquals(totalExpectedDocQty, total.get("value"));
        assertNotNull(total.get("relation"));
        assertEquals(RELATION_EQUAL_TO, total.get("relation"));
        assertTrue(getMaxScore(searchResponseAsMap).isPresent());
        if (totalExpectedDocQty > 0) {
            assertTrue(maxScoreRange.contains(getMaxScore(searchResponseAsMap).get()));
        } else {
            assertEquals(0.0, getMaxScore(searchResponseAsMap).get(), 0.001f);
        }

        List<Map<String, Object>> hitsNestedList = getNestedHits(searchResponseAsMap);
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> oneHit : hitsNestedList) {
            ids.add((String) oneHit.get("_id"));
            scores.add((Double) oneHit.get("_score"));
        }
        // verify scores order
        assertTrue(IntStream.range(0, scores.size() - 1).noneMatch(idx -> scores.get(idx) < scores.get(idx + 1)));
        // verify the scores are normalized
        if (totalExpectedDocQty > 0) {
            assertTrue(maxScoreRange.contains(scores.stream().max(Double::compare).get().floatValue()));
            if (assertMinScore) {
                assertEquals(0.001, (double) scores.stream().min(Double::compare).get(), 0.001);
            }
        }
        // verify that all ids are unique
        assertEquals(Set.copyOf(ids).size(), ids.size());
    }
}
