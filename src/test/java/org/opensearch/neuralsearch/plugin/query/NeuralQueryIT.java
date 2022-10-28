/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin.query;

import static org.opensearch.neuralsearch.plugin.TestUtils.createRandomVector;
import static org.opensearch.neuralsearch.plugin.TestUtils.objectToFloat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import com.google.common.primitives.Floats;

public class NeuralQueryIT extends BaseNeuralSearchIT {
    private static final String TEST_BASIC_INDEX_NAME = "test-neural-basic-index";
    private static final String TEST_MULTI_FIELD_INDEX_NAME = "test-neural-multi-field-index";
    private static final String TEST_NESTED_INDEX_NAME = "test-neural-nested-index";
    private static final String TEST_QUERY_TEXT = "Hello world";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_2 = "test-knn-vector-2";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_NESTED = "nested.knn.field";

    private static final int TEST_DIMENSION = 768;
    private static final SpaceType TEST_SPACE_TYPE = SpaceType.L2;
    private final float[] testVector = createRandomVector(TEST_DIMENSION);
    private final AtomicReference<String> modelId = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelId.compareAndSet(null, prepareModel());
    }

    private void maybeInitializeIndex(String indexName) throws IOException {
        if (TEST_BASIC_INDEX_NAME.equals(indexName) && !indexExists(TEST_BASIC_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_BASIC_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_BASIC_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(TEST_BASIC_INDEX_NAME));
        }

        if (TEST_MULTI_FIELD_INDEX_NAME.equals(indexName) && !indexExists(TEST_MULTI_FIELD_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_MULTI_FIELD_INDEX_NAME,
                List.of(
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE),
                    new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_2, TEST_DIMENSION, TEST_SPACE_TYPE)
                )
            );
            addKnnDoc(
                TEST_MULTI_FIELD_INDEX_NAME,
                "1",
                List.of(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_KNN_VECTOR_FIELD_NAME_2),
                List.of(Floats.asList(testVector).toArray(), Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(TEST_MULTI_FIELD_INDEX_NAME));
        }

        if (TEST_NESTED_INDEX_NAME.equals(indexName) && !indexExists(TEST_NESTED_INDEX_NAME)) {
            prepareKnnIndex(
                TEST_NESTED_INDEX_NAME,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_NESTED, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                TEST_NESTED_INDEX_NAME,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_NESTED),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(TEST_NESTED_INDEX_NAME));
        }
    }

    @SneakyThrows
    public void testBasicQuery() {
        maybeInitializeIndex(TEST_BASIC_INDEX_NAME);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals(firstInnerHit.get("_id"), "1");

        float expectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    @SneakyThrows
    public void testBoostQuery() {
        maybeInitializeIndex(TEST_BASIC_INDEX_NAME);
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        final float boost = 2.0f;
        neuralQueryBuilder.boost(boost);
        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals(firstInnerHit.get("_id"), "1");
        float expectedScore = boost * computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    @SneakyThrows
    public void testRescoreQuery() {
        maybeInitializeIndex(TEST_BASIC_INDEX_NAME);
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        NeuralQueryBuilder rescoreNeuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        Map<String, Object> searchResponseAsMap = search(TEST_BASIC_INDEX_NAME, matchAllQueryBuilder, rescoreNeuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals(firstInnerHit.get("_id"), "1");
        float expectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    @SneakyThrows
    public void testBooleanQuery() {
        maybeInitializeIndex(TEST_MULTI_FIELD_INDEX_NAME);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        NeuralQueryBuilder neuralQueryBuilder1 = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_1,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );
        NeuralQueryBuilder neuralQueryBuilder2 = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_2,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        boolQueryBuilder.should(neuralQueryBuilder1).should(neuralQueryBuilder2);

        Map<String, Object> searchResponseAsMap = search(TEST_MULTI_FIELD_INDEX_NAME, boolQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals(firstInnerHit.get("_id"), "1");
        float expectedScore = 2 * computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    @SneakyThrows
    public void testNestedQuery() {
        maybeInitializeIndex(TEST_NESTED_INDEX_NAME);

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            TEST_KNN_VECTOR_FIELD_NAME_NESTED,
            TEST_QUERY_TEXT,
            modelId.get(),
            1,
            null
        );

        Map<String, Object> searchResponseAsMap = search(TEST_NESTED_INDEX_NAME, neuralQueryBuilder, 1);
        Map<String, Object> firstInnerHit = getFirstInnerHit(searchResponseAsMap);

        assertEquals(firstInnerHit.get("_id"), "1");
        float expectedScore = computeExpectedScore(modelId.get(), testVector, TEST_SPACE_TYPE);
        assertEquals(expectedScore, objectToFloat(firstInnerHit.get("_score")), 0.0);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getFirstInnerHit(Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Object> hits2List = (List<Object>) hits1map.get("hits");
        assertTrue(hits2List.size() > 0);
        return (Map<String, Object>) hits2List.get(0);
    }

    @SneakyThrows
    private String prepareModel() {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        String modelId = uploadModel(requestBody);
        loadModel(modelId);
        return modelId;
    }

    @SneakyThrows
    private void prepareKnnIndex(String indexName, List<KNNFieldConfig> knnFieldConfigs) {
        createIndexWithConfiguration(indexName, buildIndexConfiguration(knnFieldConfigs), "");
    }

    @SneakyThrows
    private String buildIndexConfiguration(List<KNNFieldConfig> knnFieldConfigs) {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field("number_of_shards", 3)
            .field("index.knn", true)
            .endObject()
            .startObject("mappings")
            .startObject("properties");

        for (KNNFieldConfig knnFieldConfig : knnFieldConfigs) {
            xContentBuilder.startObject(knnFieldConfig.getName())
                .field("type", "knn_vector")
                .field("dimension", Integer.toString(knnFieldConfig.getDimension()))
                .startObject("method")
                .field("engine", "lucene")
                .field("space_type", knnFieldConfig.getSpaceType().getValue())
                .field("name", "hnsw")
                .endObject()
                .endObject();
        }
        return Strings.toString(xContentBuilder.endObject().endObject().endObject());
    }

    private float computeExpectedScore(String modelId, float[] indexVector, SpaceType spaceType) throws IOException {
        float[] queryVector = runInference(modelId, TEST_QUERY_TEXT);
        return spaceType.getVectorSimilarityFunction().compare(queryVector, indexVector);
    }

    @AllArgsConstructor
    @Getter
    private static class KNNFieldConfig {
        private final String name;
        private final Integer dimension;
        private final SpaceType spaceType;
    }
}
