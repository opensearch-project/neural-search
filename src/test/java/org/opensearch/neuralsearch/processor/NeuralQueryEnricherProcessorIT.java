/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.TEST_DIMENSION;
import static org.opensearch.neuralsearch.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.TestUtils.createRandomVector;

import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import com.google.common.primitives.Floats;

import lombok.SneakyThrows;

public class NeuralQueryEnricherProcessorIT extends BaseNeuralSearchIT {

    private static final String index = "my-nlp-index";
    private static final String search_pipeline = "search-pipeline";
    private static final String ingest_pipeline = "nlp-pipeline";
    private static final String TEST_KNN_VECTOR_FIELD_NAME_1 = "test-knn-vector-1";
    private final float[] testVector = createRandomVector(TEST_DIMENSION);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
        prepareModel();
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteSearchPipeline(search_pipeline);
        findDeployedModels().forEach(this::deleteModel);
        deleteIndex(index);
    }

    @SneakyThrows
    public void testNeuralQueryEnricherProcessor_whenNoModelIdPassed_thenSuccess() {
        initializeIndexIfNotExist();
        String modelId = getDeployedModelId();
        createSearchRequestProcessor(modelId, search_pipeline);
        createPipelineProcessor(modelId, ingest_pipeline,ProcessorType.TEXT_EMBEDDING);
        updateIndexSettings(index, Settings.builder().put("index.search.default_pipeline", search_pipeline));
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName(TEST_KNN_VECTOR_FIELD_NAME_1);
        neuralQueryBuilder.queryText("Hello World");
        neuralQueryBuilder.k(1);
        Map<String, Object> response = search(index, neuralQueryBuilder, 2);

        assertFalse(response.isEmpty());

    }

    @SneakyThrows
    private void initializeIndexIfNotExist() {
        if (index.equals(NeuralQueryEnricherProcessorIT.index) && !indexExists(index)) {
            prepareKnnIndex(
                index,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                index,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(index));
        }
    }
}
