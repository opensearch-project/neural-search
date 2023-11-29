/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.TestUtils.TEST_BASIC_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.TEST_KNN_VECTOR_FIELD_NAME_1;
import static org.opensearch.neuralsearch.TestUtils.ingest_pipeline;
import static org.opensearch.neuralsearch.TestUtils.search_pipeline;

import java.util.Map;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

public class NeuralQueryEnricherProcessorIT extends BaseNeuralSearchIT {
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
        deleteIndex(TEST_BASIC_INDEX_NAME);
    }

    @SneakyThrows
    public void testNeuralQueryEnricherProcessor_whenNoModelIdPassed_thenSuccess() {
        initializeBasicIndexIfNotExist(TEST_BASIC_INDEX_NAME);
        String modelId = getDeployedModelId();
        createSearchRequestProcessor(modelId, search_pipeline);
        createPipelineProcessor(modelId, ingest_pipeline);
        updateIndexSettings(TEST_BASIC_INDEX_NAME, Settings.builder().put("index.search.default_pipeline", search_pipeline));
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName(TEST_KNN_VECTOR_FIELD_NAME_1);
        neuralQueryBuilder.queryText("Hello World");
        neuralQueryBuilder.k(1);
        Map<String, Object> response = search(TEST_BASIC_INDEX_NAME, neuralQueryBuilder, 2);

        assertFalse(response.isEmpty());

    }
}
