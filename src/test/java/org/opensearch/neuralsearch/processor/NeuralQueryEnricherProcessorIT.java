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

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
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
    }

    @SneakyThrows
    public void testNeuralQueryEnricherProcessor_whenNoModelIdPassed_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(index);
            modelId = prepareModel();
            createSearchRequestProcessor(modelId, search_pipeline);
            createPipelineProcessor(modelId, ingest_pipeline, ProcessorType.TEXT_EMBEDDING);
            updateIndexSettings(index, Settings.builder().put("index.search.default_pipeline", search_pipeline));
            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
            neuralQueryBuilder.fieldName(TEST_KNN_VECTOR_FIELD_NAME_1);
            neuralQueryBuilder.queryText("Hello World");
            neuralQueryBuilder.k(1);
            Map<String, Object> response = search(index, neuralQueryBuilder, 2);
            assertFalse(response.isEmpty());
        } finally {
            wipeOfTestResources(index, ingest_pipeline, modelId, search_pipeline);
        }
    }

    @SneakyThrows
    public void testNeuralQueryEnricherProcessor_whenGetEmptyQueryBody_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(index);
            modelId = prepareModel();
            createSearchRequestProcessor(modelId, search_pipeline);
            createPipelineProcessor(modelId, ingest_pipeline, ProcessorType.TEXT_EMBEDDING);
            updateIndexSettings(index, Settings.builder().put("index.search.default_pipeline", search_pipeline));
            Request request = new Request("POST", "/" + index + "/_search");
            Response response = client().performRequest(request);
            assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
            String responseBody = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseInMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseBody, false);
            assertFalse(responseInMap.isEmpty());
            assertEquals(3, ((Map) responseInMap.get("hits")).size());
        } finally {
            wipeOfTestResources(index, ingest_pipeline, modelId, search_pipeline);
        }
    }

    @SneakyThrows
    public void testNeuralQueryEnricherProcessor_whenHybridQueryBuilderAndNoModelIdPassed_thenSuccess() {
        String modelId = null;
        try {
            initializeIndexIfNotExist(index);
            modelId = prepareModel();
            createSearchRequestProcessor(modelId, search_pipeline);
            createPipelineProcessor(modelId, ingest_pipeline, ProcessorType.TEXT_EMBEDDING);
            updateIndexSettings(index, Settings.builder().put("index.search.default_pipeline", search_pipeline));
            NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
            neuralQueryBuilder.fieldName(TEST_KNN_VECTOR_FIELD_NAME_1);
            neuralQueryBuilder.queryText("Hello World");
            neuralQueryBuilder.k(1);
            HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
            hybridQueryBuilder.add(neuralQueryBuilder);
            Map<String, Object> response = search(index, hybridQueryBuilder, 2);

            assertFalse(response.isEmpty());
        } finally {
            wipeOfTestResources(index, ingest_pipeline, modelId, search_pipeline);
        }
    }

    @SneakyThrows
    private void initializeIndexIfNotExist(String indexName) {
        if (indexName.equals(NeuralQueryEnricherProcessorIT.index) && !indexExists(indexName)) {
            prepareKnnIndex(
                indexName,
                Collections.singletonList(new KNNFieldConfig(TEST_KNN_VECTOR_FIELD_NAME_1, TEST_DIMENSION, TEST_SPACE_TYPE))
            );
            addKnnDoc(
                indexName,
                "1",
                Collections.singletonList(TEST_KNN_VECTOR_FIELD_NAME_1),
                Collections.singletonList(Floats.asList(testVector).toArray())
            );
            assertEquals(1, getDocCount(indexName));
        }
    }
}
