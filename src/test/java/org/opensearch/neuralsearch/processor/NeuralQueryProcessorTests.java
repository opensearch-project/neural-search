/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralQueryProcessorTests extends OpenSearchTestCase {

    public void testFactory() throws Exception {
        NeuralQueryProcessor.Factory factory = new NeuralQueryProcessor.Factory();
        NeuralQueryProcessor processor = createTestProcessor(factory);
        assertEquals("vasdcvkcjkbldbjkd", processor.getModelId());
        assertEquals("bahbkcdkacb", processor.getNeuralFieldDefaultIdMap().get("fieldName").toString());

        // Missing "query" parameter:
        expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null)
        );
    }

    public void testFactory_whenModelIdIsNotString_thenFail() {
        NeuralQueryProcessor.Factory factory = new NeuralQueryProcessor.Factory();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("default_model_id", 55555L);
        expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), null, null, false, configMap, null));
    }

    public void testProcessRequest() throws Exception {
        NeuralQueryProcessor.Factory factory = new NeuralQueryProcessor.Factory();
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        NeuralQueryProcessor processor = createTestProcessor(factory);
        SearchRequest processSearchRequest = processor.processRequest(searchRequest);
        assertEquals(processSearchRequest, searchRequest);
    }

    public void testType() throws Exception {
        NeuralQueryProcessor.Factory factory = new NeuralQueryProcessor.Factory();
        NeuralQueryProcessor processor = createTestProcessor(factory);
        assertEquals(NeuralQueryProcessor.TYPE, processor.getType());
    }

    private NeuralQueryProcessor createTestProcessor(NeuralQueryProcessor.Factory factory) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("default_model_id", "vasdcvkcjkbldbjkd");
        configMap.put("neural_field_default_id", Map.of("fieldName", "bahbkcdkacb"));
        NeuralQueryProcessor processor = factory.create(Collections.emptyMap(), null, null, false, configMap, null);
        return processor;
    }
}
