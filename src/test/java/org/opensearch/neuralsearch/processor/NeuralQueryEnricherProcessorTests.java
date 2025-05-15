/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralQueryEnricherProcessorTests extends OpenSearchTestCase {

    public void testFactory_whenMissingQueryParam_thenThrowException() throws Exception {
        NeuralQueryEnricherProcessor.Factory factory = new NeuralQueryEnricherProcessor.Factory();
        NeuralQueryEnricherProcessor processor = createTestProcessor(factory);
        assertEquals("vasdcvkcjkbldbjkd", processor.getModelId());
        assertEquals("bahbkcdkacb", processor.getNeuralFieldDefaultIdMap().get("fieldName").toString());

        // Missing "query" parameter:
        expectThrows(
            IllegalArgumentException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null)
        );
    }

    public void testFactory_whenModelIdIsNotString_thenFail() {
        NeuralQueryEnricherProcessor.Factory factory = new NeuralQueryEnricherProcessor.Factory();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("default_model_id", 55555L);
        expectThrows(OpenSearchParseException.class, () -> factory.create(Collections.emptyMap(), null, null, false, configMap, null));
    }

    public void testProcessRequest_whenVisitingQueryBuilder_thenSuccess() throws Exception {
        NeuralSearchClusterTestUtils.setUpClusterService();
        NeuralQueryEnricherProcessor.Factory factory = new NeuralQueryEnricherProcessor.Factory();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName("field_name").queryText("query_text").build();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        NeuralQueryEnricherProcessor processor = createTestProcessor(factory);
        SearchRequest processSearchRequest = processor.processRequest(searchRequest);
        assertEquals(processSearchRequest, searchRequest);
    }

    public void testProcessRequest_whenVisitingEmptyQueryBody_thenSuccess() throws Exception {
        NeuralQueryEnricherProcessor.Factory factory = new NeuralQueryEnricherProcessor.Factory();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder());
        assertNull(searchRequest.source().query());
        NeuralQueryEnricherProcessor processor = createTestProcessor(factory);
        SearchRequest processSearchRequest = processor.processRequest(searchRequest);
        // should do nothing
        assertNull(processSearchRequest.source().query());
    }

    public void testType() throws Exception {
        NeuralQueryEnricherProcessor.Factory factory = new NeuralQueryEnricherProcessor.Factory();
        NeuralQueryEnricherProcessor processor = createTestProcessor(factory);
        assertEquals(NeuralQueryEnricherProcessor.TYPE, processor.getType());
    }

    private NeuralQueryEnricherProcessor createTestProcessor(NeuralQueryEnricherProcessor.Factory factory) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("default_model_id", "vasdcvkcjkbldbjkd");
        configMap.put("neural_field_default_id", Map.of("fieldName", "bahbkcdkacb"));
        NeuralQueryEnricherProcessor processor = factory.create(Collections.emptyMap(), null, null, false, configMap, null);
        return processor;
    }
}
