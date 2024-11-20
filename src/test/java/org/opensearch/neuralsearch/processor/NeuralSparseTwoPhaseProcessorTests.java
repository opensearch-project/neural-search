/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NeuralSparseTwoPhaseProcessorTests extends OpenSearchTestCase {
    static final private String PARAMETER_KEY = "two_phase_parameter";
    static final private String ENABLE_KEY = "enabled";
    static final private String EXPANSION_KEY = "expansion_rate";
    static final private String MAX_WINDOW_SIZE_KEY = "max_window_size";

    public void testFactory_whenCreateDefaultPipeline_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory);
        assertEquals(0.3f, processor.getPruneRatio(), 1e-3);
        assertEquals(4.0f, processor.getWindowExpansion(), 1e-3);
        assertEquals(10000, processor.getMaxWindowSize());
        assertEquals(PruneType.MAX_RATIO, processor.getPruneType());

        NeuralSparseTwoPhaseProcessor defaultProcessor = factory.create(
            Collections.emptyMap(),
            null,
            null,
            false,
            Collections.emptyMap(),
            null
        );
        assertEquals(0.4f, defaultProcessor.getPruneRatio(), 1e-3);
        assertEquals(5.0f, defaultProcessor.getWindowExpansion(), 1e-3);
        assertEquals(10000, defaultProcessor.getMaxWindowSize());
        assertEquals(PruneType.MAX_RATIO, processor.getPruneType());
    }

    public void testFactory_whenCreatePipelineWithCustomPruneType_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 5f, "top_k", true, 5f, 1000);
        assertEquals(5f, processor.getPruneRatio(), 1e-6);
        assertEquals(PruneType.TOP_K, processor.getPruneType());
    }

    public void testFactory_whenRatioOutOfRange_thenThrowException() {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 1.1f, true, 5.0f, 10000));
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 1.1f, "max_ratio", true, 5.0f, 10000));
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 0f, "top_k", true, 5.0f, 10000));
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 1.1f, "alpha_mass", true, 5.0f, 10000));
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, -1f, "abs_value", true, 5.0f, 10000));
    }

    public void testFactory_whenWindowExpansionOutOfRange_thenThrowException() {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 0.1f, true, 0.5f, 10000));
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 0.1f, true, -0.5f, 10000));
    }

    public void testFactory_whenMaxWindowSizeOutOfRange_thenThrowException() {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        expectThrows(IllegalArgumentException.class, () -> createTestProcessor(factory, 0.1f, true, 5.5f, -1));
    }

    public void testProcessRequest_whenTwoPhaseEnabled_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, true, 4.0f, 10000);
        processor.processRequest(searchRequest);
        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) searchRequest.source().query();
        assertEquals(queryBuilder.twoPhasePruneRatio(), 0.5f, 1e-3);
        assertNotNull(searchRequest.source().rescores());
    }

    public void testProcessRequest_whenUseCustomPruneType_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, "alpha_mass", true, 4.0f, 10000);
        processor.processRequest(searchRequest);
        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) searchRequest.source().query();
        assertEquals(queryBuilder.twoPhasePruneRatio(), 0.5f, 1e-3);
        assertEquals(queryBuilder.twoPhasePruneType(), PruneType.ALPHA_MASS);
        assertNotNull(searchRequest.source().rescores());
    }

    public void testProcessRequest_whenTwoPhaseEnabledAndNestedBoolean_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(neuralQueryBuilder);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, true, 4.0f, 10000);
        processor.processRequest(searchRequest);
        BoolQueryBuilder queryBuilder = (BoolQueryBuilder) searchRequest.source().query();
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = (NeuralSparseQueryBuilder) queryBuilder.should().get(0);
        assertEquals(neuralSparseQueryBuilder.twoPhasePruneRatio(), 0.5f, 1e-3);
        assertNotNull(searchRequest.source().rescores());
    }

    public void testProcessRequestWithRescorer_whenTwoPhaseEnabled_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        QueryRescorerBuilder queryRescorerBuilder = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        queryRescorerBuilder.setRescoreQueryWeight(0f);
        searchRequest.source().addRescorer(queryRescorerBuilder);
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, true, 4.0f, 10000);
        processor.processRequest(searchRequest);
        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) searchRequest.source().query();
        assertEquals(queryBuilder.twoPhasePruneRatio(), 0.5f, 1e-3);
        assertNotNull(searchRequest.source().rescores());
    }

    public void testProcessRequest_whenTwoPhaseDisabled_thenSuccess() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, false, 4.0f, 10000);
        processor.processRequest(searchRequest);
        NeuralSparseQueryBuilder queryBuilder = (NeuralSparseQueryBuilder) searchRequest.source().query();
        assertEquals(queryBuilder.twoPhasePruneRatio(), 0f, 1e-3);
        assertNull(searchRequest.source().rescores());
    }

    @SneakyThrows
    public void testProcessRequest_whenTwoPhaseEnabledAndOutOfWindowSize_thenThrowException() {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(neuralQueryBuilder));
        QueryRescorerBuilder queryRescorerBuilder = new QueryRescorerBuilder(new MatchAllQueryBuilder());
        queryRescorerBuilder.setRescoreQueryWeight(0f);
        searchRequest.source().addRescorer(queryRescorerBuilder);
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, true, 400.0f, 100);
        expectThrows(IllegalArgumentException.class, () -> processor.processRequest(searchRequest));
    }

    @SneakyThrows
    public void testProcessRequest_whenTwoPhaseEnabledAndWithOutNeuralSparseQuery_thenReturnRequest() {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(new MatchAllQueryBuilder());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory, 0.5f, true, 400.0f, 100);
        SearchRequest returnRequest = processor.processRequest(searchRequest);
        assertNull(returnRequest.source().rescores());
    }

    public void testType() throws Exception {
        NeuralSparseTwoPhaseProcessor.Factory factory = new NeuralSparseTwoPhaseProcessor.Factory();
        NeuralSparseTwoPhaseProcessor processor = createTestProcessor(factory);
        assertEquals(NeuralSparseTwoPhaseProcessor.TYPE, processor.getType());
    }

    private NeuralSparseTwoPhaseProcessor createTestProcessor(
        NeuralSparseTwoPhaseProcessor.Factory factory,
        float ratio,
        boolean enabled,
        float expand,
        int max_window
    ) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ENABLE_KEY, enabled);
        Map<String, Object> twoPhaseParaMap = new HashMap<>();
        twoPhaseParaMap.put(PruneUtils.PRUNE_RATIO_FIELD, ratio);
        twoPhaseParaMap.put(EXPANSION_KEY, expand);
        twoPhaseParaMap.put(MAX_WINDOW_SIZE_KEY, max_window);
        configMap.put(PARAMETER_KEY, twoPhaseParaMap);
        return factory.create(Collections.emptyMap(), null, null, false, configMap, null);
    }

    private NeuralSparseTwoPhaseProcessor createTestProcessor(
        NeuralSparseTwoPhaseProcessor.Factory factory,
        float ratio,
        String type,
        boolean enabled,
        float expand,
        int max_window
    ) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ENABLE_KEY, enabled);
        Map<String, Object> twoPhaseParaMap = new HashMap<>();
        twoPhaseParaMap.put(PruneUtils.PRUNE_RATIO_FIELD, ratio);
        twoPhaseParaMap.put(EXPANSION_KEY, expand);
        twoPhaseParaMap.put(MAX_WINDOW_SIZE_KEY, max_window);
        twoPhaseParaMap.put(PruneUtils.PRUNE_TYPE_FIELD, type);
        configMap.put(PARAMETER_KEY, twoPhaseParaMap);
        return factory.create(Collections.emptyMap(), null, null, false, configMap, null);
    }

    private NeuralSparseTwoPhaseProcessor createTestProcessor(NeuralSparseTwoPhaseProcessor.Factory factory) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ENABLE_KEY, true);
        Map<String, Object> twoPhaseParaMap = new HashMap<>();
        twoPhaseParaMap.put(PruneUtils.PRUNE_RATIO_FIELD, 0.3f);
        twoPhaseParaMap.put(EXPANSION_KEY, 4.0f);
        twoPhaseParaMap.put(MAX_WINDOW_SIZE_KEY, 10000);
        configMap.put(PARAMETER_KEY, twoPhaseParaMap);
        return factory.create(Collections.emptyMap(), null, null, false, configMap, null);
    }
}
