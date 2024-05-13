/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.common.SetOnce;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A SearchRequestProcessor to generate two-phase NeuralSparseQueryBuilder,
 * and add it to the Rescore of a searchRequest.
 */
@Setter
@Getter
public class NeuralSparseTwoPhaseProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "neural_sparse_two_phase_processor";
    private boolean enabled;
    private float ratio;
    private float window_expansion;
    private int max_window_size;
    private static final  String PARAMETER_KEY = "two_phase_parameter";
    private static final  String RATIO_KEY = "prune_ratio";
    private static final  String ENABLE_KEY = "enabled";
    private static final  String EXPANSION_KEY = "expansion_rate";
    private static final  String MAX_WINDOW_SIZE_KEY = "max_window_size";

    protected NeuralSparseTwoPhaseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        boolean enabled,
        float ratio,
        float window_expansion,
        int max_window_size
    ) {
        super(tag, description, ignoreFailure);
        this.enabled = enabled;
        if (ratio < 0f || ratio > 1f) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "The prune ratio must be within [0, 1]. Received: %f", ratio));
        }
        this.ratio = ratio;
        if (window_expansion < 1.0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The window expansion must be greater than or equal to 1.0. Received: %f", window_expansion)
            );
        }
        this.window_expansion = window_expansion;
        if (max_window_size < 50) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The maximum window size must be greater than or equal to 50. Received: %n" + max_window_size)
            );
        }
        this.max_window_size = max_window_size;
    }

    /**
     * Process the search request of neural_sparse_two_phase_processor
     * @param request the search request (which may have been modified by an earlier processor)
     * @return request the search request that add the two-phase rescore query of neural sparse query.
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        if (!enabled) return request;
        QueryBuilder queryBuilder = request.source().query();
        Map<NeuralSparseQueryBuilder, Float> queryBuilderMap = new HashMap<>();
        processQueryBuilder(queryBuilder, queryBuilderMap, 1.0f);
        BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderFromNeuralSparseQueryBuilderMap(queryBuilderMap);
        boolQueryBuilder.boost(getOriginQueryWeightAfterRescore(request.source()));
        RescorerBuilder<QueryRescorerBuilder> rescorerBuilder = new QueryRescorerBuilder(boolQueryBuilder);
        int windowSize = (int) ((request.source().size() == -1 ? 10 : request.source().size()) * window_expansion);
        if (windowSize > max_window_size || windowSize < 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The two-phase window size of neural_sparse_two_phase_processor should be [0,%d], but get the value of %d",
                    max_window_size,
                    windowSize
                )
            );
        }
        rescorerBuilder.windowSize(windowSize);
        request.source().addRescorer(rescorerBuilder);
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Based on ratio, split a Map into two map by the value.
     * @param queryTokens the queryTokens map, key is the token String, value is the score.
     * @param ratio The ratio that control how tokens map be split.
     * @return A map has two element, {[True, token map whose value above threshold],[False, token map whose value below threshold]}
     */
    public static Map<Boolean, SetOnce<Map<String, Float>>> getSplitSetOnceByScoreThreshold(Map<String, Float> queryTokens, float ratio) {
        float max = 0f;
        for (Float value : queryTokens.values())
            max = value > max ? value : max;
        float threshold = max * ratio;
        Map<Boolean, Map<String, Float>> queryTokensByScore = queryTokens.entrySet()
            .stream()
            .collect(
                Collectors.partitioningBy(entry -> entry.getValue() >= threshold, Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
        SetOnce<Map<String, Float>> highScoreTksSetOnce = new SetOnce<>(queryTokensByScore.get(Boolean.TRUE));
        SetOnce<Map<String, Float>> lowScoreTksSetOnce = new SetOnce<>(queryTokensByScore.get(Boolean.FALSE));
        if (highScoreTksSetOnce.get() == null) throw new IllegalArgumentException();
        return Map.of(Boolean.TRUE, highScoreTksSetOnce, Boolean.FALSE, lowScoreTksSetOnce);
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        @Override
        public NeuralSparseTwoPhaseProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException {

            boolean enabled = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, ENABLE_KEY, true);
            Map<String, Object> map = ConfigurationUtils.readOptionalMap(TYPE, tag, config, PARAMETER_KEY);
            float ratio = 0.4f;
            float window_expansion = 5.0f;
            int max_window_size = 10000;
            if (map != null) {
                if (map.containsKey(RATIO_KEY)) {
                    ratio = ((Number) map.get(RATIO_KEY)).floatValue();
                }
                if (map.containsKey(EXPANSION_KEY)) {
                    window_expansion = ((Number) map.get(EXPANSION_KEY)).floatValue();
                }
                if (map.containsKey(MAX_WINDOW_SIZE_KEY)) {
                    max_window_size = ((Number) map.get(MAX_WINDOW_SIZE_KEY)).intValue();
                }
            }
            return new NeuralSparseTwoPhaseProcessor(tag, description, ignoreFailure, enabled, ratio, window_expansion, max_window_size);

        }
    }

    private void processQueryBuilder(
        QueryBuilder queryBuilder,
        Map<NeuralSparseQueryBuilder, Float> neuralSparseQueryBuilderFloatMap,
        float baseBoost
    ) {
        if (queryBuilder instanceof BoolQueryBuilder) {
            processQueryBuilder((BoolQueryBuilder) queryBuilder, neuralSparseQueryBuilderFloatMap, baseBoost);
        } else if (queryBuilder instanceof NeuralSparseQueryBuilder) {
            processQueryBuilder((NeuralSparseQueryBuilder) queryBuilder, neuralSparseQueryBuilderFloatMap, baseBoost);
        }
    }

    private void processQueryBuilder(
        BoolQueryBuilder queryBuilder,
        Map<NeuralSparseQueryBuilder, Float> neuralSparseQueryBuilderFloatMap,
        float baseBoost
    ) {
        baseBoost *= queryBuilder.boost();
        for (QueryBuilder subShouldQueryBuilder : queryBuilder.should()) {
            processQueryBuilder(subShouldQueryBuilder, neuralSparseQueryBuilderFloatMap, baseBoost);
        }
    }

    private void processQueryBuilder(
        NeuralSparseQueryBuilder queryBuilder,
        Map<NeuralSparseQueryBuilder, Float> neuralSparseQueryBuilderFloatMap,
        float baseBoost
    ) {
        float finalBaseBoost = baseBoost * queryBuilder.boost();
        neuralSparseQueryBuilderFloatMap.put(queryBuilder.copyForTwoPhase(ratio), finalBaseBoost);
    }

    private BoolQueryBuilder getBoolQueryBuilderFromNeuralSparseQueryBuilderMap(Map<NeuralSparseQueryBuilder, Float> queryBuilderFloatMap) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        queryBuilderFloatMap.forEach((neuralSparseQueryBuilder, boost) -> {
            final float finalBoost = neuralSparseQueryBuilder.boost() * boost;
            boolQueryBuilder.should(neuralSparseQueryBuilder.boost(finalBoost));
        });
        return boolQueryBuilder;
    }

    private static float getOriginQueryWeightAfterRescore(SearchSourceBuilder searchSourceBuilder) {
        if (searchSourceBuilder.rescores() == null) return 1.0f;
        return searchSourceBuilder.rescores()
            .stream()
            .map(rescorerBuilder -> ((QueryRescorerBuilder) rescorerBuilder).getQueryWeight())
            .reduce(1.0f, (a, b) -> a * b);
    }

}
