/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A SearchRequestProcessor to generate two-phase NeuralSparseQueryBuilder,
 * and add it to the Rescore of a searchRequest.
 */
@Setter
@Getter
public class NeuralSparseTwoPhaseProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "neural_sparse_two_phase_processor";
    private boolean enabled;
    private float pruneRatio;
    private PruneType pruneType;
    private float windowExpansion;
    private int maxWindowSize;
    private static final String PARAMETER_KEY = "two_phase_parameter";
    private static final String ENABLE_KEY = "enabled";
    private static final String EXPANSION_KEY = "expansion_rate";
    private static final String MAX_WINDOW_SIZE_KEY = "max_window_size";
    private static final boolean DEFAULT_ENABLED = true;
    private static final float DEFAULT_RATIO = 0.4f;
    private static final PruneType DEFAULT_PRUNE_TYPE = PruneType.MAX_RATIO;
    private static final float DEFAULT_WINDOW_EXPANSION = 5.0f;
    private static final int DEFAULT_MAX_WINDOW_SIZE = 10000;
    private static final int DEFAULT_BASE_QUERY_SIZE = 10;
    private static final int MAX_WINDOWS_SIZE_LOWER_BOUND = 50;
    private static final float WINDOW_EXPANSION_LOWER_BOUND = 1.0f;

    protected NeuralSparseTwoPhaseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        boolean enabled,
        float pruneRatio,
        PruneType pruneType,
        float windowExpansion,
        int maxWindowSize
    ) {
        super(tag, description, ignoreFailure);
        this.enabled = enabled;
        this.pruneRatio = pruneRatio;
        this.pruneType = pruneType;
        if (windowExpansion < WINDOW_EXPANSION_LOWER_BOUND) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The two_phase_parameter.expansion_rate must >= 1.0. Received: %f", windowExpansion)
            );
        }
        this.windowExpansion = windowExpansion;
        if (maxWindowSize < MAX_WINDOWS_SIZE_LOWER_BOUND) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "The two_phase_parameter.max_window_size must >= 50. Received: %n" + maxWindowSize)
            );
        }
        this.maxWindowSize = maxWindowSize;
    }

    /**
     * Process the search request of neural_sparse_two_phase_processor
     * @param request the search request (which may have been modified by an earlier processor)
     * @return request the search request that add the two-phase rescore query of neural sparse query.
     */
    @Override
    public SearchRequest processRequest(final SearchRequest request) {
        if (!enabled || pruneRatio == 0f) {
            return request;
        }
        QueryBuilder queryBuilder = request.source().query();
        // Collect the nested NeuralSparseQueryBuilder in the whole query.
        Multimap<NeuralSparseQueryBuilder, Float> queryBuilderMap;
        queryBuilderMap = collectNeuralSparseQueryBuilder(queryBuilder, 1.0f);
        if (queryBuilderMap.isEmpty()) {
            return request;
        }
        // Make a nestedQueryBuilder which includes all the two-phase QueryBuilder.
        QueryBuilder nestedTwoPhaseQueryBuilder = getNestedQueryBuilderFromNeuralSparseQueryBuilderMap(queryBuilderMap);
        nestedTwoPhaseQueryBuilder.boost(getOriginQueryWeightAfterRescore(request.source()));
        // Add it to the rescorer.
        RescorerBuilder<QueryRescorerBuilder> twoPhaseRescorer = buildRescoreQueryBuilderForTwoPhase(nestedTwoPhaseQueryBuilder, request);
        request.source().addRescorer(twoPhaseRescorer);
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private QueryBuilder getNestedQueryBuilderFromNeuralSparseQueryBuilderMap(
        final Multimap<NeuralSparseQueryBuilder, Float> queryBuilderFloatMap
    ) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        queryBuilderFloatMap.asMap().forEach((neuralSparseQueryBuilder, boosts) -> {
            float reduceBoost = boosts.stream().reduce(0.0f, Float::sum);
            boolQueryBuilder.should(neuralSparseQueryBuilder.boost(reduceBoost));
        });
        return boolQueryBuilder;
    }

    private float getOriginQueryWeightAfterRescore(final SearchSourceBuilder searchSourceBuilder) {
        if (Objects.isNull(searchSourceBuilder.rescores())) {
            return 1.0f;
        }
        return searchSourceBuilder.rescores()
            .stream()
            .map(rescorerBuilder -> ((QueryRescorerBuilder) rescorerBuilder).getQueryWeight())
            .reduce(1.0f, (a, b) -> a * b);
    }

    private Multimap<NeuralSparseQueryBuilder, Float> collectNeuralSparseQueryBuilder(final QueryBuilder queryBuilder, float baseBoost) {
        Multimap<NeuralSparseQueryBuilder, Float> result = ArrayListMultimap.create();

        if (queryBuilder instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) queryBuilder;
            float updatedBoost = baseBoost * boolQueryBuilder.boost();
            for (QueryBuilder subQuery : boolQueryBuilder.should()) {
                Multimap<NeuralSparseQueryBuilder, Float> subResult = collectNeuralSparseQueryBuilder(subQuery, updatedBoost);
                result.putAll(subResult);
            }
        } else if (queryBuilder instanceof NeuralSparseQueryBuilder) {
            NeuralSparseQueryBuilder neuralSparseQueryBuilder = (NeuralSparseQueryBuilder) queryBuilder;
            float updatedBoost = baseBoost * neuralSparseQueryBuilder.boost();
            /*
             * We obtain a copied modifiedQueryBuilder from the valid origin NeuralSparseQueryBuilder. After this,
             * when the original NeuralSparseQueryBuilder starts to rewrite, it will only retain the tokens that
             * have higher scores (controlled by the maxScore * ratio). The tokens with lower scores will be
             * passed to the modifiedQueryBuilder's queryTokenSupplier.
             *
             * By doing this, we reduce the score computation time for the original NeuralSparseQueryBuilder,
             * and use the modifiedQueryBuilder to make a score increment on TopDocs.
             *
             * When 2-phase is enabled:
             *     - Docs besides TopDocs: Score = HighScoreToken's score
             *     - Final TopDocs: Score = HighScoreToken's score + LowScoreToken's score
             */
            NeuralSparseQueryBuilder modifiedQueryBuilder = neuralSparseQueryBuilder.getCopyNeuralSparseQueryBuilderForTwoPhase(
                pruneRatio,
                pruneType
            );
            result.put(modifiedQueryBuilder, updatedBoost);
        }
        // We only support BoostQuery, BooleanQuery and NeuralSparseQuery now. For other compound query type which are not support now, will
        // do nothing and just quit.
        return result;
    }

    private RescorerBuilder<QueryRescorerBuilder> buildRescoreQueryBuilderForTwoPhase(
        final QueryBuilder nestedTwoPhaseQueryBuilder,
        final SearchRequest searchRequest
    ) {
        RescorerBuilder<QueryRescorerBuilder> twoPhaseRescorer = new QueryRescorerBuilder(nestedTwoPhaseQueryBuilder);
        int requestSize = searchRequest.source().size();
        int windowSize = (int) ((requestSize == -1 ? DEFAULT_BASE_QUERY_SIZE : requestSize) * windowExpansion);
        if (windowSize > maxWindowSize || windowSize < 0) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The two-phase window size of neural_sparse_two_phase_processor should be [0,%d], but get the value of %d",
                    maxWindowSize,
                    windowSize
                )
            );
        }
        twoPhaseRescorer.windowSize(windowSize);
        return twoPhaseRescorer;
    }

    /**
     * Factory to create NeuralSparseTwoPhaseProcessor, provide default parameter,
     *
     */
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

            boolean enabled = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, ENABLE_KEY, DEFAULT_ENABLED);
            Map<String, Object> twoPhaseConfigMap = ConfigurationUtils.readOptionalMap(TYPE, tag, config, PARAMETER_KEY);

            float pruneRatio = DEFAULT_RATIO;
            float windowExpansion = DEFAULT_WINDOW_EXPANSION;
            int maxWindowSize = DEFAULT_MAX_WINDOW_SIZE;
            PruneType pruneType = DEFAULT_PRUNE_TYPE;
            if (Objects.nonNull(twoPhaseConfigMap)) {
                pruneRatio = ((Number) twoPhaseConfigMap.getOrDefault(PruneUtils.PRUNE_RATIO_FIELD, pruneRatio)).floatValue();
                windowExpansion = ((Number) twoPhaseConfigMap.getOrDefault(EXPANSION_KEY, windowExpansion)).floatValue();
                maxWindowSize = ((Number) twoPhaseConfigMap.getOrDefault(MAX_WINDOW_SIZE_KEY, maxWindowSize)).intValue();
                pruneType = PruneType.fromString(
                    twoPhaseConfigMap.getOrDefault(PruneUtils.PRUNE_TYPE_FIELD, pruneType.getValue()).toString()
                );
            }
            if (!PruneUtils.isValidPruneRatio(pruneType, pruneRatio)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Illegal prune_ratio %f for prune_type: %s. %s",
                        pruneRatio,
                        pruneType.getValue(),
                        PruneUtils.getValidPruneRatioDescription(pruneType)
                    )
                );
            }

            return new NeuralSparseTwoPhaseProcessor(
                tag,
                description,
                ignoreFailure,
                enabled,
                pruneRatio,
                pruneType,
                windowExpansion,
                maxWindowSize
            );
        }
    }

}
