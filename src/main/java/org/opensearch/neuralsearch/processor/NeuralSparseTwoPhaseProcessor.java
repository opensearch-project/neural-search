/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.SetOnce;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@Setter
@Getter
public class NeuralSparseTwoPhaseProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static String TYPE = "neural_sparse_two_phase_processor";
    boolean enable;
    float ratio;
    float window_expansion;
    int max_window_size;
    static final String PARAMETER_KEY = "two_phase_parameter";
    static final String RATIO_KEY = "prune_ratio";
    static final String ENABLE_KEY = "enabled";
    static final String EXPANSION_KEY = "expansion";
    static final String MAX_WINDOW_SIZE_KEY = "max_window_size";

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
        this.enable = enabled;
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

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        if (!enable) return request;
        QueryBuilder queryBuilder = request.source().query();
        Map<NeuralSparseQueryBuilder, Float> queryBuilderMap = new HashMap<>();
        QueryBuilderVisitor queryBuilderVisitor = new TwoPhaseQueryBuilderVisitor(queryBuilderMap, 1.0f);
        queryBuilder.visit(queryBuilderVisitor);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        queryBuilderMap.forEach((key, value) -> boolQueryBuilder.should(((QueryBuilder) key).boost(value)));
        RescorerBuilder<QueryRescorerBuilder> rescorerBuilder = new QueryRescorerBuilder(boolQueryBuilder);
        rescorerBuilder.windowSize((int) (request.source().size() == -1 ? 10 : request.source().size() * window_expansion));
        request.source().addRescorer(rescorerBuilder);
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static Map<Boolean, SetOnce<Map<String, Float>>> getSplitSetOnceByScoreThreshold(Map<String, Float> queryTokens) {
        float max = 0f;
        for (Float value : queryTokens.values())
            max = value > max ? value : max;
        float threshold = max * 0.4f;
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

    @Setter
    static private class TwoPhaseQueryBuilderVisitor implements QueryBuilderVisitor {
        @Setter
        float boost;
        float subBoost;
        Map<NeuralSparseQueryBuilder, Float> queryBuilderMap;

        public TwoPhaseQueryBuilderVisitor(Map<NeuralSparseQueryBuilder, Float> queryBuilderMap, float boost) {
            this.queryBuilderMap = queryBuilderMap;
            this.boost = boost;
            this.subBoost = boost;
        }

        @Override
        public void accept(QueryBuilder qb) {
            if (qb instanceof NeuralSparseQueryBuilder) {
                this.queryBuilderMap.put(((NeuralSparseQueryBuilder) qb).copyForTwoPhase(), boost * qb.boost());
            } else {
                subBoost *= qb.boost();
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            if (occur.equals(BooleanClause.Occur.SHOULD)) return new TwoPhaseQueryBuilderVisitor(queryBuilderMap, subBoost);
            return NO_OP_VISITOR;
        }
    }
}
