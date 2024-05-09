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
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Setter
@Getter
public class NeuralSparseTwoPhaseProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static String TYPE = "neural_sparse_two_phase_processor";
    public float ratio = 0.4F;

    protected NeuralSparseTwoPhaseProcessor(String tag, String description, boolean ignoreFailure) {
        super(tag, description, ignoreFailure);
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        QueryBuilder queryBuilder = request.source().query();
        Map<NeuralSparseQueryBuilder, Float> queryBuilderMap = new HashMap<>();
        QueryBuilderVisitor queryBuilderVisitor = new TwoPhaseQueryBuilderVisitor(queryBuilderMap);
        queryBuilder.visit(queryBuilderVisitor);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        queryBuilderMap.forEach((key, value) -> boolQueryBuilder.should(((QueryBuilder) key).boost(value)));
        RescorerBuilder<QueryRescorerBuilder> rescorerBuilder = new QueryRescorerBuilder(boolQueryBuilder);
        request.source().addRescorer(rescorerBuilder);
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static Map<Boolean, SetOnce<Map<String, Float>>> splitSetOnce(Map<String, Float> queryTokens) {
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
            return new NeuralSparseTwoPhaseProcessor(tag, description, ignoreFailure);
        }

    }

    @Setter
    static class TwoPhaseQueryBuilderVisitor implements QueryBuilderVisitor {

        float boost = 1;
        Map<NeuralSparseQueryBuilder, Float> queryBuilderMap;

        public TwoPhaseQueryBuilderVisitor(Map<NeuralSparseQueryBuilder, Float> queryBuilderMap) {
            this.queryBuilderMap = queryBuilderMap;
        }

        @Override
        public void accept(QueryBuilder qb) {
            if (qb instanceof NeuralSparseQueryBuilder) {
                this.queryBuilderMap.put(((NeuralSparseQueryBuilder) qb).copyForTwoPhase(), boost);
            } else {
                boost *= qb.boost();
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            if (occur.equals(BooleanClause.Occur.SHOULD)) return this;
            return NO_OP_VISITOR;
        }
    }
}
