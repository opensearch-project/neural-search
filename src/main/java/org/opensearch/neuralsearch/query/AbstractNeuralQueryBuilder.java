/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.neuralsearch.util.prune.PruneType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Abstract class defines the common fields and behavior shared by the neural query builder and the neural sparse query
 * builder.
 * @param <QB>
 */
@Getter
@Setter
@NoArgsConstructor
public abstract class AbstractNeuralQueryBuilder<QB extends AbstractNeuralQueryBuilder<QB>> extends AbstractQueryBuilder<QB>
    implements
        ModelInferenceQueryBuilder {
    @VisibleForTesting
    public static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    @VisibleForTesting
    public static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    @VisibleForTesting
    public static final ParseField QUERY_TOKENS_FIELD = new ParseField("query_tokens");

    // both dense and sparse embedding can use
    protected String modelId;
    protected String fieldName;
    protected String queryText;

    // sparse embedding only
    protected String searchAnalyzer;
    protected Supplier<Map<String, Float>> queryTokensMapSupplier;
    protected NeuralSparseQueryTwoPhaseInfo neuralSparseQueryTwoPhaseInfo = new NeuralSparseQueryTwoPhaseInfo();

    public AbstractNeuralQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public QB modelId(String modelId) {
        this.modelId = modelId;
        return (QB) this;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    public QB fieldName(String fieldName) {
        this.fieldName = fieldName;
        return (QB) this;
    }

    public final String queryText() {
        return queryText;
    }

    public final QB queryText(String queryText) {
        this.queryText = queryText;
        return (QB) this;
    }

    public final String searchAnalyzer() {
        return searchAnalyzer;
    }

    public final QB searchAnalyzer(String searchAnalyzer) {
        this.searchAnalyzer = searchAnalyzer;
        return (QB) this;
    }

    public final Supplier<Map<String, Float>> queryTokensMapSupplier() {
        return queryTokensMapSupplier;
    }

    public final QB queryTokensMapSupplier(Supplier<Map<String, Float>> queryTokensMapSupplier) {
        this.queryTokensMapSupplier = queryTokensMapSupplier;
        return (QB) this;
    }

    public final NeuralSparseQueryTwoPhaseInfo neuralSparseQueryTwoPhaseInfo() {
        return neuralSparseQueryTwoPhaseInfo;
    }

    public final QB neuralSparseQueryTwoPhaseInfo(NeuralSparseQueryTwoPhaseInfo neuralSparseQueryTwoPhaseInfo) {
        this.neuralSparseQueryTwoPhaseInfo = neuralSparseQueryTwoPhaseInfo;
        return (QB) this;
    }

    /**
     * Copy this QueryBuilder for two phase rescorer. This function will be invoked by the search processor
     * NeuralSparseTwoPhaseProcessor which happens before the rewrite phase.
     * @param pruneRatio the parameter of the NeuralSparseTwoPhaseProcessor, control the ratio of splitting the queryTokens to two phase.
     * @param pruneType the parameter of the NeuralSparseTwoPhaseProcessor, control how to split the queryTokens to two phase.
     * @return A copy QueryBuilder for twoPhase, it will be added to the rescorer.
     */
    abstract public QB prepareTwoPhaseQuery(float pruneRatio, PruneType pruneType);

    public boolean isSparseTwoPhaseTwo() {
        return Objects.nonNull(neuralSparseQueryTwoPhaseInfo)
            && NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_TWO.equals(neuralSparseQueryTwoPhaseInfo.getStatus());
    }

    public boolean isSparseTwoPhaseOne() {
        return Objects.nonNull(neuralSparseQueryTwoPhaseInfo)
            && NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE.equals(neuralSparseQueryTwoPhaseInfo.getStatus());
    }
}
