/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.knn.index.query.parser.KNNQueryBuilderParser;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.knn.index.util.IndexUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * NeuralKNNQueryBuilder wraps KNNQueryBuilder to:
 * 1. Isolate KNN plugin API changes to a single location
 * 2. Allow extension with neural-search-specific information (e.g., query text)
 */

@Getter
public class NeuralKNNQueryBuilder extends AbstractQueryBuilder<NeuralKNNQueryBuilder> {
    private final KNNQueryBuilder knnQueryBuilder;
    private final String originalQueryText;

    /**
     * Creates a new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    public String fieldName() {
        return knnQueryBuilder.fieldName();
    }

    public int k() {
        return knnQueryBuilder.getK();
    }

    /**
     * Builder for NeuralKNNQueryBuilder.
     */
    public static class Builder {
        private String fieldName;
        private float[] vector;
        private Integer k;
        private QueryBuilder filter;
        private Float maxDistance;
        private Float minScore;
        private Boolean expandNested;
        private Map<String, ?> methodParameters;
        private RescoreContext rescoreContext;
        private String originalQueryText;

        private Builder() {}

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder vector(float[] vector) {
            this.vector = vector;
            return this;
        }

        public Builder k(Integer k) {
            this.k = k;
            return this;
        }

        public Builder filter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public Builder maxDistance(Float maxDistance) {
            this.maxDistance = maxDistance;
            return this;
        }

        public Builder minScore(Float minScore) {
            this.minScore = minScore;
            return this;
        }

        public Builder expandNested(Boolean expandNested) {
            this.expandNested = expandNested;
            return this;
        }

        public Builder methodParameters(Map<String, ?> methodParameters) {
            this.methodParameters = methodParameters;
            return this;
        }

        public Builder rescoreContext(RescoreContext rescoreContext) {
            this.rescoreContext = rescoreContext;
            return this;
        }

        public Builder originalQueryText(String originalQueryText) {
            this.originalQueryText = originalQueryText;
            return this;
        }

        public NeuralKNNQueryBuilder build() {
            KNNQueryBuilder knnBuilder = KNNQueryBuilder.builder()
                .fieldName(fieldName)
                .vector(vector)
                .k(k)
                .filter(filter)
                .maxDistance(maxDistance)
                .minScore(minScore)
                .expandNested(expandNested)
                .methodParameters(methodParameters)
                .rescoreContext(rescoreContext)
                .build();
            return new NeuralKNNQueryBuilder(knnBuilder, originalQueryText);
        }
    }

    private NeuralKNNQueryBuilder(KNNQueryBuilder knnQueryBuilder, String originalQueryText) {
        this.knnQueryBuilder = knnQueryBuilder;
        this.originalQueryText = originalQueryText;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        KNNQueryBuilderParser.streamOutput(out, knnQueryBuilder, IndexUtil::isClusterOnOrAfterMinRequiredVersion);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        knnQueryBuilder.doXContent(builder, params);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder rewritten = knnQueryBuilder.rewrite(context);
        if (rewritten == knnQueryBuilder) {
            return this;
        }
        return new NeuralKNNQueryBuilder((KNNQueryBuilder) rewritten, originalQueryText);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query knnQuery = knnQueryBuilder.toQuery(context);
        return new NeuralKNNQuery(knnQuery, originalQueryText);
    }

    @Override
    protected boolean doEquals(NeuralKNNQueryBuilder other) {
        return Objects.equals(knnQueryBuilder, other.knnQueryBuilder) && Objects.equals(originalQueryText, other.originalQueryText);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(knnQueryBuilder, originalQueryText);
    }

    @Override
    public String getWriteableName() {
        return knnQueryBuilder.getWriteableName();
    }
}
