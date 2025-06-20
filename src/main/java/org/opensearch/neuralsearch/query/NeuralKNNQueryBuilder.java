/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.knn.index.query.parser.KNNQueryBuilderParser;
import org.opensearch.knn.index.query.parser.MethodParametersParser;
import org.opensearch.knn.index.query.parser.RescoreParser;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.knn.index.util.IndexUtil;
import org.opensearch.neuralsearch.common.MinClusterVersionUtil;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.common.ParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.knn.index.query.KNNQueryBuilder.METHOD_PARAMS_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.VECTOR_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.K_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MAX_DISTANCE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MIN_SCORE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.EXPAND_NESTED_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.RESCORE_FIELD;

/**
 * NeuralKNNQueryBuilder wraps KNNQueryBuilder to:
 * 1. Isolate KNN plugin API changes to a single location
 * 2. Allow extension with neural-search-specific information (e.g., query text)
 *
 * This class provides a builder pattern for creating KNN queries with neural search capabilities,
 * allowing for vector similarity search with additional neural search context.
 */

@Getter
public class NeuralKNNQueryBuilder extends AbstractQueryBuilder<NeuralKNNQueryBuilder> implements WithFieldName {
    /**
     * The name of the query
     */
    public static final String NAME = "neural_knn";

    /**
     * The field name for the original query text
     */
    public static final String ORIGINAL_QUERY_TEXT_FIELD = "original_query_text";

    /**
     * The underlying KNN query builder that handles the vector search functionality
     */
    private final KNNQueryBuilder knnQueryBuilder;

    /**
     * The original text query that was used to generate the vector for this KNN query
     */
    private final String originalQueryText;

    /**
     * Creates a new builder instance for constructing a NeuralKNNQueryBuilder.
     *
     * @return A new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the field name that this query is searching against.
     *
     * @return The field name used in the KNN query
     */
    @Override
    public String fieldName() {
        return knnQueryBuilder.fieldName();
    }

    /**
     * Gets the number of nearest neighbors to return.
     *
     * @return The k value (number of nearest neighbors)
     */
    public int k() {
        return knnQueryBuilder.getK();
    }

    /**
     * Builder for NeuralKNNQueryBuilder.
     * Provides a fluent API for constructing NeuralKNNQueryBuilder instances with various parameters.
     */
    public static class Builder {
        /**
         * The name of the field containing vector data to search against
         */
        private String fieldName;

        /**
         * The query vector to find nearest neighbors for
         */
        private float[] vector;

        /**
         * The number of nearest neighbors to return
         */
        private Integer k;

        /**
         * Optional filter to apply to the KNN search results
         */
        private QueryBuilder filter;

        /**
         * Optional maximum distance threshold for results
         */
        private Float maxDistance;

        /**
         * Optional minimum score threshold for results
         */
        private Float minScore;

        /**
         * Whether to expand nested documents during search
         */
        private Boolean expandNested;

        /**
         * Optional parameters for the KNN method implementation
         */
        private Map<String, ?> methodParameters;

        /**
         * Optional rescore context for post-processing results
         */
        private RescoreContext rescoreContext;

        /**
         * The original text query that was used to generate the vector
         */
        private String originalQueryText;

        /**
         * Private constructor to enforce the builder pattern
         */
        private Builder() {}

        /**
         * Sets the field name to search against.
         *
         * @param fieldName The name of the field containing vector data
         * @return This builder for method chaining
         */
        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the query vector to find nearest neighbors for.
         *
         * @param vector The query vector as a float array
         * @return This builder for method chaining
         */
        public Builder vector(float[] vector) {
            this.vector = vector;
            return this;
        }

        /**
         * Sets the number of nearest neighbors to return.
         *
         * @param k The number of nearest neighbors
         * @return This builder for method chaining
         */
        public Builder k(Integer k) {
            this.k = k;
            return this;
        }

        /**
         * Sets an optional filter to apply to the KNN search results.
         *
         * @param filter The filter query
         * @return This builder for method chaining
         */
        public Builder filter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets an optional maximum distance threshold for results.
         *
         * @param maxDistance The maximum distance threshold
         * @return This builder for method chaining
         */
        public Builder maxDistance(Float maxDistance) {
            this.maxDistance = maxDistance;
            return this;
        }

        /**
         * Sets an optional minimum score threshold for results.
         *
         * @param minScore The minimum score threshold
         * @return This builder for method chaining
         */
        public Builder minScore(Float minScore) {
            this.minScore = minScore;
            return this;
        }

        /**
         * Sets whether to expand nested documents during search.
         *
         * @param expandNested Whether to expand nested documents
         * @return This builder for method chaining
         */
        public Builder expandNested(Boolean expandNested) {
            this.expandNested = expandNested;
            return this;
        }

        /**
         * Sets optional parameters for the KNN method implementation.
         *
         * @param methodParameters A map of method-specific parameters
         * @return This builder for method chaining
         */
        public Builder methodParameters(Map<String, ?> methodParameters) {
            this.methodParameters = methodParameters;
            return this;
        }

        /**
         * Sets an optional rescore context for post-processing results.
         *
         * @param rescoreContext The rescore context
         * @return This builder for method chaining
         */
        public Builder rescoreContext(RescoreContext rescoreContext) {
            this.rescoreContext = rescoreContext;
            return this;
        }

        /**
         * Sets the original text query that was used to generate the vector.
         *
         * @param originalQueryText The original text query
         * @return This builder for method chaining
         */
        public Builder originalQueryText(String originalQueryText) {
            this.originalQueryText = originalQueryText;
            return this;
        }

        /**
         * Builds a new NeuralKNNQueryBuilder with the configured parameters.
         *
         * @return A new NeuralKNNQueryBuilder instance
         */
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

    /**
     * Private constructor used by the Builder to create a NeuralKNNQueryBuilder.
     *
     * @param knnQueryBuilder The underlying KNN query builder
     * @param originalQueryText The original text query that was used to generate the vector
     */
    private NeuralKNNQueryBuilder(KNNQueryBuilder knnQueryBuilder, String originalQueryText) {
        this.knnQueryBuilder = knnQueryBuilder;
        this.originalQueryText = originalQueryText;
    }

    /**
     * Writes this query to the given output stream.
     *
     * @param out The output stream to write to
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        KNNQueryBuilderParser.streamOutput(out, knnQueryBuilder, IndexUtil::isClusterOnOrAfterMinRequiredVersion);

        if (MinClusterVersionUtil.isVersionOnOrAfterMinReqVersionForNeuralKNNQueryText(out.getVersion())) {
            out.writeOptionalString(originalQueryText);
        }
    }

    /**
     * Constructs a NeuralKNNQueryBuilder from a StreamInput.
     * Used for deserializing the query from a stream, such as during cluster communication.
     *
     * @param in The StreamInput to read from
     * @throws IOException If an I/O error occurs
     */
    public NeuralKNNQueryBuilder(StreamInput in) throws IOException {
        this.knnQueryBuilder = new KNNQueryBuilder(in);
        if (MinClusterVersionUtil.isVersionOnOrAfterMinReqVersionForNeuralKNNQueryText(in.getVersion())) {
            this.originalQueryText = in.readOptionalString();
        } else {
            this.originalQueryText = null;
        }
    }

    /**
     * Renders this query as XContent.
     *
     * @param builder The XContent builder to write to
     * @param params The parameters for rendering
     * @throws IOException If an I/O error occurs
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        knnQueryBuilder.doXContent(builder, params);
    }

    /**
     * Rewrites this query, potentially transforming it into a simpler or more efficient form.
     *
     * @param context The context for query rewriting
     * @return The rewritten query
     * @throws IOException If an I/O error occurs
     */
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder rewritten = knnQueryBuilder.rewrite(context);
        if (rewritten == knnQueryBuilder) {
            return this;
        }
        return new NeuralKNNQueryBuilder((KNNQueryBuilder) rewritten, originalQueryText);
    }

    /**
     * Converts this query builder to a Lucene query.
     *
     * @param context The shard context for query conversion
     * @return The Lucene query
     * @throws IOException If an I/O error occurs
     */
    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query knnQuery = knnQueryBuilder.toQuery(context);
        return new NeuralKNNQuery(knnQuery, originalQueryText);
    }

    /**
     * Checks if this query is equal to another NeuralKNNQueryBuilder.
     *
     * @param other The other NeuralKNNQueryBuilder to compare with
     * @return true if the queries are equal, false otherwise
     */
    @Override
    protected boolean doEquals(NeuralKNNQueryBuilder other) {
        return Objects.equals(knnQueryBuilder, other.knnQueryBuilder) && Objects.equals(originalQueryText, other.originalQueryText);
    }

    /**
     * Computes a hash code for this query.
     *
     * @return The hash code
     */
    @Override
    protected int doHashCode() {
        return Objects.hash(knnQueryBuilder, originalQueryText);
    }

    /**
     * Gets the name of this query for serialization purposes.
     *
     * @return The writeable name
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Creates NeuralKNNQueryBuilder from xContent.
     * The expected parsing form looks like:
     * {
     *   "FIELD_NAME": {
     *     "vector": [1.0, 2.0, ...],
     *     "k": 10,
     *     "filter": { ... },
     *     "max_distance": 1.0,
     *     "min_score": 0.5,
     *     "expand_nested": true,
     *     "method_parameters": { ... },
     *     "rescore": { ... },
     *     "original_query_text": "text"
     *   }
     * }
     *
     * @param parser XContentParser
     * @return NeuralKNNQueryBuilder
     * @throws IOException can be thrown by parser
     */
    public static NeuralKNNQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = parser.currentName();
        XContentParser.Token token;
        KNNQueryBuilder.Builder builder = new KNNQueryBuilder.Builder();
        String originalQueryText = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                token = parser.nextToken();

                if (VECTOR_FIELD.equals(currentFieldName)) {
                    List<Float> vector = new ArrayList<>();
                    if (token == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            vector.add(parser.floatValue());
                        }
                        float[] vectorArray = new float[vector.size()];
                        for (int i = 0; i < vector.size(); i++) {
                            vectorArray[i] = vector.get(i);
                        }
                        builder.vector(vectorArray);
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + NAME + "] vector must be an array of floats");
                    }
                } else if (K_FIELD.equals(currentFieldName)) {
                    builder.k(parser.intValue());
                } else if (FILTER_FIELD.equals(currentFieldName)) {
                    builder.filter(AbstractQueryBuilder.parseInnerQueryBuilder(parser));
                } else if (MAX_DISTANCE_FIELD.equals(currentFieldName)) {
                    builder.maxDistance(parser.floatValue());
                } else if (MIN_SCORE_FIELD.equals(currentFieldName)) {
                    builder.minScore(parser.floatValue());
                } else if (EXPAND_NESTED_FIELD.equals(currentFieldName)) {
                    builder.expandNested(parser.booleanValue());
                } else if (METHOD_PARAMS_FIELD.equals(currentFieldName)) {
                    builder.methodParameters(MethodParametersParser.fromXContent(parser));
                } else if (RESCORE_FIELD.equals(currentFieldName)) {
                    builder.rescoreContext(RescoreParser.fromXContent(parser));
                } else if (ORIGINAL_QUERY_TEXT_FIELD.equals(currentFieldName)) {
                    originalQueryText = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            }
        }

        KNNQueryBuilder knnBuilder = builder.build();
        return new NeuralKNNQueryBuilder(knnBuilder, originalQueryText);
    }
}
