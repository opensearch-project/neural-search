/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static org.opensearch.knn.index.query.KNNQueryBuilder.EXPAND_NESTED_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MAX_DISTANCE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.METHOD_PARAMS_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MIN_SCORE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.RESCORE_FIELD;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersion;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForRadialSearch;
import static org.opensearch.neuralsearch.common.VectorUtil.vectorAsListToArray;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_IMAGE;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_TEXT;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.common.SetOnce;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.query.parser.MethodParametersParser;
import org.opensearch.knn.index.query.parser.RescoreParser;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.common.MinClusterVersionUtil;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.MapInferenceRequest;

/**
 * NeuralQueryBuilder is responsible for producing "neural" query types. A "neural" query type is a wrapper around a
 * k-NN vector query. It uses a ML language model to produce a dense vector from a query string that is then used as
 * the query vector for the k-NN search.
 */

@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class NeuralQueryBuilder extends AbstractQueryBuilder<NeuralQueryBuilder> implements ModelInferenceQueryBuilder, WithFieldName {

    public static final String NAME = "neural";

    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");

    public static final ParseField QUERY_IMAGE_FIELD = new ParseField("query_image");

    public static final ParseField MODEL_ID_FIELD = new ParseField("model_id");

    @VisibleForTesting
    static final ParseField K_FIELD = new ParseField("k");

    private static final int DEFAULT_K = 10;

    private static MLCommonsClientAccessor ML_CLIENT;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralQueryBuilder.ML_CLIENT = mlClient;
    }

    private String fieldName;
    private String queryText;
    private String queryImage;
    private String modelId;
    private Integer k = null;
    private Float maxDistance = null;
    private Float minScore = null;
    private Boolean expandNested;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private Supplier<float[]> vectorSupplier;
    private QueryBuilder filter;
    private Map<String, ?> methodParameters;
    private RescoreContext rescoreContext;

    /**
     * A custom builder class to enforce valid Neural Query Builder instantiation
     */
    public static class Builder {
        private String fieldName;
        private String queryText;
        private String queryImage;
        private String modelId;
        private Integer k = null;
        private Float maxDistance = null;
        private Float minScore = null;
        private Boolean expandNested;
        private Supplier<float[]> vectorSupplier;
        private QueryBuilder filter;
        private Map<String, ?> methodParameters;
        private RescoreContext rescoreContext;
        private String queryName;
        private float boost = DEFAULT_BOOST;

        public Builder() {}

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder queryText(String queryText) {
            this.queryText = queryText;
            return this;
        }

        public Builder queryImage(String queryImage) {
            this.queryImage = queryImage;
            return this;
        }

        public Builder modelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder k(Integer k) {
            this.k = k;
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

        public Builder vectorSupplier(Supplier<float[]> vectorSupplier) {
            this.vectorSupplier = vectorSupplier;
            return this;
        }

        public Builder filter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public Builder methodParameters(Map<String, ?> methodParameters) {
            this.methodParameters = methodParameters;
            return this;
        }

        public Builder queryName(String queryName) {
            this.queryName = queryName;
            return this;
        }

        public Builder boost(float boost) {
            this.boost = boost;
            return this;
        }

        public Builder rescoreContext(RescoreContext rescoreContext) {
            this.rescoreContext = rescoreContext;
            return this;
        }

        public NeuralQueryBuilder build() {
            validateQueryParameters(fieldName, queryText, queryImage);
            boolean queryTypeIsProvided = validateKNNQueryType(k, maxDistance, minScore);
            if (queryTypeIsProvided == false) {
                k = DEFAULT_K;
            }
            return new NeuralQueryBuilder(
                fieldName,
                queryText,
                queryImage,
                modelId,
                k,
                maxDistance,
                minScore,
                expandNested,
                vectorSupplier,
                filter,
                methodParameters,
                rescoreContext
            ).boost(boost).queryName(queryName);
        }

    }

    public static NeuralQueryBuilder.Builder builder() {
        return new NeuralQueryBuilder.Builder();
    }

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public NeuralQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        // The query image field was introduced since v2.11.0 through the
        // https://github.com/opensearch-project/neural-search/pull/359 but at that time we didn't add it to
        // NeuralQueryBuilder(StreamInput in) and doWriteTo(StreamOutput out) function. The fix will be
        // introduced in v2.19.0 so we need this check for the backward compatibility.
        if (isClusterOnOrAfterMinReqVersion(QUERY_IMAGE_FIELD.getPreferredName())) {
            this.queryText = in.readOptionalString();
            this.queryImage = in.readOptionalString();
        } else {
            this.queryText = in.readString();
        }
        // If cluster version is on or after 2.11 then default model Id support is enabled
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            this.modelId = in.readOptionalString();
        } else {
            this.modelId = in.readString();
        }
        if (isClusterOnOrAfterMinReqVersionForRadialSearch()) {
            this.k = in.readOptionalInt();
        } else {
            this.k = in.readVInt();
        }
        this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (isClusterOnOrAfterMinReqVersionForRadialSearch()) {
            this.maxDistance = in.readOptionalFloat();
            this.minScore = in.readOptionalFloat();
        }
        if (isClusterOnOrAfterMinReqVersion(EXPAND_NESTED_FIELD.getPreferredName())) {
            this.expandNested = in.readOptionalBoolean();
        }
        if (isClusterOnOrAfterMinReqVersion(METHOD_PARAMS_FIELD.getPreferredName())) {
            this.methodParameters = MethodParametersParser.streamInput(in, MinClusterVersionUtil::isClusterOnOrAfterMinReqVersion);
        }
        this.rescoreContext = RescoreParser.streamInput(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        // The query image field was introduced since v2.11.0 through the
        // https://github.com/opensearch-project/neural-search/pull/359 but at that time we didn't add it to
        // NeuralQueryBuilder(StreamInput in) and doWriteTo(StreamOutput out) function. The fix will be
        // introduced in v2.19.0 so we need this check for the backward compatibility.
        if (isClusterOnOrAfterMinReqVersion(QUERY_IMAGE_FIELD.getPreferredName())) {
            out.writeOptionalString(this.queryText);
            out.writeOptionalString(this.queryImage);
        } else {
            out.writeString(this.queryText);
        }
        // If cluster version is on or after 2.11 then default model Id support is enabled
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            out.writeOptionalString(this.modelId);
        } else {
            out.writeString(this.modelId);
        }
        if (isClusterOnOrAfterMinReqVersionForRadialSearch()) {
            out.writeOptionalInt(this.k);
        } else {
            out.writeVInt(this.k);
        }
        out.writeOptionalNamedWriteable(this.filter);
        if (isClusterOnOrAfterMinReqVersionForRadialSearch()) {
            out.writeOptionalFloat(this.maxDistance);
            out.writeOptionalFloat(this.minScore);
        }
        if (isClusterOnOrAfterMinReqVersion(EXPAND_NESTED_FIELD.getPreferredName())) {
            out.writeOptionalBoolean(this.expandNested);
        }

        if (isClusterOnOrAfterMinReqVersion(METHOD_PARAMS_FIELD.getPreferredName())) {
            MethodParametersParser.streamOutput(out, methodParameters, MinClusterVersionUtil::isClusterOnOrAfterMinReqVersion);
        }
        RescoreParser.streamOutput(out, rescoreContext);
    }

    /**
     * Add a filter to Neural Query Builder
     * @param filterToBeAdded filter to be added
     * @return return itself with underlying filter combined with passed in filter
     */
    public QueryBuilder filter(QueryBuilder filterToBeAdded) {
        if (validateFilterParams(filterToBeAdded) == false) {
            return this;
        }
        if (filter == null) {
            filter = filterToBeAdded;
        } else {
            filter = filter.filter(filterToBeAdded);
        }
        return this;

    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        if (Objects.nonNull(queryText)) {
            xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        }
        if (Objects.nonNull(queryImage)) {
            xContentBuilder.field(QUERY_IMAGE_FIELD.getPreferredName(), queryImage);
        }
        if (Objects.nonNull(modelId)) {
            xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        }
        if (Objects.nonNull(k)) {
            xContentBuilder.field(K_FIELD.getPreferredName(), k);
        }
        if (Objects.nonNull(filter)) {
            xContentBuilder.field(FILTER_FIELD.getPreferredName(), filter);
        }
        if (Objects.nonNull(maxDistance)) {
            xContentBuilder.field(MAX_DISTANCE_FIELD.getPreferredName(), maxDistance);
        }
        if (Objects.nonNull(minScore)) {
            xContentBuilder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        if (Objects.nonNull(expandNested)) {
            xContentBuilder.field(EXPAND_NESTED_FIELD.getPreferredName(), expandNested);
        }
        if (Objects.nonNull(methodParameters)) {
            MethodParametersParser.doXContent(xContentBuilder, methodParameters);
        }
        if (Objects.nonNull(rescoreContext)) {
            RescoreParser.doXContent(xContentBuilder, rescoreContext);
        }
        printBoostAndQueryName(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    /**
     * Creates NeuralQueryBuilder from xContent.
     *
     * The expected parsing form looks like:
     * {
     *  "VECTOR_FIELD": {
     *    "query_text": "string",
     *    "model_id": "string",
     *    "k": int,
     *    "name": "string", (optional)
     *    "boost": float (optional),
     *    "filter": map (optional)
     *  }
     * }
     *
     * @param parser XContentParser
     * @return NeuralQueryBuilder
     * @throws IOException can be thrown by parser
     */
    public static NeuralQueryBuilder fromXContent(XContentParser parser) throws IOException {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Token must be START_OBJECT");
        }
        parser.nextToken();
        neuralQueryBuilder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, neuralQueryBuilder);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "["
                    + NAME
                    + "] query doesn't support multiple fields, found ["
                    + neuralQueryBuilder.fieldName()
                    + "] and ["
                    + parser.currentName()
                    + "]"
            );
        }
        validateQueryParameters(neuralQueryBuilder.fieldName(), neuralQueryBuilder.queryText(), neuralQueryBuilder.queryImage());
        if (!isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            requireValue(neuralQueryBuilder.modelId(), "Model ID must be provided for neural query");
        }

        boolean queryTypeIsProvided = validateKNNQueryType(
            neuralQueryBuilder.k(),
            neuralQueryBuilder.maxDistance(),
            neuralQueryBuilder.minScore()
        );
        if (queryTypeIsProvided == false) {
            neuralQueryBuilder.k(DEFAULT_K);
        }

        return neuralQueryBuilder;
    }

    private static void parseQueryParams(XContentParser parser, NeuralQueryBuilder neuralQueryBuilder) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.queryText(parser.text());
                } else if (QUERY_IMAGE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.queryImage(parser.text());
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.modelId(parser.text());
                } else if (K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.k((Integer) NumberFieldMapper.NumberType.INTEGER.parse(parser.objectBytes(), false));
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.boost(parser.floatValue());
                } else if (MAX_DISTANCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.maxDistance(parser.floatValue());
                } else if (MIN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.minScore(parser.floatValue());
                } else if (EXPAND_NESTED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.expandNested(parser.booleanValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.filter(parseInnerQueryBuilder(parser));
                } else if (METHOD_PARAMS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.methodParameters(MethodParametersParser.fromXContent(parser));
                } else if (RESCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.rescoreContext(RescoreParser.fromXContent(parser));
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // When re-writing a QueryBuilder, if the QueryBuilder is not changed, doRewrite should return itself
        // (see
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/index/query/QueryBuilder.java#L90-L98).
        // Otherwise, it should return the modified copy (see rewrite logic
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/index/query/Rewriteable.java#L117.
        // With the asynchronous call, on first rewrite, we create a new
        // vector supplier that will get populated once the asynchronous call finishes and pass this supplier in to
        // create a new builder. Once the supplier's value gets set, we return a NeuralKNNQueryBuilder
        // which wrapped KNNQueryBuilder. Otherwise, we just return the current unmodified query builder.
        if (vectorSupplier() != null) {
            if (vectorSupplier().get() == null) {
                return this;
            }

            return NeuralKNNQueryBuilder.builder()
                .fieldName(fieldName())
                .vector(vectorSupplier.get())
                .k(k())
                .filter(filter())
                .maxDistance(maxDistance())
                .minScore(minScore())
                .expandNested(expandNested())
                .methodParameters(methodParameters())
                .rescoreContext(rescoreContext())
                .originalQueryText(queryText())
                .build();
        }

        SetOnce<float[]> vectorSetOnce = new SetOnce<>();
        Map<String, String> inferenceInput = new HashMap<>();
        if (StringUtils.isNotBlank(queryText())) {
            inferenceInput.put(INPUT_TEXT, queryText());
        }
        if (StringUtils.isNotBlank(queryImage())) {
            inferenceInput.put(INPUT_IMAGE, queryImage());
        }
        queryRewriteContext.registerAsyncAction(
            ((client, actionListener) -> ML_CLIENT.inferenceSentencesMap(
                MapInferenceRequest.builder().modelId(modelId()).inputObjects(inferenceInput).build(),
                ActionListener.wrap(floatList -> {
                    vectorSetOnce.set(vectorAsListToArray(floatList));
                    actionListener.onResponse(null);
                }, actionListener::onFailure)
            ))
        );
        return new NeuralQueryBuilder(
            fieldName(),
            queryText(),
            queryImage(),
            modelId(),
            k(),
            maxDistance(),
            minScore(),
            expandNested(),
            vectorSetOnce::get,
            filter(),
            methodParameters(),
            rescoreContext()
        );
    }

    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) {
        // All queries should be generated by the k-NN Query Builder
        throw new UnsupportedOperationException("Query cannot be created by NeuralQueryBuilder directly");
    }

    @Override
    protected boolean doEquals(NeuralQueryBuilder obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(fieldName, obj.fieldName);
        equalsBuilder.append(queryText, obj.queryText);
        equalsBuilder.append(queryImage, obj.queryImage);
        equalsBuilder.append(modelId, obj.modelId);
        equalsBuilder.append(k, obj.k);
        equalsBuilder.append(maxDistance, obj.maxDistance);
        equalsBuilder.append(minScore, obj.minScore);
        equalsBuilder.append(expandNested, obj.expandNested);
        equalsBuilder.append(getVector(vectorSupplier), getVector(obj.vectorSupplier));
        equalsBuilder.append(filter, obj.filter);
        equalsBuilder.append(methodParameters, obj.methodParameters);
        equalsBuilder.append(rescoreContext, obj.rescoreContext);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            fieldName,
            queryText,
            queryImage,
            modelId,
            k,
            maxDistance,
            minScore,
            expandNested,
            Arrays.hashCode(getVector(vectorSupplier)),
            filter,
            methodParameters,
            rescoreContext
        );
    }

    private float[] getVector(final Supplier<float[]> vectorSupplier) {
        return Objects.isNull(vectorSupplier) ? null : vectorSupplier.get();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static void validateQueryParameters(String fieldName, String queryText, String queryImage) {
        if (StringUtils.isBlank(queryText) && StringUtils.isBlank(queryImage)) {
            throw new IllegalArgumentException("Either query text or image text must be provided for neural query");
        }
        requireValue(fieldName, "Field name must be provided for neural query");
    }

    private static boolean validateKNNQueryType(Integer k, Float maxDistance, Float minScore) {
        int queryCount = 0;
        if (k != null) {
            queryCount++;
        }
        if (maxDistance != null) {
            queryCount++;
        }
        if (minScore != null) {
            queryCount++;
        }
        if (queryCount > 1) {
            throw new IllegalArgumentException("Only one of k, max_distance, or min_score can be provided");
        }
        return queryCount == 1;
    }

    /**
     * Gets the field name that this query is searching against.
     *
     * @return The field name used in the Neural query
     */
    @Override
    public String fieldName() {
        return this.fieldName;
    }
}
