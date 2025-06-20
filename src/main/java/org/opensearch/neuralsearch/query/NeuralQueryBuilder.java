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
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_SEMANTIC_FIELD;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersion;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForDefaultDenseModelIdSupport;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForRadialSearch;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForSemanticFieldType;
import static org.opensearch.neuralsearch.common.VectorUtil.vectorAsListToArray;
import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_FIELD_NAME;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_IMAGE;
import static org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor.INPUT_TEXT;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.modelIdToQueryTokensSupplierMapStreamInput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.modelIdToQueryTokensSupplierMapStreamOutput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.modelIdToVectorSupplierMapStreamInput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.modelIdToVectorSupplierMapStreamOutput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.queryTokensMapSupplierStreamInput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.queryTokensMapSupplierStreamOutput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.vectorSupplierStreamInput;
import static org.opensearch.neuralsearch.query.parser.NeuralQueryParser.vectorSupplierStreamOutput;
import static org.opensearch.neuralsearch.util.NeuralQueryValidationUtil.validateNeuralQueryForKnn;
import static org.opensearch.neuralsearch.util.NeuralQueryValidationUtil.validateNeuralQueryForSemanticDense;
import static org.opensearch.neuralsearch.util.NeuralQueryValidationUtil.validateNeuralQueryForSemanticSparse;
import static org.opensearch.neuralsearch.util.NeuralQueryValidationUtil.validateTargetFieldConfig;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getIndexToTargetFieldConfigMapFromIndexMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.NonNull;
import org.apache.commons.lang.StringUtils;
import org.opensearch.core.common.Strings;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.IndicesRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SetOnce;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.RankFeaturesFieldMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.query.parser.MethodParametersParser;
import org.opensearch.knn.index.query.parser.RescoreParser;
import org.opensearch.knn.index.query.rescore.RescoreContext;
import org.opensearch.neuralsearch.common.MinClusterVersionUtil;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.query.dto.NeuralQueryBuildStage;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

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
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.query.dto.NeuralQueryTargetFieldConfig;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.transport.RemoteClusterService;

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

    // common fields used for both dense and sparse model
    @VisibleForTesting
    public static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");

    public static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    public static final ParseField SEMANTIC_FIELD_SEARCH_ANALYZER_FIELD = new ParseField("semantic_field_search_analyzer");

    // fields only used for dense model
    public static final ParseField QUERY_IMAGE_FIELD = new ParseField("query_image");

    @VisibleForTesting
    static final ParseField K_FIELD = new ParseField("k");

    public static final int DEFAULT_K = 10;
    public static final Set<String> SUPPORTED_TARGET_FIELD_TYPES = Set.of(
        SemanticFieldMapper.CONTENT_TYPE,
        KNNVectorFieldMapper.CONTENT_TYPE
    );

    // fields for sparse model
    public static final ParseField QUERY_TOKENS_FIELD = new ParseField("query_tokens");

    // client to invoke ml-common APIs
    private static MLCommonsClientAccessor ML_CLIENT;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralQueryBuilder.ML_CLIENT = mlClient;
    }

    // common fields used for both dense and sparse model
    private String fieldName;
    private String queryText;
    private String modelId;
    private String embeddingFieldType;

    // fields only used for dense model
    private String queryImage;
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
    // fields to support the semantic field for dense model
    private Map<String, Supplier<float[]>> modelIdToVectorSupplierMap;

    // fields only used for sparse model
    private Supplier<Map<String, Float>> queryTokensMapSupplier;
    // fields to support the semantic field for sparse model
    private Map<String, Supplier<Map<String, Float>>> modelIdToQueryTokensSupplierMap;
    private String searchAnalyzer;

    /**
     * A custom builder class to enforce valid Neural Query Builder instantiation
     */
    public static class Builder {
        private String fieldName;
        private String queryText;
        private String queryImage;
        private String modelId;
        private String searchAnalyzer;
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
        private String embeddingFieldType;
        private Map<String, Supplier<float[]>> modelIdToVectorSupplierMap;
        private Supplier<Map<String, Float>> queryTokensMapSupplier;
        private Map<String, Supplier<Map<String, Float>>> modelIdToQueryTokensSupplierMap;
        private Boolean isSemanticField = false;
        private NeuralQueryBuildStage buildStage;

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

        public Builder embeddingFieldType(String embeddingFieldType) {
            this.embeddingFieldType = embeddingFieldType;
            return this;
        }

        public Builder modelIdToVectorSupplierMap(Map<String, Supplier<float[]>> modelIdToVectorSupplierMap) {
            this.modelIdToVectorSupplierMap = modelIdToVectorSupplierMap;
            return this;
        }

        public Builder queryTokensMapSupplier(Supplier<Map<String, Float>> queryTokensMapSupplier) {
            this.queryTokensMapSupplier = queryTokensMapSupplier;
            return this;
        }

        public Builder modelIdToQueryTokensSupplierMap(Map<String, Supplier<Map<String, Float>>> modelIdToQueryTokensSupplierMap) {
            this.modelIdToQueryTokensSupplierMap = modelIdToQueryTokensSupplierMap;
            return this;
        }

        public Builder isSemanticField(Boolean isSemanticField) {
            this.isSemanticField = isSemanticField;
            return this;
        }

        public Builder buildStage(NeuralQueryBuildStage buildStage) {
            this.buildStage = buildStage;
            return this;
        }

        public Builder searchAnalyzer(String searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public NeuralQueryBuilder build() {
            requireValue(fieldName, "Field name must be provided for neural query");

            final NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
                fieldName,
                queryText,
                modelId,
                embeddingFieldType,
                queryImage,
                k,
                maxDistance,
                minScore,
                expandNested,
                vectorSupplier,
                filter,
                methodParameters,
                rescoreContext,
                modelIdToVectorSupplierMap,
                queryTokensMapSupplier,
                modelIdToQueryTokensSupplierMap,
                searchAnalyzer

            ).boost(boost).queryName(queryName);

            List<String> errors = new ArrayList<>();

            // Try to build the neural query builder in fromXContent function
            if (buildStage == null || NeuralQueryBuildStage.FROM_X_CONTENT.equals(buildStage)) {
                // Before semantic field we only support query against a knn field so validate for that case
                // If we can support the semantic field we will delay the validation to COORDINATE_REWRITE where we
                // have more info to do the validation.
                if (isClusterOnOrAfterMinReqVersionForSemanticFieldType() == false) {
                    errors = validateNeuralQueryForKnn(neuralQueryBuilder, buildStage);
                }
            } else if (NeuralQueryBuildStage.REWRITE.equals(buildStage)) {
                if (isSemanticField == null || isSemanticField == false) {
                    // Currently if the target field is not a semantic field we will assume the target field should be a knn
                    // field. In the future we can add the support for the case when the target field is a rank_features.
                    errors = validateNeuralQueryForKnn(neuralQueryBuilder, buildStage);
                } else if (KNNVectorFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
                    errors = validateNeuralQueryForSemanticDense(neuralQueryBuilder);
                } else if (RankFeaturesFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
                    errors = validateNeuralQueryForSemanticSparse(neuralQueryBuilder);
                } else {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Unsupported embedding field type: %s", embeddingFieldType)
                    );
                }
            }

            if (errors.isEmpty() == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Failed to build the NeuralQueryBuilder: %s", String.join("; ", errors))
                );
            } else {
                return neuralQueryBuilder;
            }
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
        if (isClusterOnOrAfterMinReqVersionForDefaultDenseModelIdSupport()) {
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
        if (in.getVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_SEMANTIC_FIELD)) {
            this.vectorSupplier = vectorSupplierStreamInput(in);
            this.queryTokensMapSupplier = queryTokensMapSupplierStreamInput(in);
            this.modelIdToVectorSupplierMap = modelIdToVectorSupplierMapStreamInput(in);
            this.modelIdToQueryTokensSupplierMap = modelIdToQueryTokensSupplierMapStreamInput(in);
            this.searchAnalyzer = in.readOptionalString();
        }
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
        if (isClusterOnOrAfterMinReqVersionForDefaultDenseModelIdSupport()) {
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

        if (out.getVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_SEMANTIC_FIELD)) {
            vectorSupplierStreamOutput(out, vectorSupplier);
            queryTokensMapSupplierStreamOutput(out, queryTokensMapSupplier);
            modelIdToVectorSupplierMapStreamOutput(out, modelIdToVectorSupplierMap);
            modelIdToQueryTokensSupplierMapStreamOutput(out, modelIdToQueryTokensSupplierMap);
            out.writeOptionalString(this.searchAnalyzer);
        }
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
        if (Objects.nonNull(queryTokensMapSupplier) && Objects.nonNull(queryTokensMapSupplier.get())) {
            xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), queryTokensMapSupplier.get());
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
        EventStatsManager.increment(EventStatName.NEURAL_QUERY_REQUESTS);
        final Builder builder = new Builder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Token must be START_OBJECT");
        }
        parser.nextToken();
        builder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, builder);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + NAME + "] query doesn't support multiple fields, found [" + builder.fieldName + "] and [" + parser.currentName() + "]"
            );
        }

        builder.buildStage(NeuralQueryBuildStage.FROM_X_CONTENT);

        return builder.build();
    }

    private static void parseQueryParams(XContentParser parser, Builder builder) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.queryText(parser.text());
                } else if (QUERY_IMAGE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.queryImage(parser.text());
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.modelId(parser.text());
                } else if (K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.k((Integer) NumberFieldMapper.NumberType.INTEGER.parse(parser.objectBytes(), false));
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.boost(parser.floatValue());
                } else if (MAX_DISTANCE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.maxDistance(parser.floatValue());
                } else if (MIN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.minScore(parser.floatValue());
                } else if (EXPAND_NESTED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.expandNested(parser.booleanValue());
                } else if (SEMANTIC_FIELD_SEARCH_ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.searchAnalyzer(parser.text());
                } else {
                    throw getUnsupportedFieldException(parser.getTokenLocation(), currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.filter(parseInnerQueryBuilder(parser));
                } else if (METHOD_PARAMS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.methodParameters(MethodParametersParser.fromXContent(parser));
                } else if (RESCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    builder.rescoreContext(RescoreParser.fromXContent(parser));
                } else if (QUERY_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (isClusterOnOrAfterMinReqVersionForSemanticFieldType()) {
                        final Map<String, Float> queryTokens = parser.map(HashMap::new, XContentParser::floatValue);
                        builder.queryTokensMapSupplier(() -> queryTokens);
                    } else {
                        throw getUnsupportedFieldException(parser.getTokenLocation(), currentFieldName);
                    }
                } else {
                    throw getUnsupportedFieldException(parser.getTokenLocation(), currentFieldName);
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
    }

    private static ParsingException getUnsupportedFieldException(XContentLocation tokenLocation, String currentFieldName) {
        return new ParsingException(tokenLocation, "[" + NAME + "] query does not support [" + currentFieldName + "]");
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // For backward compatibility
        if (isClusterOnOrAfterMinReqVersionForSemanticFieldType() == false) {
            return rewriteQueryAgainstKnnField(queryRewriteContext);
        }

        final QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (coordinatorContext != null) {
            return rewriteQueryWithCoordinatorContext(coordinatorContext);
        }

        final QueryShardContext queryShardContext = queryRewriteContext.convertToShardContext();
        if (queryShardContext != null) {
            return rewriteQueryWithQueryShardContext(queryShardContext);
        }

        // We can rewrite with a BaseQueryRewriteContext on the shard level:
        // In SearchService do a quick rewrite to handle AliasFilters rewrite. This happens after the
        // coordinate rewrite, and we do not need to rewrite the neural query for this case. We will do rewrite later
        // with queryShardContext.
        return this;
    }

    private QueryBuilder rewriteQueryWithCoordinatorContext(@NonNull final QueryCoordinatorContext queryRewriteContext) {
        final IndicesRequest searchRequest = queryRewriteContext.getSearchRequest();
        final List<String> remoteIndices = getRemoteIndices(searchRequest);
        // If we have remote target indices in the search request we assume the target field should be a knn field
        // for backward compatibility. For semantic field we do not support cross cluster search for now.
        if (remoteIndices.isEmpty() == false) {
            return rewriteQueryAgainstKnnField(queryRewriteContext);
        }

        final Map<String, NeuralQueryTargetFieldConfig> indexToTargetFieldConfigMap = getIndexToTargetFieldConfigMap(searchRequest);
        final NeuralQueryTargetFieldConfig firstTargetFieldConfig = getFirstTargetFieldConfig(indexToTargetFieldConfigMap);
        // If no target field config found or the target field is not a semantic field then fall back to the old logic.
        // Here we just need to check the target field config in one index since we already validate the target field
        // in all the target indices should all be semantic field or non-semantic field.
        if (firstTargetFieldConfig == null || Boolean.TRUE.equals(firstTargetFieldConfig.getIsSemanticField()) == false) {
            // When the target field is not a semantic field we simply fall back to the old logic to support the
            // dense model. But in the future we can add the logic to also support the sparse model.
            return rewriteQueryAgainstKnnField(queryRewriteContext);
        } else {
            // If it is not null it means we already start the async actions in the previous rewrite.
            // Current rewrite happens after all the async actions done so simply return this to end the rewrite.
            if (modelIdToVectorSupplierMap != null
                || modelIdToQueryTokensSupplierMap != null
                || queryTokensMapSupplier != null
                || searchAnalyzer != null) {
                // If we only have one target index it means we know the path to the target embedding feild so we can
                // continue the rewrite
                if (indexToTargetFieldConfigMap.size() == 1) {
                    return rewriteQueryForSemanticField(firstTargetFieldConfig);
                }
                // If we have more than one target index we need QueryShardContext to know the target index so simply
                // return this to end the rewrite. Will do rewrite later on the shard level with QueryShardContext.
                return this;
            }

            final Set<String> modelIds = indexToTargetFieldConfigMap.values()
                .stream()
                .map(NeuralQueryTargetFieldConfig::getSearchModelId)
                .collect(Collectors.toSet());

            return inferenceForSemanticField(queryRewriteContext, modelIds, firstTargetFieldConfig.getEmbeddingFieldType());
        }
    }

    private QueryBuilder rewriteQueryForSemanticField(@NonNull final NeuralQueryTargetFieldConfig targetFieldConfig) {
        final String searchModelId = getSearchModelId(targetFieldConfig);
        final String semanticFieldSearchAnalyzer = getSearchAnalyzer(targetFieldConfig);
        final Boolean chunkingEnabled = targetFieldConfig.getChunkingEnabled();
        final String embeddingFieldType = targetFieldConfig.getEmbeddingFieldType();
        final String embeddingFieldPath = targetFieldConfig.getEmbeddingFieldPath();
        final String chunksPath = targetFieldConfig.getChunksPath();

        if (KNNVectorFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            EventStatsManager.increment(EventStatName.NEURAL_QUERY_AGAINST_SEMANTIC_DENSE_REQUESTS);
            if (modelIdToVectorSupplierMap == null
                || modelIdToVectorSupplierMap.get(searchModelId) == null
                || modelIdToVectorSupplierMap.get(searchModelId).get() == null) {
                throw new RuntimeException(
                    getErrorMessageWithBaseErrorForSemantic("Not able to find the dense embedding when try to rewrite it to the KNN query.")
                );
            }
            final float[] vector = modelIdToVectorSupplierMap.get(searchModelId).get();
            final QueryBuilder knnQueryBuilder = createKNNQueryBuilder(embeddingFieldPath, vector);
            if (Boolean.TRUE.equals(chunkingEnabled)) {
                return new NestedQueryBuilder(chunksPath, knnQueryBuilder, ScoreMode.Max);
            } else {
                return knnQueryBuilder;
            }
        } else if (RankFeaturesFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            EventStatsManager.increment(EventStatName.NEURAL_QUERY_AGAINST_SEMANTIC_SPARSE_REQUESTS);
            Supplier<Map<String, Float>> queryTokensSupplier = queryTokensMapSupplier;
            // If the raw token is not provided or no search analyzer provided
            // then try to find the token generated by the ml model
            if (queryTokensSupplier == null && searchAnalyzer == null && semanticFieldSearchAnalyzer == null) {
                if (modelIdToQueryTokensSupplierMap == null || modelIdToQueryTokensSupplierMap.get(searchModelId) == null) {
                    throw new RuntimeException(
                        getErrorMessageWithBaseErrorForSemantic(
                            "Not able to find the sparse embedding when try to rewrite it neural sparse query."
                        )
                    );
                }
                queryTokensSupplier = modelIdToQueryTokensSupplierMap.get(searchModelId);
            }

            NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().fieldName(embeddingFieldPath);
            if (Strings.isNullOrEmpty(semanticFieldSearchAnalyzer) == false) {
                neuralSparseQueryBuilder = neuralSparseQueryBuilder.analyzer(this.searchAnalyzer).queryText(this.queryText);
            } else {
                neuralSparseQueryBuilder = neuralSparseQueryBuilder.queryTokensSupplier(queryTokensSupplier);
            }
            if (Boolean.TRUE.equals(chunkingEnabled)) {
                return new NestedQueryBuilder(chunksPath, neuralSparseQueryBuilder, ScoreMode.Max);
            } else {
                return neuralSparseQueryBuilder;
            }
        } else {
            throw new RuntimeException(
                getErrorMessageWithBaseErrorForSemantic(
                    "Expect the embedding field type to be knn_vector or ran_features but found unsupported embedding field type: "
                        + embeddingFieldType
                )
            );
        }
    }

    private Map<String, NeuralQueryTargetFieldConfig> getIndexToTargetFieldConfigMap(@NonNull final IndicesRequest searchRequest) {
        final List<IndexMetadata> targetIndexMetadataList = NeuralSearchClusterUtil.instance().getIndexMetadataList(searchRequest);
        final Map<String, NeuralQueryTargetFieldConfig> indexToTargetFieldConfigMap = getIndexToTargetFieldConfigMapFromIndexMetadata(
            fieldName,
            targetIndexMetadataList
        );
        validateTargetFieldConfig(fieldName, indexToTargetFieldConfigMap);
        return indexToTargetFieldConfigMap;
    }

    private List<String> getRemoteIndices(@NonNull final IndicesRequest searchRequest) {
        return Arrays.stream(searchRequest.indices())
            .filter(index -> index.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0)
            .collect(Collectors.toList());
    }

    private NeuralQueryTargetFieldConfig getFirstTargetFieldConfig(
        @NonNull final Map<String, NeuralQueryTargetFieldConfig> indexToTargetFieldConfigMap
    ) {
        final Set<String> targetIndices = indexToTargetFieldConfigMap.keySet();
        if (targetIndices.isEmpty()) {
            return null;
        }
        return indexToTargetFieldConfigMap.get(targetIndices.iterator().next());
    }

    private QueryBuilder rewriteQueryAgainstKnnField(QueryRewriteContext queryRewriteContext) {
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
            EventStatsManager.increment(EventStatName.NEURAL_QUERY_AGAINST_KNN_REQUESTS);
            return createKNNQueryBuilder(fieldName(), vectorSupplier.get());
        }

        SetOnce<float[]> vectorSetOnce = new SetOnce<>();
        Map<String, String> inferenceInput = new HashMap<>();
        // build first to leverage the validation in the build function
        final NeuralQueryBuilder neuralQueryBuilder = createNeuralQueryBuilder(
            KNNVectorFieldMapper.CONTENT_TYPE,
            vectorSetOnce::get,
            false
        );

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

        return neuralQueryBuilder;
    }

    private QueryBuilder createKNNQueryBuilder(String fieldName, float[] vector) {
        // Check if cluster supports NeuralKNNQueryBuilder (introduced in 3.0.0)
        if (MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForNeuralKNNQueryBuilder()) {
            // Use NeuralKNNQueryBuilder for version 3.0.0 and later
            NeuralKNNQueryBuilder.Builder builder = NeuralKNNQueryBuilder.builder()
                .fieldName(fieldName)
                .vector(vector)
                .filter(filter())
                .expandNested(expandNested())
                .methodParameters(methodParameters())
                .rescoreContext(rescoreContext())
                .originalQueryText(queryText());

            // Only set k if it's not a radial search (i.e., when minScore and maxDistance are null)
            if (minScore() == null && maxDistance() == null) {
                builder.k(k());
            }

            // Set radial search parameters if present
            builder.maxDistance(maxDistance());
            builder.minScore(minScore());

            return builder.build();
        } else {
            // For versions before 3.0.0 (like 2.19.0), return NeuralQueryBuilder
            // to maintain backward compatibility during rolling upgrades
            return createNeuralQueryBuilder(KNNVectorFieldMapper.CONTENT_TYPE, () -> vector, false);
        }
    }

    private NeuralQueryBuilder createNeuralQueryBuilder(
        String embeddingFieldType,
        Supplier<float[]> vectorSupplier,
        boolean isSemanticField
    ) {
        return NeuralQueryBuilder.builder()
            .fieldName(fieldName())
            .queryText(queryText())
            .modelId(modelId())
            .embeddingFieldType(embeddingFieldType)
            .queryImage(queryImage())
            .k(k())
            .maxDistance(maxDistance())
            .minScore(minScore())
            .expandNested(expandNested())
            .vectorSupplier(vectorSupplier)
            .filter(filter())
            .methodParameters(methodParameters())
            .rescoreContext(rescoreContext())
            .isSemanticField(isSemanticField)
            .buildStage(NeuralQueryBuildStage.REWRITE)
            .queryTokensMapSupplier(queryTokensMapSupplier())
            .modelIdToQueryTokensSupplierMap(modelIdToQueryTokensSupplierMap())
            .modelIdToVectorSupplierMap(modelIdToVectorSupplierMap())
            .searchAnalyzer(searchAnalyzer())
            .build();
    }

    private QueryBuilder rewriteQueryWithQueryShardContext(QueryShardContext shardContext) {
        final MappedFieldType mappedFieldType = shardContext.fieldMapper(fieldName());
        if (mappedFieldType == null) {
            // We will convert it to NoMatchDocQuery later in doToQuery function.
            return this;
        }

        // In PercolatorFieldMapper we can try to rewrite the neural query with QueryShardContext directly. In that
        // case if the target field is a knn field we should fall back to the old logic to process it.
        if (KNNVectorFieldMapper.CONTENT_TYPE.equals(mappedFieldType.typeName())) {
            return rewriteQueryAgainstKnnField(shardContext);
        }

        if (SemanticFieldMapper.CONTENT_TYPE.equals(mappedFieldType.typeName()) == false) {
            throw new RuntimeException(
                "Expect the neural query target field to be a semantic field but found: " + mappedFieldType.typeName()
            );
        }

        final SemanticFieldMapper.SemanticFieldType semanticFieldType = (SemanticFieldMapper.SemanticFieldType) mappedFieldType;
        final Boolean chunkingEnabled = semanticFieldType.getSemanticParameters().getChunkingEnabled();
        final String semanticFieldSearchAnalyzer = semanticFieldType.getSemanticParameters().getSemanticFieldSearchAnalyzer();
        final NeuralQueryTargetFieldConfig.NeuralQueryTargetFieldConfigBuilder targetFieldConfigBuilder = NeuralQueryTargetFieldConfig
            .builder()
            .isSemanticField(true)
            .searchModelId(getSearchModelId(semanticFieldType))
            .chunkingEnabled(chunkingEnabled)
            .semanticFieldSearchAnalyzer(semanticFieldSearchAnalyzer);

        final String semanticInfoFieldPath = semanticFieldType.getSemanticInfoFieldPath();
        String embeddingFieldPath;
        if (Boolean.TRUE.equals(chunkingEnabled)) {
            final String chunksPath = semanticInfoFieldPath + PATH_SEPARATOR + CHUNKS_FIELD_NAME;
            targetFieldConfigBuilder.chunksPath(chunksPath);
            embeddingFieldPath = chunksPath + PATH_SEPARATOR + EMBEDDING_FIELD_NAME;
        } else {
            embeddingFieldPath = semanticInfoFieldPath + PATH_SEPARATOR + EMBEDDING_FIELD_NAME;
        }
        targetFieldConfigBuilder.embeddingFieldPath(embeddingFieldPath);

        final MappedFieldType embeddingFieldType = shardContext.fieldMapper(embeddingFieldPath);
        if (embeddingFieldType == null) {
            throw new RuntimeException(
                getErrorMessageWithBaseErrorForSemantic("Expect the embedding field exists in the index mapping but not able to find it.")
            );
        }

        final String embeddingFieldTypeName = embeddingFieldType.typeName();

        final NeuralQueryTargetFieldConfig targetFieldConfig = targetFieldConfigBuilder.embeddingFieldType(embeddingFieldTypeName).build();

        // In PercolatorFieldMapper we can try to rewrite the neural query with QueryShardContext directly. In that case
        // we need to generate the embedding on the shard level.
        if (modelIdToVectorSupplierMap == null && modelIdToQueryTokensSupplierMap == null) {
            return inferenceForSemanticField(shardContext, Set.of(targetFieldConfig.getSearchModelId()), embeddingFieldTypeName);
        }

        return rewriteQueryForSemanticField(targetFieldConfig);
    }

    /**
     * Get the search model id from the semanticFieldType
     * @param semanticFieldType semantic field type
     * @return search model id
     */
    private String getSearchModelId(@NonNull final SemanticFieldMapper.SemanticFieldType semanticFieldType) {
        if (semanticFieldType.getSemanticParameters().getSearchModelId() != null) {
            return semanticFieldType.getSemanticParameters().getSearchModelId();
        } else {
            return semanticFieldType.getSemanticParameters().getModelId();
        }
    }

    /**
     * If we have a model id defined in the query we should use it to override the model id defined in the field config
     * @param config target field config
     * @return search model id
     */
    private String getSearchModelId(@NonNull final NeuralQueryTargetFieldConfig config) {
        if (modelId != null) {
            return modelId;
        } else {
            return config.getSearchModelId();
        }
    }

    /**
     * If we have a Semantic Field Search Analyzer defined in the query we should use it to override the search analyzer defined in the field config
     * @param config target field config
     * @return Semantic Field Search Analyzer
     */
    private String getSearchAnalyzer(@NonNull final NeuralQueryTargetFieldConfig config) {
        if (searchAnalyzer != null) {
            return searchAnalyzer;
        } else {
            return config.getSemanticFieldSearchAnalyzer();
        }
    }

    private String getErrorMessageWithBaseErrorForSemantic(@NonNull final String errorMessage) {
        return "Failed to rewrite the neural query against the semantic field " + fieldName + ". " + errorMessage;
    }

    private QueryBuilder inferenceForSemanticField(
        @NonNull final QueryRewriteContext queryRewriteContext,
        @NonNull final Set<String> modelIdsFromTargetFields,
        @NonNull final String embeddingFieldType
    ) {
        Set<String> modelIds = modelIdsFromTargetFields;
        if (modelId != null) {
            // If user explicitly define a model id in the query we should use it to override
            // the model id defined in the index mapping.
            modelIds = Set.of(modelId);
        }

        if (KNNVectorFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            modelIdToVectorSupplierMap = new HashMap<>(modelIds.size());
        } else if (RankFeaturesFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            modelIdToQueryTokensSupplierMap = new HashMap<>(modelIds.size());
        } else {
            throw new RuntimeException(
                String.format(
                    Locale.ROOT,
                    "Not able to do inference for the neural query against the " + "field %s. Unsupported embedding field type: %s.",
                    fieldName,
                    embeddingFieldType
                )
            );
        }

        // build first to leverage the validation in the build function
        final NeuralQueryBuilder neuralQueryBuilder = createNeuralQueryBuilder(embeddingFieldType, vectorSupplier(), true);

        if (KNNVectorFieldMapper.CONTENT_TYPE.equals(embeddingFieldType)) {
            inferenceByDenseModel(modelIds, queryRewriteContext);
        } else {
            // We only inference for the sparse model if the raw query tokens are not provided
            if (queryTokensMapSupplier == null) {
                inferenceBySparseModel(modelIds, queryRewriteContext);
            }
        }

        // We don't do rewrite and just start the async actions to inference the query text
        // We still need to return a different object to enter the code block to execute the async tasks
        // Otherwise we will directly end the rewrite.
        return neuralQueryBuilder;
    }

    private void inferenceBySparseModel(@NonNull final Set<String> modelIds, @NonNull QueryRewriteContext queryRewriteContext) {
        for (String modelId : modelIds) {
            final SetOnce<Map<String, Float>> setOnce = new SetOnce<>();
            modelIdToQueryTokensSupplierMap.put(modelId, setOnce::get);
            queryRewriteContext.registerAsyncAction(
                ((client, actionListener) -> ML_CLIENT.inferenceSentencesWithMapResult(
                    TextInferenceRequest.builder().modelId(modelId).inputTexts(List.of(queryText)).build(),
                    ActionListener.wrap(mapResultList -> {
                        final Map<String, Float> queryTokens = TokenWeightUtil.fetchListOfTokenWeightMap(mapResultList).get(0);
                        // Currently we don't support NeuralSparseTwoPhaseProcessor which can be supported
                        // in the future.
                        setOnce.set(queryTokens);
                        actionListener.onResponse(null);
                    }, actionListener::onFailure)
                ))
            );
        }
    }

    private void inferenceByDenseModel(@NonNull final Set<String> modelIds, @NonNull QueryRewriteContext queryRewriteContext) {
        final Map<String, String> inferenceInput = getInferenceInputForDenseModel();
        for (String modelId : modelIds) {
            final SetOnce<float[]> vectorSetOnce = new SetOnce<>();
            modelIdToVectorSupplierMap.put(modelId, vectorSetOnce::get);
            queryRewriteContext.registerAsyncAction(
                ((client, actionListener) -> ML_CLIENT.inferenceSentencesMap(
                    MapInferenceRequest.builder().modelId(modelId).inputObjects(inferenceInput).build(),
                    ActionListener.wrap(floatList -> {
                        vectorSetOnce.set(vectorAsListToArray(floatList));
                        actionListener.onResponse(null);
                    }, actionListener::onFailure)
                ))
            );
        }
    }

    private Map<String, String> getInferenceInputForDenseModel() {
        Map<String, String> inferenceInput = new HashMap<>();
        if (StringUtils.isNotBlank(queryText())) {
            inferenceInput.put(INPUT_TEXT, queryText());
        }
        if (StringUtils.isNotBlank(queryImage())) {
            inferenceInput.put(INPUT_IMAGE, queryImage());
        }
        return inferenceInput;
    }

    /**
     * We only rely on this function to handle the case when the target field is an unmapped field and simply convert
     * it to a MatchNoDocsQuery. For other use cases the query should be rewritten to other query builders, and we
     * should not reach this function.
     * @param queryShardContext query context on shard level
     * @return query
     */
    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) {
        final MappedFieldType mappedFieldType = queryShardContext.fieldMapper(this.fieldName);
        if (mappedFieldType == null) {
            return new MatchNoDocsQuery();
        } else {
            throw new UnsupportedOperationException("Query cannot be created by NeuralQueryBuilder directly");
        }
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
        equalsBuilder.append(searchAnalyzer, obj.searchAnalyzer);
        equalsBuilder.append(k, obj.k);
        equalsBuilder.append(maxDistance, obj.maxDistance);
        equalsBuilder.append(minScore, obj.minScore);
        equalsBuilder.append(expandNested, obj.expandNested);
        equalsBuilder.append(getVector(vectorSupplier), getVector(obj.vectorSupplier));
        equalsBuilder.append(filter, obj.filter);
        equalsBuilder.append(methodParameters, obj.methodParameters);
        equalsBuilder.append(rescoreContext, obj.rescoreContext);
        equalsBuilder.append(getQueryTokenMap(queryTokensMapSupplier), getQueryTokenMap(obj.queryTokensMapSupplier));
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            fieldName,
            queryText,
            queryImage,
            modelId,
            searchAnalyzer,
            k,
            maxDistance,
            minScore,
            expandNested,
            Arrays.hashCode(getVector(vectorSupplier)),
            filter,
            methodParameters,
            rescoreContext,
            getQueryTokenMap(queryTokensMapSupplier)
        );
    }

    private float[] getVector(final Supplier<float[]> vectorSupplier) {
        return Objects.isNull(vectorSupplier) ? null : vectorSupplier.get();
    }

    private Map<String, Float> getQueryTokenMap(final Supplier<Map<String, Float>> queryTokensSupplierMap) {
        return Objects.isNull(queryTokensSupplierMap) ? null : queryTokensSupplierMap.get();
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
