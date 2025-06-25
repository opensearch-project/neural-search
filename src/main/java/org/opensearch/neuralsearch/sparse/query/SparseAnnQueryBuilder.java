/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.neuralsearch.query.ModelInferenceQueryBuilder;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.transport.client.Client;
import org.opensearch.common.SetOnce;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import com.google.common.annotations.VisibleForTesting;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * SparseEncodingQueryBuilder is responsible for handling "neural_sparse" query types. It uses an ML NEURAL_SPARSE model
 * or SPARSE_TOKENIZE model to produce a Map with String keys and Float values for input text. Then it will be transformed
 * to Lucene FeatureQuery wrapped by Lucene BooleanQuery.
 */
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class SparseAnnQueryBuilder extends AbstractQueryBuilder<SparseAnnQueryBuilder> implements ModelInferenceQueryBuilder {
    public static final String NAME = "sparse_ann";
    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    @VisibleForTesting
    static final ParseField QUERY_TOKENS_FIELD = new ParseField("query_tokens");
    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    @VisibleForTesting
    static final ParseField CUT_FIELD = new ParseField("cut");
    @VisibleForTesting
    static final ParseField TOP_K_FIELD = new ParseField("k");
    @VisibleForTesting
    static final ParseField HEAP_FACTOR_FIELD = new ParseField("heap_factor");

    private static MLCommonsClientAccessor ML_CLIENT;
    private String fieldName;
    private String queryText;
    private String modelId;
    private Supplier<Map<String, Float>> queryTokensSupplier;
    private Integer queryCut;
    private Integer k;
    private Float heapFactor;

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_13_0;
    private static final int DEFAULT_TOP_K = 10;
    private static final float DEFAULT_HEAP_FACTOR = 1.0f;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        SparseAnnQueryBuilder.ML_CLIENT = mlClient;
    }

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public SparseAnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryText = in.readString();
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            this.modelId = in.readOptionalString();
        } else {
            this.modelId = in.readString();
        }
        if (in.readBoolean()) {
            Map<String, Float> queryTokens = in.readMap(StreamInput::readString, StreamInput::readFloat);
            this.queryTokensSupplier = () -> queryTokens;
        }

        this.queryCut = in.readOptionalInt();
        this.k = in.readOptionalInt();
        this.heapFactor = in.readOptionalFloat();

        // to be backward compatible with previous version, we need to use writeString/readString API instead of optionalString API
        // after supporting query by tokens, queryText and modelId can be null. here we write an empty String instead
        if (StringUtils.EMPTY.equals(this.queryText)) {
            this.queryText = null;
        }
        if (StringUtils.EMPTY.equals(this.modelId)) {
            this.modelId = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        // to be backward compatible with previous version, we need to use writeString/readString API instead of optionalString API
        // after supporting query by tokens, queryText and modelId can be null. here we write an empty String instead
        out.writeString(StringUtils.defaultString(this.queryText, StringUtils.EMPTY));
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            out.writeOptionalString(this.modelId);
        } else {
            out.writeString(StringUtils.defaultString(this.modelId, StringUtils.EMPTY));
        }
        if (!Objects.isNull(this.queryTokensSupplier) && !Objects.isNull(this.queryTokensSupplier.get())) {
            out.writeBoolean(true);
            out.writeMap(this.queryTokensSupplier.get(), StreamOutput::writeString, StreamOutput::writeFloat);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalInt(this.queryCut);
        out.writeOptionalInt(this.k);
        out.writeOptionalFloat(this.heapFactor);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        if (Objects.nonNull(queryText)) {
            xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        }
        if (Objects.nonNull(modelId)) {
            xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        }
        if (Objects.nonNull(queryTokensSupplier) && Objects.nonNull(queryTokensSupplier.get())) {
            xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), queryTokensSupplier.get());
        }
        if (Objects.nonNull(queryCut)) {
            xContentBuilder.field(CUT_FIELD.getPreferredName(), queryCut);
        }
        if (Objects.nonNull(k)) {
            xContentBuilder.field(TOP_K_FIELD.getPreferredName(), k);
        }
        if (Objects.nonNull(heapFactor)) {
            xContentBuilder.field(HEAP_FACTOR_FIELD.getPreferredName(), heapFactor);
        }
        printBoostAndQueryName(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    /**
     * The expected parsing form looks like:
     *  "SAMPLE_FIELD": {
     *    "query_text": "string",
     *    "model_id": "string",
     *    "max_token_score": float (optional)
     *  }
     *
     *  or
     *  "SAMPLE_FIELD": {
     *      "query_tokens": {
     *          "token_a": float,
     *          "token_b": float,
     *          ...
     *       }
     *  }
     *
     *
     * @param parser XContentParser
     * @return NeuralQueryBuilder
     * @throws IOException can be thrown by parser
     */
    public static SparseAnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        SparseAnnQueryBuilder sparseAnnQueryBuilder = new SparseAnnQueryBuilder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "First token of " + NAME + "query must be START_OBJECT");
        }
        parser.nextToken();
        sparseAnnQueryBuilder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, sparseAnnQueryBuilder);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "[%s] query doesn't support multiple fields, found [%s] and [%s]",
                    NAME,
                    sparseAnnQueryBuilder.fieldName(),
                    parser.currentName()
                )
            );
        }

        requireValue(sparseAnnQueryBuilder.fieldName(), "Field name must be provided for " + NAME + " query");
        if (Objects.isNull(sparseAnnQueryBuilder.queryTokensSupplier())) {
            requireValue(
                sparseAnnQueryBuilder.queryText(),
                String.format(
                    Locale.ROOT,
                    "either %s field or %s field must be provided for [%s] query",
                    QUERY_TEXT_FIELD.getPreferredName(),
                    QUERY_TOKENS_FIELD.getPreferredName(),
                    NAME
                )
            );
            if (!isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
                requireValue(
                    sparseAnnQueryBuilder.modelId(),
                    String.format(
                        Locale.ROOT,
                        "using %s, %s field must be provided for [%s] query",
                        QUERY_TEXT_FIELD.getPreferredName(),
                        MODEL_ID_FIELD.getPreferredName(),
                        NAME
                    )
                );
            }
        }

        if (StringUtils.EMPTY.equals(sparseAnnQueryBuilder.queryText())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s field can not be empty", QUERY_TEXT_FIELD.getPreferredName())
            );
        }
        if (StringUtils.EMPTY.equals(sparseAnnQueryBuilder.modelId())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s field can not be empty", MODEL_ID_FIELD.getPreferredName()));
        }

        return sparseAnnQueryBuilder;
    }

    private static void parseQueryParams(XContentParser parser, SparseAnnQueryBuilder sparseAnnQueryBuilder) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.boost(parser.floatValue());
                } else if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.queryText(parser.text());
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.modelId(parser.text());
                } else if (CUT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.queryCut(parser.intValue());
                } else if (TOP_K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.k(parser.intValue());
                } else if (HEAP_FACTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseAnnQueryBuilder.heapFactor(parser.floatValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] query does not support [%s] field", NAME, currentFieldName)
                    );
                }
            } else if (QUERY_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                Map<String, Float> queryTokens = parser.map(HashMap::new, XContentParser::floatValue);
                sparseAnnQueryBuilder.queryTokensSupplier(() -> queryTokens);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "[%s] unknown token [%s] after [%s]", NAME, token, currentFieldName)
                );
            }
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // We need to inference the sentence to get the queryTokens. The logic is similar to NeuralQueryBuilder
        // If the inference is finished, then rewrite to self and call doToQuery, otherwise, continue doRewrite
        // QueryTokensSupplier means 2 case now,
        // 1. It's the queryBuilder built for two-phase, doesn't need any rewrite.
        // 2. It's registerAsyncAction has been registered successful.
        if (Objects.nonNull(queryTokensSupplier)) {
            return this;
        }
        validateForRewrite(queryText, modelId);
        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(getModelInferenceAsync(queryTokensSetOnce));
        return new SparseAnnQueryBuilder().fieldName(fieldName)
            .queryText(queryText)
            .modelId(modelId)
            .queryCut(queryCut)
            .k(k)
            .heapFactor(heapFactor)
            .queryTokensSupplier(queryTokensSetOnce::get);
    }

    private BiConsumer<Client, ActionListener<?>> getModelInferenceAsync(SetOnce<Map<String, Float>> setOnce) {
        // When Two-phase shared query tokens is null,
        // it set queryTokensSupplier to the inference result which has all query tokens with score.
        // When Two-phase shared query tokens exist,
        // it splits the tokens using a threshold defined by a ratio of the maximum score of tokens, updating the token set
        // accordingly.
        return ((client, actionListener) -> ML_CLIENT.inferenceSentencesWithMapResult(
            TextInferenceRequest.builder().modelId(modelId()).inputTexts(List.of(queryText)).build(),
            ActionListener.wrap(mapResultList -> {
                Map<String, Float> queryTokens = TokenWeightUtil.fetchListOfTokenWeightMap(mapResultList).get(0);
                setOnce.set(queryTokens);
                actionListener.onResponse(null);
            }, actionListener::onFailure)
        ));
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        validateFieldType(ft);
        Map<String, Float> queryTokens = queryTokensSupplier.get();
        if (Objects.isNull(queryTokens)) {
            throw new IllegalArgumentException("Query tokens cannot be null.");
        }
        // cut tokens
        int n = queryCut == null ? queryTokens.size() : Math.min(queryCut, queryTokens.size());
        List<String> topTokens = queryTokens.entrySet()
            .stream()
            .sorted(Map.Entry.<String, Float>comparingByValue().reversed()) // Sort by values in descending order
            .limit(n) // Take only top N elements
            .map(Map.Entry::getKey)
            .toList();

        SparseQueryContext sparseQueryContext = SparseQueryContext.builder()
            .tokens(topTokens)
            .heapFactor(heapFactor == null ? DEFAULT_HEAP_FACTOR : heapFactor)
            .k((k == null || k == 0) ? DEFAULT_TOP_K : k)
            .build();

        SparseVectorQuery.SparseVectorQueryBuilder builder = new SparseVectorQuery.SparseVectorQueryBuilder();
        builder.fieldName(fieldName)
            .queryContext(sparseQueryContext)
            .queryVector(new SparseVector(queryTokens))
            .originalQuery(constructRankFeaturesQuery(queryTokens));
        return builder.build();
    }

    private Query constructRankFeaturesQuery(Map<String, Float> queryTokens) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            builder.add(FeatureField.newLinearQuery(fieldName, entry.getKey(), entry.getValue()), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    private static void validateForRewrite(String queryText, String modelId) {
        if (StringUtils.isBlank(queryText) || StringUtils.isBlank(modelId)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "%s and %s cannot be null",
                    QUERY_TEXT_FIELD.getPreferredName(),
                    MODEL_ID_FIELD.getPreferredName()
                )
            );
        }
    }

    private static void validateFieldType(MappedFieldType fieldType) {
        if (Objects.isNull(fieldType) || !fieldType.typeName().equals(SparseTokensFieldMapper.CONTENT_TYPE)) {
            throw new IllegalArgumentException("[" + NAME + "] query only works on [" + SparseTokensFieldMapper.CONTENT_TYPE + "] fields");
        }
    }

    @Override
    protected boolean doEquals(SparseAnnQueryBuilder obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        if (Objects.isNull(queryTokensSupplier) && Objects.nonNull(obj.queryTokensSupplier)) {
            return false;
        }
        if (Objects.nonNull(queryTokensSupplier) && Objects.isNull(obj.queryTokensSupplier)) {
            return false;
        }

        EqualsBuilder equalsBuilder = new EqualsBuilder().append(fieldName, obj.fieldName)
            .append(queryText, obj.queryText)
            .append(modelId, obj.modelId)
            .append(queryCut, obj.queryCut)
            .append(heapFactor, obj.heapFactor)
            .append(k, obj.k);
        if (Objects.nonNull(queryTokensSupplier)) {
            equalsBuilder.append(queryTokensSupplier.get(), obj.queryTokensSupplier.get());
        }
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(fieldName).append(queryText).append(modelId).append(k);
        if (Objects.nonNull(queryTokensSupplier)) {
            builder.append(queryTokensSupplier.get());
        }
        return builder.toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static boolean isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID);
    }

}
