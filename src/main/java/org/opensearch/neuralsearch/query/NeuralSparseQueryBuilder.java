/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.transport.client.Client;
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Tuple;
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
import org.opensearch.index.query.WithFieldName;
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
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;

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
public class NeuralSparseQueryBuilder extends AbstractQueryBuilder<NeuralSparseQueryBuilder>
    implements
        ModelInferenceQueryBuilder,
        WithFieldName {
    public static final String NAME = "neural_sparse";
    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    @VisibleForTesting
    static final ParseField QUERY_TOKENS_FIELD = new ParseField("query_tokens");
    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    // We use max_token_score field to help WAND scorer prune query clause in lucene 9.7. But in lucene 9.8 the inner
    // logics change, this field is not needed any more.
    @VisibleForTesting
    @Deprecated
    static final ParseField MAX_TOKEN_SCORE_FIELD = new ParseField("max_token_score").withAllDeprecated();
    @VisibleForTesting
    static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static MLCommonsClientAccessor ML_CLIENT;
    private static final String DEFAULT_ANALYZER = "bert-uncased";

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralSparseQueryBuilder.ML_CLIENT = mlClient;
    }

    private String fieldName;
    private String queryText;
    private String modelId;
    private String analyzer;
    private Float maxTokenScore;
    private Supplier<Map<String, Float>> queryTokensSupplier;
    // A field that for neural_sparse_two_phase_processor, if twoPhaseSharedQueryToken is not null,
    // it means it's origin NeuralSparseQueryBuilder and should split the low score tokens form itself then put it into
    // twoPhaseSharedQueryToken.
    private Map<String, Float> twoPhaseSharedQueryToken;
    private NeuralSparseQueryTwoPhaseInfo neuralSparseQueryTwoPhaseInfo = new NeuralSparseQueryTwoPhaseInfo();

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_13_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_ANALYZER = Version.V_3_0_0;

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public NeuralSparseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryText = in.readString();
        if (isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport()) {
            this.modelId = in.readOptionalString();
        } else {
            this.modelId = in.readString();
        }
        this.maxTokenScore = in.readOptionalFloat();
        if (in.readBoolean()) {
            Map<String, Float> queryTokens = in.readMap(StreamInput::readString, StreamInput::readFloat);
            this.queryTokensSupplier = () -> queryTokens;
        }
        if (isClusterOnOrAfterMinReqVersionForAnalyzer()) {
            this.analyzer = in.readOptionalString();
            this.neuralSparseQueryTwoPhaseInfo = new NeuralSparseQueryTwoPhaseInfo(in);
        }
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
        out.writeOptionalFloat(maxTokenScore);
        if (!Objects.isNull(this.queryTokensSupplier) && !Objects.isNull(this.queryTokensSupplier.get())) {
            out.writeBoolean(true);
            out.writeMap(this.queryTokensSupplier.get(), StreamOutput::writeString, StreamOutput::writeFloat);
        } else {
            out.writeBoolean(false);
        }
        if (isClusterOnOrAfterMinReqVersionForAnalyzer()) {
            out.writeOptionalString(this.analyzer);
            this.neuralSparseQueryTwoPhaseInfo.writeTo(out);
        }
    }

    /**
     * Copy this QueryBuilder for two phase rescorer.
     * @param pruneRatio the parameter of the NeuralSparseTwoPhaseProcessor, control the ratio of splitting the queryTokens to two phase.
     * @param pruneType the parameter of the NeuralSparseTwoPhaseProcessor, control how to split the queryTokens to two phase.
     * @return A copy NeuralSparseQueryBuilder for twoPhase, it will be added to the rescorer.
     */
    public NeuralSparseQueryBuilder getCopyNeuralSparseQueryBuilderForTwoPhase(float pruneRatio, PruneType pruneType) {
        this.neuralSparseQueryTwoPhaseInfo = new NeuralSparseQueryTwoPhaseInfo(
            NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE,
            pruneRatio,
            pruneType
        );
        NeuralSparseQueryBuilder copy = new NeuralSparseQueryBuilder().fieldName(this.fieldName)
            .queryName(this.queryName)
            .queryText(this.queryText)
            .modelId(this.modelId)
            .analyzer(this.analyzer)
            .maxTokenScore(this.maxTokenScore)
            .neuralSparseQueryTwoPhaseInfo(
                new NeuralSparseQueryTwoPhaseInfo(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_TWO, pruneRatio, pruneType)
            );
        if (Objects.nonNull(this.queryTokensSupplier)) {
            Map<String, Float> tokens = queryTokensSupplier.get();
            // Splitting tokens based on a threshold value: tokens greater than the threshold are stored in v1,
            // while those less than or equal to the threshold are stored in v2.
            Tuple<Map<String, Float>, Map<String, Float>> splitTokens = PruneUtils.splitSparseVector(pruneType, pruneRatio, tokens);
            this.queryTokensSupplier(() -> splitTokens.v1());
            copy.queryTokensSupplier(() -> splitTokens.v2());
        } else if (shouldUseModelId()) {
            this.twoPhaseSharedQueryToken = new HashMap<>();
            copy.queryTokensSupplier(() -> this.twoPhaseSharedQueryToken);
        }
        return copy;
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
        if (Objects.nonNull(analyzer)) {
            xContentBuilder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        if (Objects.nonNull(maxTokenScore)) {
            xContentBuilder.field(MAX_TOKEN_SCORE_FIELD.getPreferredName(), maxTokenScore);
        }
        if (Objects.nonNull(queryTokensSupplier) && Objects.nonNull(queryTokensSupplier.get())) {
            xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), queryTokensSupplier.get());
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
    public static NeuralSparseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        NeuralSparseQueryBuilder sparseEncodingQueryBuilder = new NeuralSparseQueryBuilder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "First token of " + NAME + "query must be START_OBJECT");
        }
        parser.nextToken();
        sparseEncodingQueryBuilder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, sparseEncodingQueryBuilder);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "[%s] query doesn't support multiple fields, found [%s] and [%s]",
                    NAME,
                    sparseEncodingQueryBuilder.fieldName(),
                    parser.currentName()
                )
            );
        }

        requireValue(sparseEncodingQueryBuilder.fieldName(), "Field name must be provided for " + NAME + " query");
        if (Objects.isNull(sparseEncodingQueryBuilder.queryTokensSupplier())) {
            requireValue(
                sparseEncodingQueryBuilder.queryText(),
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
                    sparseEncodingQueryBuilder.modelId(),
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

        if (Objects.isNull(sparseEncodingQueryBuilder.analyzer())) {
            sparseEncodingQueryBuilder.analyzer(DEFAULT_ANALYZER);
        }

        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.queryText())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s field can not be empty", QUERY_TEXT_FIELD.getPreferredName())
            );
        }
        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.modelId())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s field can not be empty", MODEL_ID_FIELD.getPreferredName()));
        }

        return sparseEncodingQueryBuilder;
    }

    private static void parseQueryParams(XContentParser parser, NeuralSparseQueryBuilder sparseEncodingQueryBuilder) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.boost(parser.floatValue());
                } else if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.queryText(parser.text());
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.modelId(parser.text());
                } else if (ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.analyzer(parser.text());
                } else if (MAX_TOKEN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.maxTokenScore(parser.floatValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] query does not support [%s] field", NAME, currentFieldName)
                    );
                }
            } else if (QUERY_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                Map<String, Float> queryTokens = parser.map(HashMap::new, XContentParser::floatValue);
                sparseEncodingQueryBuilder.queryTokensSupplier(() -> queryTokens);
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
        if (!shouldUseModelId() && Objects.nonNull(analyzer)) {
            return this;
        }
        validateForRewrite(queryText, modelId);
        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(getModelInferenceAsync(queryTokensSetOnce));
        return new NeuralSparseQueryBuilder().fieldName(fieldName)
            .queryText(queryText)
            .modelId(modelId)
            .maxTokenScore(maxTokenScore)
            .queryTokensSupplier(queryTokensSetOnce::get)
            .twoPhaseSharedQueryToken(twoPhaseSharedQueryToken)
            .neuralSparseQueryTwoPhaseInfo(neuralSparseQueryTwoPhaseInfo);
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
                if (Objects.nonNull(twoPhaseSharedQueryToken)) {
                    Tuple<Map<String, Float>, Map<String, Float>> splitQueryTokens = PruneUtils.splitSparseVector(
                        neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneType(),
                        neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneRatio(),
                        queryTokens
                    );
                    setOnce.set(splitQueryTokens.v1());
                    twoPhaseSharedQueryToken = splitQueryTokens.v2();
                } else {
                    setOnce.set(queryTokens);
                }
                actionListener.onResponse(null);
            }, actionListener::onFailure)
        ));
    }

    Map<String, Float> getQueryTokens(QueryShardContext context) {
        if (Objects.nonNull(queryTokensSupplier)) {
            return queryTokensSupplier.get();
        } else if (Objects.nonNull(analyzer)) {
            Map<String, Float> queryTokens = new HashMap<>();
            Analyzer luceneAnalyzer = context.getIndexAnalyzers().getAnalyzers().get(this.analyzer);
            if (Objects.isNull(luceneAnalyzer)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Analyzer [%s] not found in shard context. ", this.analyzer));
            }
            try (TokenStream stream = luceneAnalyzer.tokenStream(fieldName, queryText)) {
                stream.reset();
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                PayloadAttribute payload = stream.addAttribute(PayloadAttribute.class);

                while (stream.incrementToken()) {
                    String token = term.toString();
                    float weight = Objects.isNull(payload.getPayload()) ? 1.0f : bytesToFloat(payload.getPayload().bytes);
                    if (weight > 0) {
                        queryTokens.put(token, weight);
                    }
                }
                stream.end();

                return switch (neuralSparseQueryTwoPhaseInfo.getStatus()) {
                    case NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_TWO -> PruneUtils.splitSparseVector(
                        neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneType(),
                        neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneRatio(),
                        queryTokens
                    ).v2();
                    case NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE -> PruneUtils.splitSparseVector(
                        neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneType(),
                        neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneRatio(),
                        queryTokens
                    ).v1();
                    default -> queryTokens;
                };
            } catch (IOException e) {
                throw new OpenSearchException("failed to analyze query text. ", e);
            } catch (BufferUnderflowException e) {
                throw new OpenSearchException("failed to parse query token weight from analyzer. ", e);
            }
        }
        throw new IllegalArgumentException("Both query tokens and analyzer is null.");
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        validateFieldType(ft);
        Map<String, Float> queryTokens = getQueryTokens(context);
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
        if (Objects.isNull(fieldType) || !fieldType.typeName().equals("rank_features")) {
            throw new IllegalArgumentException("[" + NAME + "] query only works on [rank_features] fields");
        }
    }

    @Override
    protected boolean doEquals(NeuralSparseQueryBuilder obj) {
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
            .append(maxTokenScore, obj.maxTokenScore)
            .append(neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneType(), obj.neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneType())
            .append(neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneRatio(), obj.neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneRatio())
            .append(neuralSparseQueryTwoPhaseInfo.getStatus().getValue(), obj.neuralSparseQueryTwoPhaseInfo.getStatus().getValue())
            .append(twoPhaseSharedQueryToken, obj.twoPhaseSharedQueryToken)
            .append(analyzer, obj.analyzer);
        if (Objects.nonNull(queryTokensSupplier)) {
            equalsBuilder.append(queryTokensSupplier.get(), obj.queryTokensSupplier.get());
        }
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(fieldName)
            .append(queryText)
            .append(modelId)
            .append(maxTokenScore)
            .append(neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneType())
            .append(neuralSparseQueryTwoPhaseInfo.getTwoPhasePruneRatio())
            .append(neuralSparseQueryTwoPhaseInfo.getStatus().getValue())
            .append(twoPhaseSharedQueryToken)
            .append(analyzer);
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

    private static boolean isClusterOnOrAfterMinReqVersionForAnalyzer() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_ANALYZER);
    }

    private boolean shouldUseModelId() {
        return StringUtils.isNotEmpty(modelId);
    }

    public static float bytesToFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    /**
     * Gets the field name that this query is searching against.
     *
     * @return The field name used in the Neural Sparse query
     */
    @Override
    public String fieldName() {
        return this.fieldName;
    }
}
