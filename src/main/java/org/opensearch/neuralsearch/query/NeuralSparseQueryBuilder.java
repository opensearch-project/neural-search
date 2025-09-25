/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.ml.common.input.parameter.textembedding.AsymmetricTextEmbeddingParameters;
import org.opensearch.ml.common.input.parameter.textembedding.SparseEmbeddingFormat;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorFieldType;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder.METHOD_PARAMETERS_FIELD;

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
public class NeuralSparseQueryBuilder extends AbstractNeuralQueryBuilder<NeuralSparseQueryBuilder> implements WithFieldName {
    public static final String NAME = "neural_sparse";
    // We use max_token_score field to help WAND scorer prune query clause in lucene 9.7. But in lucene 9.8 the inner
    // logics change, this field is not needed any more. deprecated since OpenSearch 2.12
    @VisibleForTesting
    @Deprecated
    static final ParseField MAX_TOKEN_SCORE_FIELD = new ParseField("max_token_score").withAllDeprecated();
    @VisibleForTesting
    static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static MLCommonsClientAccessor ML_CLIENT;
    private static final String DEFAULT_ANALYZER = "bert-uncased";
    private static final AsymmetricTextEmbeddingParameters TOKEN_ID_PARAMETER = AsymmetricTextEmbeddingParameters.builder()
        .sparseEmbeddingFormat(SparseEmbeddingFormat.TOKEN_ID)
        .build();

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralSparseQueryBuilder.ML_CLIENT = mlClient;
    }

    // deprecated since OpenSearch 2.12
    @Deprecated
    private Float maxTokenScore;
    // A field that for neural_sparse_two_phase_processor. Original query builder will set the lower score tokens to
    // this field so that the rescore query can use it to save an inference call.
    protected Map<String, Float> twoPhaseSharedQueryToken;
    private SparseAnnQueryBuilder sparseAnnQueryBuilder;
    private ClusterService clusterService;

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_13_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_ANALYZER = Version.V_3_1_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_SEISMIC = Version.V_3_3_0;

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
            this.queryTokensMapSupplier = () -> queryTokens;
        }
        if (isClusterOnOrAfterMinReqVersionForAnalyzer()) {
            this.searchAnalyzer = in.readOptionalString();
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
        if (isSeismicSupported() && in.readBoolean()) {
            this.sparseAnnQueryBuilder = new SparseAnnQueryBuilder(in);
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
        if (!Objects.isNull(this.queryTokensMapSupplier) && !Objects.isNull(this.queryTokensMapSupplier.get())) {
            out.writeBoolean(true);
            out.writeMap(this.queryTokensMapSupplier.get(), StreamOutput::writeString, StreamOutput::writeFloat);
        } else {
            out.writeBoolean(false);
        }
        if (isClusterOnOrAfterMinReqVersionForAnalyzer()) {
            out.writeOptionalString(this.searchAnalyzer);
            this.neuralSparseQueryTwoPhaseInfo.writeTo(out);
        }
        if (isSeismicSupported()) {
            if (this.sparseAnnQueryBuilder != null) {
                out.writeBoolean(true);
                this.sparseAnnQueryBuilder.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    /**
     * Copy this QueryBuilder for two phase rescorer. This function will be invoked by the search processor
     * NeuralSparseTwoPhaseProcessor which happens before the rewrite phase.
     * @param pruneRatio the parameter of the NeuralSparseTwoPhaseProcessor, control the ratio of splitting the queryTokens to two phase.
     * @param pruneType the parameter of the NeuralSparseTwoPhaseProcessor, control how to split the queryTokens to two phase.
     * @return A copy NeuralSparseQueryBuilder for twoPhase, it will be added to the rescorer.
     */
    public NeuralSparseQueryBuilder prepareTwoPhaseQuery(float pruneRatio, PruneType pruneType) {
        this.neuralSparseQueryTwoPhaseInfo = new NeuralSparseQueryTwoPhaseInfo(
            NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE,
            pruneRatio,
            pruneType
        );
        NeuralSparseQueryBuilder copy = new NeuralSparseQueryBuilder().fieldName(this.fieldName)
            .queryName(this.queryName)
            .queryText(this.queryText)
            .modelId(this.modelId)
            .searchAnalyzer(this.searchAnalyzer)
            .maxTokenScore(this.maxTokenScore)
            .neuralSparseQueryTwoPhaseInfo(
                new NeuralSparseQueryTwoPhaseInfo(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_TWO, pruneRatio, pruneType)
            )
            .sparseAnnQueryBuilder(sparseAnnQueryBuilder);

        // If raw tokens are provided directly in the query, split them without additional processing
        if (Objects.nonNull(this.queryTokensMapSupplier)) {
            Map<String, Float> tokens = queryTokensMapSupplier.get();
            // Splitting tokens based on a threshold value: tokens greater than the threshold are stored in v1,
            // while those less than or equal to the threshold are stored in v2.
            Tuple<Map<String, Float>, Map<String, Float>> splitTokens = PruneUtils.splitSparseVector(pruneType, pruneRatio, tokens);
            this.queryTokensMapSupplier(() -> splitTokens.v1());
            copy.queryTokensMapSupplier(() -> splitTokens.v2());
        } else {
            // Otherwise delay the split work to when the query tokens are available.
            // Here we leverage the twoPhaseSharedQueryToken to pass the tokens generated by ML model for the phase one
            // query to the phase two query to save one inference call.
            copy.queryTokensMapSupplier(() -> this.twoPhaseSharedQueryToken);
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
        if (Objects.nonNull(searchAnalyzer)) {
            xContentBuilder.field(ANALYZER_FIELD.getPreferredName(), searchAnalyzer);
        }
        if (Objects.nonNull(maxTokenScore)) {
            xContentBuilder.field(MAX_TOKEN_SCORE_FIELD.getPreferredName(), maxTokenScore);
        }
        if (Objects.nonNull(queryTokensMapSupplier) && Objects.nonNull(queryTokensMapSupplier.get())) {
            xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), queryTokensMapSupplier.get());
        }
        if (Objects.nonNull(sparseAnnQueryBuilder) && isSeismicSupported()) {
            xContentBuilder.field(METHOD_PARAMETERS_FIELD.getPreferredName(), sparseAnnQueryBuilder);
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
        EventStatsManager.increment(EventStatName.NEURAL_SPARSE_QUERY_REQUESTS);
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
        if (Objects.isNull(sparseEncodingQueryBuilder.queryTokensMapSupplier())) {
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

        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.queryText())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "%s field can not be empty", QUERY_TEXT_FIELD.getPreferredName())
            );
        }
        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.modelId())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s field can not be empty", MODEL_ID_FIELD.getPreferredName()));
        }
        if (StringUtils.EMPTY.equals(sparseEncodingQueryBuilder.searchAnalyzer())) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s field can not be empty", ANALYZER_FIELD.getPreferredName()));
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
                    sparseEncodingQueryBuilder.searchAnalyzer(parser.text());
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
                sparseEncodingQueryBuilder.queryTokensMapSupplier(() -> queryTokens);
            } else if (METHOD_PARAMETERS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (sparseEncodingQueryBuilder.isSeismicSupported()) {
                    SparseAnnQueryBuilder builder = SparseAnnQueryBuilder.fromXContent(parser);
                    sparseEncodingQueryBuilder.sparseAnnQueryBuilder(builder);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] unknown token [%s] after [%s]", NAME, token, currentFieldName)
                    );
                }
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
        // QueryTokensSupplier exist can be below cases:
        // 1. It's the queryBuilder built for two-phase, doesn't need any rewrite.
        // 2. Inference is finished, and we set the tokens to queryTokensSupplier.
        // 3. Raw tokens are provided through the query directly so we do not need to do inference.
        if (Objects.nonNull(queryTokensMapSupplier)) {
            return this;
        }

        // If we should use analyzer then no need to generate the embedding using the model id so simply return
        // this to end the rewrite. In doToQuery we will use the analyzer to tokenize the query_text.
        if (shouldUseAnalyzer()) {
            return this;
        }
        boolean withTokenId = shouldInferenceWithTokenIdResponse(queryRewriteContext);
        validateForRewrite(queryText, modelId);
        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(getModelInferenceAsync(queryTokensSetOnce, withTokenId));
        return new NeuralSparseQueryBuilder().fieldName(fieldName)
            .queryText(queryText)
            .modelId(modelId)
            .maxTokenScore(maxTokenScore)
            .queryTokensMapSupplier(queryTokensSetOnce::get)
            .twoPhaseSharedQueryToken(twoPhaseSharedQueryToken)
            .neuralSparseQueryTwoPhaseInfo(neuralSparseQueryTwoPhaseInfo)
            .sparseAnnQueryBuilder(sparseAnnQueryBuilder);
    }

    private boolean shouldUseAnalyzer() {
        if (modelId != null && searchAnalyzer != null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Cannot use both [%s: %s] and [%s: %s] for neural sparse query tokenization. Specify only one tokenization method. These parameters can be set either in the query or through the %s search processor.",
                    MODEL_ID_FIELD.getPreferredName(),
                    modelId,
                    ANALYZER_FIELD.getPreferredName(),
                    searchAnalyzer,
                    NeuralQueryEnricherProcessor.TYPE
                )
            );
        }

        // If searchAnalyzer is not null then use it.
        if (searchAnalyzer != null) {
            return true;
        } else if (modelId == null && isClusterOnOrAfterMinReqVersionForAnalyzer()) {
            // If both are null and all nodes can support the analyzer then we should use the default analyzer
            searchAnalyzer = DEFAULT_ANALYZER;
            return true;
        }

        return false;
    }

    private boolean shouldInferenceWithTokenIdResponse(QueryRewriteContext queryRewriteContext) {
        final QueryCoordinatorContext coordinatorContext = queryRewriteContext.convertToCoordinatorContext();
        if (coordinatorContext != null) {
            final IndicesRequest searchRequest = coordinatorContext.getSearchRequest();
            for (String index : searchRequest.indices()) {
                Set<String> sparseAnnFields = SparseFieldUtils.getSparseAnnFields(index, clusterService());
                if (CollectionUtils.isNotEmpty(sparseAnnFields) && sparseAnnFields.contains(fieldName)) {
                    return true;
                }
            }
        }
        return shouldInferenceWithTokenIdResponse(queryRewriteContext.convertToShardContext());
    }

    private boolean shouldInferenceWithTokenIdResponse(QueryShardContext queryShardContext) {
        if (queryShardContext == null) {
            return false;
        }
        MappedFieldType fieldType = queryShardContext.fieldMapper(fieldName);
        return isSeismicFieldType(fieldType);
    }

    private BiConsumer<Client, ActionListener<?>> getModelInferenceAsync(SetOnce<Map<String, Float>> setOnce, boolean withTokenId) {
        // When Two-phase shared query tokens is null,
        // it set queryTokensSupplier to the inference result which has all query tokens with score.
        // When Two-phase shared query tokens exist,
        // it splits the tokens using a threshold defined by a ratio of the maximum score of tokens, updating the token set
        // accordingly.
        final AsymmetricTextEmbeddingParameters parameters = withTokenId ? TOKEN_ID_PARAMETER : null;
        return ((client, actionListener) -> ML_CLIENT.inferenceSentencesWithMapResult(
            TextInferenceRequest.builder().modelId(modelId()).inputTexts(List.of(queryText)).build(),
            parameters,
            ActionListener.wrap(mapResultList -> {
                Map<String, Float> queryTokens = TokenWeightUtil.fetchListOfTokenWeightMap(mapResultList).get(0);
                if (isSparseTwoPhaseOne()) {
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
        // There can be certain cases that we can use the queryTokensSupplier directly:
        // 1. If the raw query tokens are provided through the query.
        // 2. If we use an ML model to generate the query tokens based on the query text.
        if (Objects.nonNull(queryTokensMapSupplier) && Objects.nonNull(queryTokensMapSupplier.get())) {
            return queryTokensMapSupplier.get();
        }

        if (shouldUseAnalyzer()) {
            Map<String, Float> queryTokens = new HashMap<>();
            Analyzer luceneAnalyzer = context.getIndexAnalyzers().getAnalyzers().get(this.searchAnalyzer);
            if (Objects.isNull(luceneAnalyzer)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Analyzer [%s] not found in shard context. ", this.searchAnalyzer)
                );
            }
            boolean shouldUserQueryTokenId = shouldInferenceWithTokenIdResponse(context);
            try (TokenStream stream = luceneAnalyzer.tokenStream(fieldName, queryText)) {
                stream.reset();
                if (shouldUserQueryTokenId) {
                    TypeAttribute typeAttr = stream.addAttribute(TypeAttribute.class);
                    typeAttr.setType(SparseEmbeddingFormat.TOKEN_ID.toString());
                }
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                PayloadAttribute payload = stream.addAttribute(PayloadAttribute.class);

                while (stream.incrementToken()) {
                    String token = term.toString();
                    BytesRef bytesRefPayload = payload.getPayload();
                    float weight = Objects.isNull(bytesRefPayload) ? 1.0f : bytesToFloat(bytesRefPayload.bytes);
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
        } else {
            throw new IllegalArgumentException("Cannot convert neural sparse query to Lucene query: query tokens must not be null.");
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        boolean isSeismic = isSeismicFieldType(ft);
        if (!isSeismic) {
            validateFieldType(ft);
        }
        Map<String, Float> queryTokens = getQueryTokens(context);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            builder.add(FeatureField.newLinearQuery(fieldName, entry.getKey(), entry.getValue()), BooleanClause.Occur.SHOULD);
        }
        if (!isSeismic || sparseAnnQueryBuilder == null) {
            return builder.build();
        } else {
            QueryBuilder filter = sparseAnnQueryBuilder.filter();
            if (filter != null) {
                builder.add(filter.toQuery(context), BooleanClause.Occur.FILTER);
            }
            return sparseAnnQueryBuilder.queryTokens(queryTokens).fieldName(fieldName).fallbackQuery(builder.build()).doToQuery(context);
        }
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
        if (Objects.isNull(queryTokensMapSupplier) && Objects.nonNull(obj.queryTokensMapSupplier)) {
            return false;
        }
        if (Objects.nonNull(queryTokensMapSupplier) && Objects.isNull(obj.queryTokensMapSupplier)) {
            return false;
        }
        if (Objects.isNull(sparseAnnQueryBuilder) != Objects.isNull(obj.sparseAnnQueryBuilder)) {
            return false;
        }

        EqualsBuilder equalsBuilder = new EqualsBuilder().append(fieldName, obj.fieldName)
            .append(queryText, obj.queryText)
            .append(modelId, obj.modelId)
            .append(maxTokenScore, obj.maxTokenScore)
            .append(neuralSparseQueryTwoPhaseInfo, obj.neuralSparseQueryTwoPhaseInfo)
            .append(twoPhaseSharedQueryToken, obj.twoPhaseSharedQueryToken)
            .append(searchAnalyzer, obj.searchAnalyzer)
            .append(sparseAnnQueryBuilder, obj.sparseAnnQueryBuilder);
        if (Objects.nonNull(queryTokensMapSupplier)) {
            equalsBuilder.append(queryTokensMapSupplier.get(), obj.queryTokensMapSupplier.get());
        }
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(fieldName)
            .append(queryText)
            .append(modelId)
            .append(maxTokenScore)
            .append(neuralSparseQueryTwoPhaseInfo)
            .append(twoPhaseSharedQueryToken)
            .append(searchAnalyzer)
            .append(sparseAnnQueryBuilder);
        if (Objects.nonNull(queryTokensMapSupplier)) {
            builder.append(queryTokensMapSupplier.get());
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

    public static float bytesToFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    private boolean isSeismicSupported() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_SEISMIC);
    }

    private boolean isSeismicFieldType(MappedFieldType fieldType) {
        return isSeismicSupported() && Objects.nonNull(fieldType) && SparseVectorFieldType.isSparseVectorType(fieldType.typeName());
    }

    /**
     * Overwrite clusterService's getter function
     * @return ClusterService
     */
    public ClusterService clusterService() {
        if (clusterService == null) {
            clusterService = NeuralSearchClusterUtil.instance().getClusterService();
        }
        return clusterService;
    }
}
