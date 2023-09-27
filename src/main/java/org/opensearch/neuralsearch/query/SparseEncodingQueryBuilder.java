/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
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
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import com.google.common.annotations.VisibleForTesting;

/**
 * SparseEncodingQueryBuilder is responsible for handling "sparse_encoding" query types. It uses an ML SPARSE_ENCODING model
 * or SPARSE_TOKENIZE model to produce a Map with String keys and Float values for input text. Then it will be transformed
 * to Lucene FeatureQuery wrapped by Lucene BooleanQuery.
 */

@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class SparseEncodingQueryBuilder extends AbstractQueryBuilder<SparseEncodingQueryBuilder> {
    public static final String NAME = "sparse_encoding";
    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    @VisibleForTesting
    static final ParseField MAX_TOKEN_SCORE_FIELD = new ParseField("max_token_score");

    private static MLCommonsClientAccessor ML_CLIENT;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        SparseEncodingQueryBuilder.ML_CLIENT = mlClient;
    }

    private String fieldName;
    private String queryText;
    private String modelId;
    private Float maxTokenScore;
    private Supplier<Map<String, Float>> queryTokensSupplier;

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public SparseEncodingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryText = in.readString();
        this.modelId = in.readString();
        this.maxTokenScore = in.readOptionalFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(queryText);
        out.writeString(modelId);
        out.writeOptionalFloat(maxTokenScore);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        if (null != maxTokenScore) xContentBuilder.field(MAX_TOKEN_SCORE_FIELD.getPreferredName(), maxTokenScore);
        printBoostAndQueryName(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    /**
     * The expected parsing form looks like:
     *  "SAMPLE_FIELD": {
     *    "query_text": "string",
     *    "model_id": "string",
     *    "token_score_upper_bound": float (optional)
     *  }
     *
     * @param parser XContentParser
     * @return NeuralQueryBuilder
     * @throws IOException can be thrown by parser
     */
    public static SparseEncodingQueryBuilder fromXContent(XContentParser parser) throws IOException {
        SparseEncodingQueryBuilder sparseEncodingQueryBuilder = new SparseEncodingQueryBuilder();
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
        requireValue(
            sparseEncodingQueryBuilder.queryText(),
            String.format(Locale.ROOT, "%s field must be provided for [%s] query", QUERY_TEXT_FIELD.getPreferredName(), NAME)
        );
        requireValue(
            sparseEncodingQueryBuilder.modelId(),
            String.format(Locale.ROOT, "%s field must be provided for [%s] query", MODEL_ID_FIELD.getPreferredName(), NAME)
        );

        return sparseEncodingQueryBuilder;
    }

    private static void parseQueryParams(XContentParser parser, SparseEncodingQueryBuilder sparseEncodingQueryBuilder) throws IOException {
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
                } else if (MAX_TOKEN_SCORE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseEncodingQueryBuilder.maxTokenScore(parser.floatValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] query does not support [%s] field", NAME, currentFieldName)
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
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // We need to inference the sentence to get the queryTokens. The logic is similar to NeuralQueryBuilder
        // If the inference is finished, then rewrite to self and call doToQuery, otherwise, continue doRewrite
        if (null != queryTokensSupplier) {
            return this;
        }

        validateForRewrite(queryText, modelId);
        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            ((client, actionListener) -> ML_CLIENT.inferenceSentencesWithMapResult(
                modelId(),
                List.of(queryText),
                ActionListener.wrap(mapResultList -> {
                    queryTokensSetOnce.set(TokenWeightUtil.fetchListOfTokenWeightMap(mapResultList).get(0));
                    actionListener.onResponse(null);
                }, actionListener::onFailure)
            ))
        );
        return new SparseEncodingQueryBuilder().fieldName(fieldName)
            .queryText(queryText)
            .modelId(modelId)
            .maxTokenScore(maxTokenScore)
            .queryTokensSupplier(queryTokensSetOnce::get);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        validateFieldType(ft);

        Map<String, Float> queryTokens = queryTokensSupplier.get();
        validateQueryTokens(queryTokens);

        final Float scoreUpperBound = null != maxTokenScore ? maxTokenScore : Float.MAX_VALUE;

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            builder.add(
                new BoostQuery(new BoundedLinearFeatureQuery(fieldName, entry.getKey(), scoreUpperBound), entry.getValue()),
                BooleanClause.Occur.SHOULD
            );
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
        if (null == fieldType || !fieldType.typeName().equals("rank_features")) {
            throw new IllegalArgumentException("[" + NAME + "] query only works on [rank_features] fields");
        }
    }

    private static void validateQueryTokens(Map<String, Float> queryTokens) {
        if (null == queryTokens) {
            throw new IllegalArgumentException("Query tokens cannot be null.");
        }
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            if (entry.getValue() <= 0) {
                throw new IllegalArgumentException(
                    "Feature weight must be larger than 0, feature [" + entry.getValue() + "] has negative weight."
                );
            }
        }
    }

    @Override
    protected boolean doEquals(SparseEncodingQueryBuilder obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder().append(fieldName, obj.fieldName)
            .append(queryText, obj.queryText)
            .append(modelId, obj.modelId)
            .append(maxTokenScore, obj.maxTokenScore);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return new HashCodeBuilder().append(fieldName).append(queryText).append(modelId).append(maxTokenScore).toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
