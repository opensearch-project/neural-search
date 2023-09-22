/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query.sparse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

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

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.neuralsearch.index.mapper.SparseVectorMapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class SparseQueryBuilder extends AbstractQueryBuilder<SparseQueryBuilder> {
    public static final String NAME = "sparse";
    @VisibleForTesting
    static final ParseField QUERY_TOKENS_FIELD = new ParseField("query_tokens");
    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    @VisibleForTesting
    static final ParseField TOKEN_SCORE_UPPER_BOUND_FIELD = new ParseField("token_score_upper_bound");

    private static MLCommonsClientAccessor ML_CLIENT;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        SparseQueryBuilder.ML_CLIENT = mlClient;
    }

    private String fieldName;
    private Map<String, Float> queryTokens;
    private String queryText;
    private String modelId;
    private Float tokenScoreUpperBound;
    private Supplier<Map<String, Float>> queryTokensSupplier;

    public SparseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        // we don't have readOptionalMap or write, need to do it manually
        if (in.readBoolean()) {
            this.queryTokens = in.readMap(StreamInput::readString, StreamInput::readFloat);
        }
        this.queryText = in.readOptionalString();
        this.modelId = in.readOptionalString();
        this.tokenScoreUpperBound = in.readOptionalFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        if (null != queryTokens) {
            out.writeBoolean(true);
            out.writeMap(queryTokens, StreamOutput::writeString, StreamOutput::writeFloat);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(queryText);
        out.writeOptionalString(modelId);
        out.writeOptionalFloat(tokenScoreUpperBound);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        if (null != queryTokens) xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), queryTokens);
        if (null != queryText) xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        if (null != modelId) xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        if (null != tokenScoreUpperBound)
            xContentBuilder.field(TOKEN_SCORE_UPPER_BOUND_FIELD.getPreferredName(), tokenScoreUpperBound);
        printBoostAndQueryName(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    /**
     * The expected parsing form looks like:
     * {
     *  "SAMPLE_FIELD": {
     *    "query_tokens": {
     *        "token_a": float,
     *        "token_b": float,
     *        ...
     *    },
     *    "token_score_upper_bound": float (optional)
     *  }
     * }
     * or
     *  "SAMPLE_FIELD": {
     *    "query_text": "string",
     *    "model_id": "string",
     *    "token_score_upper_bound": float (optional)
     *  }
     *
     */
    public static SparseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        SparseQueryBuilder sparseQueryBuilder = new SparseQueryBuilder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                    parser.getTokenLocation(),
                    "First token of " + NAME + "query must be START_OBJECT"
            );
        }
        parser.nextToken();
        sparseQueryBuilder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, sparseQueryBuilder);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                    parser.getTokenLocation(),
                    "["
                            + NAME
                            + "] query doesn't support multiple fields, found ["
                            + sparseQueryBuilder.fieldName()
                            + "] and ["
                            + parser.currentName()
                            + "]"
            );
        }

        return sparseQueryBuilder;
    }

    private static void parseQueryParams(
            XContentParser parser,
            SparseQueryBuilder sparseQueryBuilder
    ) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseQueryBuilder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseQueryBuilder.boost(parser.floatValue());
                } else if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseQueryBuilder.queryText(parser.text());
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseQueryBuilder.modelId(parser.text());
                } else if (TOKEN_SCORE_UPPER_BOUND_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sparseQueryBuilder.tokenScoreUpperBound(parser.floatValue());
                } else {
                    throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (QUERY_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                sparseQueryBuilder.queryTokens(parser.map(HashMap::new, XContentParser::floatValue));
            } else {
                throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // If the user has specified query_tokens field, then we don't need to inference the sentence,
        // just re-rewrite to self. Otherwise, we need to inference the sentence to get the queryTokens. Then the
        // logic is similar to NeuralQueryBuilder
        if (null != queryTokens) {
            return this;
        }
        if (null != queryTokensSupplier) {
            return queryTokensSupplier.get() == null ? this :
                    new SparseQueryBuilder()
                            .fieldName(fieldName)
                            .queryTokens(queryTokensSupplier.get())
                            .queryText(queryText)
                            .modelId(modelId)
                            .tokenScoreUpperBound(tokenScoreUpperBound);
        }

        validateForRewrite(queryText, modelId);
        SetOnce<Map<String, Float>> queryTokensSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
                ((client, actionListener) -> ML_CLIENT.inferenceSentencesWithMapResult(
                        modelId(),
                        List.of(queryText),
                        ActionListener.wrap(mapResultList -> {
                            queryTokensSetOnce.set(TokenWeightUtil.fetchQueryTokensList(mapResultList).get(0));
                            actionListener.onResponse(null);
                        }, actionListener::onFailure))
                )
        );
        return new SparseQueryBuilder()
                .fieldName(fieldName)
                .queryText(queryText)
                .modelId(modelId)
                .tokenScoreUpperBound(tokenScoreUpperBound)
                .queryTokensSupplier(queryTokensSetOnce::get);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        validateFieldType(ft);
        validateQueryTokens(queryTokens);

        // the tokenScoreUpperBound from query has higher priority
        final Float scoreUpperBound = null != tokenScoreUpperBound? tokenScoreUpperBound:
                ((SparseVectorMapper.SparseVectorFieldType) ft).tokenScoreUpperBound();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Float> entry: queryTokens.entrySet()) {
            builder.add(
                    new BoostQuery(
                            new BoundedLinearFeatureQuery(
                                    fieldName,
                                    entry.getKey(),
                                    scoreUpperBound
                            ),
                            entry.getValue()
                    ),
                    BooleanClause.Occur.SHOULD
            );
        }
        return builder.build();
    }

    private static void validateForRewrite(String queryText, String modelId) {
        if (null == queryText||null == modelId) {
            throw new IllegalArgumentException(
                    "When " + QUERY_TOKENS_FIELD.getPreferredName() + " are not provided," +
                            QUERY_TEXT_FIELD.getPreferredName() + " and " + MODEL_ID_FIELD.getPreferredName() +
                            " can not be null."
            );
        }
    }

    private static void validateFieldType(MappedFieldType fieldType) {
        if (!(fieldType instanceof SparseVectorMapper.SparseVectorFieldType)) {
            throw new IllegalArgumentException(
                    "[" + NAME + "] query only works on [" + SparseVectorMapper.CONTENT_TYPE + "] fields, "
                            + "not ["  + fieldType.typeName() + "]"
            );
        }
    }

    private static void validateQueryTokens(Map<String, Float> queryTokens) {
        if (null == queryTokens) {
            throw new IllegalArgumentException(
                    QUERY_TOKENS_FIELD.getPreferredName() + " field can not be null."
            );
        }
        for (Map.Entry<String, Float> entry: queryTokens.entrySet()) {
            if (entry.getValue() <= 0) {
                throw new IllegalArgumentException(
                        "weight must be larger than 0, got: " + entry.getValue() + "for key " + entry.getKey());
            }
        }
    }

    @Override
    protected boolean doEquals(SparseQueryBuilder obj) {
        // todo: validate
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder()
                .append(fieldName, obj.fieldName)
                .append(queryTokens, obj.queryTokens)
                .append(queryText, obj.queryText)
                .append(modelId, obj.modelId)
                .append(tokenScoreUpperBound, obj.tokenScoreUpperBound);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return new HashCodeBuilder()
                .append(fieldName)
                .append(queryTokens)
                .append(queryText)
                .append(modelId)
                .append(tokenScoreUpperBound)
                .toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}