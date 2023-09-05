/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.neuralsearch.index.SparseVectorMapper;

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

    private String fieldName;
    // todo: if termWeight is null
    private Map<String, Float> termWeight;

    public SparseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.termWeight = in.readMap(StreamInput::readString, StreamInput::readFloat);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeMap(termWeight, StreamOutput::writeString, StreamOutput::writeFloat);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        xContentBuilder.field(QUERY_TOKENS_FIELD.getPreferredName(), termWeight);
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
     *    "max_term_score_for_sparse_query": float (optional)
     *  }
     * }
     */
    public static SparseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        SparseQueryBuilder sparseQueryBuilder = new SparseQueryBuilder();
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Token must be START_OBJECT");
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

    // todo: refactor this to switch style
    private static void parseQueryParams(XContentParser parser, SparseQueryBuilder sparseQueryBuilder) throws IOException {
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
                } else {
                    throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (QUERY_TOKENS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                sparseQueryBuilder.termWeight(parser.map(HashMap::new, XContentParser::floatValue));
//                sparseQueryBuilder.termWeight(castToTermWeight(parser.map()));
            } else {
                throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        if (!(ft instanceof SparseVectorMapper.SparseVectorFieldType)) {
            throw new IllegalArgumentException(
                    "[" + NAME + "] query only works on [" + SparseVectorMapper.CONTENT_TYPE + "] fields, "
                            + "not ["  + ft.typeName() + "]"
            );
        }
        final Float maxTermScoreForSparseQuery = ((SparseVectorMapper.SparseVectorFieldType) ft).maxTermScoreForSparseQuery();

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Map.Entry<String, Float> entry: termWeight.entrySet()) {
                builder.add(
                        new BoostQuery(
                            new BoundedLinearFeatureQuery(
                                    fieldName,
                                    entry.getKey(),
                                    maxTermScoreForSparseQuery
                            ),
                            entry.getValue()
                    ),
                        BooleanClause.Occur.SHOULD
                );
        }
        return builder.build();
    }

    @Override
    protected boolean doEquals(SparseQueryBuilder obj) {
        // todo: validate
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(fieldName, obj.fieldName);
        equalsBuilder.append(termWeight, obj.termWeight);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return new HashCodeBuilder().append(fieldName).append(termWeight).toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static Map<String, Float> castToTermWeight(Map<String, Object> uncastMap) {
        Map<String, Float> termWeight = new HashMap<>();
        for (Map.Entry<String, Object> entry: uncastMap.entrySet()) {
            termWeight.put(entry.getKey(), ((Number) entry.getValue()).floatValue());
        }
        return termWeight;
    }
}
