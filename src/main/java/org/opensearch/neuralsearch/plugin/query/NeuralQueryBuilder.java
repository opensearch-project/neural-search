/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin.query;

import java.io.IOException;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.common.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import com.google.common.annotations.VisibleForTesting;

/**
 * NeuralQueryBuilder is responsible for producing "neural" query types. A "neural" query type is a wrapper around a
 * k-NN vector query. It uses a ML language model to produce a dense vector from a query string that is then used as
 * the query vector for the k-NN search.
 */

@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
public class NeuralQueryBuilder extends AbstractQueryBuilder<NeuralQueryBuilder> {

    public static final String NAME = "neural";

    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");

    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");

    @VisibleForTesting
    static final ParseField K_FIELD = new ParseField("k");

    private static final int DEFAULT_K = 10;

    private String fieldName;
    private String queryText;
    private String modelId;
    private int k = DEFAULT_K;

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public NeuralQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryText = in.readString();
        this.modelId = in.readString();
        this.k = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeString(this.queryText);
        out.writeString(this.modelId);
        out.writeVInt(this.k);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        xContentBuilder.field(K_FIELD.getPreferredName(), k);
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
     *    "boost": float (optional)
     *  }
     * }
     *
     * @param parser XContentParser
     * @return NeuralQueryBuilder
     * @throws IOException can be thrown by parser
     */
    public static NeuralQueryBuilder fromXContent(XContentParser parser) throws IOException {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Token must be START_OBJECT");
        }
        parser.nextToken();
        neuralQueryBuilder.fieldName(parser.currentName());
        parser.nextToken();
        parseQueryParams(parser, neuralQueryBuilder);
        parser.nextToken();
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(
                    "[%s] query doesn't support multiple fields, found [%s] and [%s]",
                    NAME,
                    neuralQueryBuilder.fieldName(),
                    parser.currentName()
                )
            );
        }
        requireValue(neuralQueryBuilder.queryText(), "Query text must be provided for neural query");
        requireValue(neuralQueryBuilder.fieldName(), "Field name must be provided for neural query");
        requireValue(neuralQueryBuilder.modelId(), "Model ID must be provided for neural query");

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
                } else if (MODEL_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.modelId(parser.text());
                } else if (K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.k((Integer) NumberFieldMapper.NumberType.INTEGER.parse(parser.objectBytes(), false));
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.queryName(parser.text());
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.boost(parser.floatValue());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
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
    protected Query doToQuery(QueryShardContext queryShardContext) {
        // TODO Implement logic to build KNNQuery in this method
        return null;
    }

    @Override
    protected boolean doEquals(NeuralQueryBuilder obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(fieldName, obj.fieldName);
        equalsBuilder.append(queryText, obj.queryText);
        equalsBuilder.append(modelId, obj.modelId);
        equalsBuilder.append(k, obj.k);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return new HashCodeBuilder().append(fieldName).append(queryText).append(modelId).append(k).toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
