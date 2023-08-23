/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.queries.payloads.PayloadDecoder;
import org.apache.lucene.queries.payloads.PayloadFunction;
import org.apache.lucene.queries.payloads.PayloadScoreQuery;
import org.apache.lucene.queries.payloads.SumPayloadFunction;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.index.analysis.BertAnalyzer;
import org.opensearch.neuralsearch.ml.MLCommonsNeuralSparseClientAccessor;
import org.opensearch.neuralsearch.util.SpanQueryUtil;

import com.google.common.annotations.VisibleForTesting;

@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class NeuralSparseQueryBuilder extends AbstractQueryBuilder<NeuralSparseQueryBuilder> {
    public static final String NAME = "neural_sparse";

    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");

    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");

    @VisibleForTesting
    static final ParseField FILTER_FIELD = new ParseField("filter");

    @VisibleForTesting
    static final ParseField K_FIELD = new ParseField("k");

    private static MLCommonsNeuralSparseClientAccessor ML_CLIENT;

    private static final int DEFAULT_K = 10;

    public static void initialize(MLCommonsNeuralSparseClientAccessor mlClient) {
        NeuralSparseQueryBuilder.ML_CLIENT = mlClient;
    }

    private String fieldName;
    private String queryText;
    private String modelId;
    private int k = DEFAULT_K;

    private QueryBuilder filter;

    public NeuralSparseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.queryText = in.readString();
        this.modelId = in.readString();
        this.k = in.readVInt();
        this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeString(this.queryText);
        out.writeString(this.modelId);
        out.writeVInt(this.k);
        out.writeOptionalNamedWriteable(this.filter);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        xContentBuilder.startObject(fieldName);
        xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        xContentBuilder.field(MODEL_ID_FIELD.getPreferredName(), modelId);
        xContentBuilder.field(K_FIELD.getPreferredName(), k);
        if (filter != null) {
            xContentBuilder.field(FILTER_FIELD.getPreferredName(), filter);
        }
        printBoostAndQueryName(xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    public static NeuralSparseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        NeuralSparseQueryBuilder neuralQueryBuilder = new NeuralSparseQueryBuilder();
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
        requireValue(neuralQueryBuilder.queryText(), "Query text must be provided for neural query");
        requireValue(neuralQueryBuilder.fieldName(), "Field name must be provided for neural query");
        requireValue(neuralQueryBuilder.modelId(), "Model ID must be provided for neural query");

        return neuralQueryBuilder;
    }

    private static void parseQueryParams(XContentParser parser, NeuralSparseQueryBuilder neuralQueryBuilder) throws IOException {
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
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralQueryBuilder.filter(parseInnerQueryBuilder(parser));
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
    protected Query doToQuery(QueryShardContext context) throws IOException {
        log.info("start building query [" + fieldName + ", " + queryText + "]");
        Analyzer analyzer = new BertAnalyzer();
        SpanQuery spanQuery;
        try {
            spanQuery = SpanQueryUtil.getSpanOrTermQuery(analyzer, fieldName, queryText);
        } catch (ParserException e) {
            spanQuery = null;
        }
        PayloadFunction payloadFunction = new SumPayloadFunction();

        PayloadDecoder payloadDecoder = (BytesRef payload) -> payload == null
            ? 1
            : PayloadHelper.decodeFloat(payload.bytes, payload.offset);

        log.info("last to building query");
        return new PayloadScoreQuery(spanQuery, payloadFunction, payloadDecoder, false);
    }

    @Override
    protected boolean doEquals(NeuralSparseQueryBuilder obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(fieldName, obj.fieldName);
        equalsBuilder.append(queryText, obj.queryText);
        equalsBuilder.append(modelId, obj.modelId);
        equalsBuilder.append(k, obj.k);
        equalsBuilder.append(filter, obj.filter);
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
