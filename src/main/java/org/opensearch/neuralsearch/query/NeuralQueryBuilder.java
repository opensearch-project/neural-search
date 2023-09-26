/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import static org.opensearch.knn.index.query.KNNQueryBuilder.FILTER_FIELD;
import static org.opensearch.neuralsearch.common.VectorUtil.vectorAsListToArray;

import java.io.IOException;
import java.util.function.Supplier;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
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
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

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
@AllArgsConstructor
public class NeuralQueryBuilder extends AbstractQueryBuilder<NeuralQueryBuilder> {

    public static final String NAME = "neural";

    @VisibleForTesting
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");

    @VisibleForTesting
    static final ParseField MODEL_ID_FIELD = new ParseField("model_id");

    @VisibleForTesting
    static final ParseField K_FIELD = new ParseField("k");

    private static final int DEFAULT_K = 10;

    private static MLCommonsClientAccessor ML_CLIENT;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        NeuralQueryBuilder.ML_CLIENT = mlClient;
    }

    private String fieldName;
    private String queryText;
    private String modelId;
    private int k = DEFAULT_K;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private Supplier<float[]> vectorSupplier;
    private QueryBuilder filter;
    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_11_0;

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
        if (isClusterOnOrAfterMinRequiredVersion()) {
            this.modelId = in.readOptionalString();
        } else {
            this.modelId = in.readString();
        }
        this.k = in.readVInt();
        this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        out.writeString(this.queryText);
        if (isClusterOnOrAfterMinRequiredVersion()) {
            out.writeOptionalString(this.modelId);
        } else {
            out.writeString(this.modelId);
        }
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
        requireValue(neuralQueryBuilder.queryText(), "Query text must be provided for neural query");
        requireValue(neuralQueryBuilder.fieldName(), "Field name must be provided for neural query");
        if (!isClusterOnOrAfterMinRequiredVersion()) {
            requireValue(neuralQueryBuilder.modelId(), "Model ID must be provided for neural query");
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
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // When re-writing a QueryBuilder, if the QueryBuilder is not changed, doRewrite should return itself
        // (see
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/index/query/QueryBuilder.java#L90-L98).
        // Otherwise, it should return the modified copy (see rewrite logic
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/index/query/Rewriteable.java#L117.
        // With the asynchronous call, on first rewrite, we create a new
        // vector supplier that will get populated once the asynchronous call finishes and pass this supplier in to
        // create a new builder. Once the supplier's value gets set, we return a KNNQueryBuilder. Otherwise, we just
        // return the current unmodified query builder.
        if (vectorSupplier() != null) {
            return vectorSupplier().get() == null ? this : new KNNQueryBuilder(fieldName(), vectorSupplier.get(), k(), filter());
        }

        SetOnce<float[]> vectorSetOnce = new SetOnce<>();
        queryRewriteContext.registerAsyncAction(
            ((client, actionListener) -> ML_CLIENT.inferenceSentence(modelId(), queryText(), ActionListener.wrap(floatList -> {
                vectorSetOnce.set(vectorAsListToArray(floatList));
                actionListener.onResponse(null);
            }, actionListener::onFailure)))
        );
        return new NeuralQueryBuilder(fieldName(), queryText(), modelId(), k(), vectorSetOnce::get, filter());
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

    private static boolean isClusterOnOrAfterMinRequiredVersion() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID);
    }
}
