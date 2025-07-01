/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import lombok.Builder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;

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
@Builder
public class SparseAnnQueryBuilder extends AbstractQueryBuilder<SparseAnnQueryBuilder> {
    public static final String NAME = "sparse_ann";
    @VisibleForTesting
    static final ParseField CUT_FIELD = new ParseField("cut");
    @VisibleForTesting
    static final ParseField TOP_K_FIELD = new ParseField("k");
    @VisibleForTesting
    static final ParseField HEAP_FACTOR_FIELD = new ParseField("heap_factor");
    @VisibleForTesting
    public static final ParseField METHOD_PARAMETERS_FIELD = new ParseField("method_parameters");
    private String fieldName;
    private Integer queryCut;
    private Integer k;
    private Float heapFactor;
    private QueryBuilder filter;
    private Query fallbackQuery;
    private Map<String, Float> queryTokens;

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_13_0;
    private static final int DEFAULT_TOP_K = 10;
    private static final float DEFAULT_HEAP_FACTOR = 1.0f;

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public SparseAnnQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queryCut = in.readOptionalInt();
        this.k = in.readOptionalInt();
        this.heapFactor = in.readOptionalFloat();
    }

    public static SparseAnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String methodFieldName = "";
        Token token = parser.currentToken();
        if (token != Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "[%s] %s must be an object", NAME, METHOD_PARAMETERS_FIELD.getPreferredName())
            );
        }
        SparseAnnQueryBuilderBuilder builder = SparseAnnQueryBuilder.builder();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                methodFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CUT_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.queryCut = parser.intValue();
                } else if (TOP_K_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.k = parser.intValue();
                } else if (HEAP_FACTOR_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.heapFactor = parser.floatValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] unknown field [%s]", NAME, methodFieldName)
                    );
                }
            }
        }
        return builder.build();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(this.queryCut);
        out.writeOptionalInt(this.k);
        out.writeOptionalFloat(this.heapFactor);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
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
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        return new SparseAnnQueryBuilder().fieldName(fieldName)
            .queryCut(queryCut)
            .k(k)
            .filter(filter)
            .fallbackQuery(fallbackQuery)
            .heapFactor(heapFactor);
    }

    private SparseQueryContext constructSparseQueryContext() {
        int n = queryCut == null ? queryTokens.size() : Math.min(queryCut, queryTokens.size());
        List<String> topTokens = queryTokens.entrySet()
            .stream()
            .sorted(Map.Entry.<String, Float>comparingByValue().reversed()) // Sort by values in descending order
            .limit(n) // Take only top N elements
            .map(Map.Entry::getKey)
            .toList();

        return SparseQueryContext.builder()
            .tokens(topTokens)
            .heapFactor(heapFactor == null ? DEFAULT_HEAP_FACTOR : heapFactor)
            .k((k == null || k == 0) ? DEFAULT_TOP_K : k)
            .build();
    }

    @Override
    public Query doToQuery(QueryShardContext context) throws IOException {
        final MappedFieldType ft = context.fieldMapper(fieldName);
        validateFieldType(ft);

        SparseQueryContext sparseQueryContext = constructSparseQueryContext();

        return new SparseVectorQuery.SparseVectorQueryBuilder().fieldName(fieldName)
            .queryContext(sparseQueryContext)
            .queryVector(new SparseVector(queryTokens))
            .originalQuery(fallbackQuery)
            .build();
    }

    public static void validateFieldType(MappedFieldType fieldType) {
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
        EqualsBuilder equalsBuilder = new EqualsBuilder().append(queryCut, obj.queryCut)
            .append(heapFactor, obj.heapFactor)
            .append(k, obj.k);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(queryCut).append(heapFactor).append(k);
        return builder.toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
