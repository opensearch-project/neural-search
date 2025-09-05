/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * SparseAnnQueryBuilder is a sub query builder of NeuralSparseQueryBuilder.
 * It's responsible for handling "method_parameters" from "neural_sparse" query types when it works SEISMIC index.
 * It wraps a NeuralSparseQueryBuilder so that it can determine when to fall back to plain neural sparse query.
 */
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@Builder
public class SparseAnnQueryBuilder extends AbstractQueryBuilder<SparseAnnQueryBuilder> {
    public static final String NAME = NeuralSparseQueryBuilder.NAME;
    @VisibleForTesting
    public static final ParseField TOP_N_FIELD = new ParseField("top_n");
    @VisibleForTesting
    public static final ParseField TOP_K_FIELD = new ParseField("k");
    @VisibleForTesting
    public static final ParseField HEAP_FACTOR_FIELD = new ParseField("heap_factor");
    @VisibleForTesting
    public static final ParseField METHOD_PARAMETERS_FIELD = new ParseField("method_parameters");
    @VisibleForTesting
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    private String fieldName;
    private Integer queryCut;
    private Integer k;
    private Float heapFactor;
    private QueryBuilder filter;
    private Query fallbackQuery;
    @Setter(lombok.AccessLevel.NONE)
    private Map<String, Float> queryTokens;

    private static final int DEFAULT_TOP_K = 10;
    private static final int DEFAULT_QUERY_CUT = 10;
    private static final float DEFAULT_HEAP_FACTOR = 1.0f;

    public SparseAnnQueryBuilder(
        String fieldName,
        Integer queryCut,
        Integer k,
        Float heapFactor,
        QueryBuilder filter,
        Query fallbackQuery,
        Map<String, Float> queryTokens
    ) {
        this.fieldName = fieldName;
        this.queryCut = queryCut;
        this.k = k;
        this.heapFactor = heapFactor;
        this.filter = filter;
        this.fallbackQuery = fallbackQuery;
        this.queryTokens = preprocessQueryTokens(queryTokens);
    }

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
        this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public void setQueryTokens(Map<String, Float> queryTokens) {
        this.queryTokens = preprocessQueryTokens(queryTokens);
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
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                methodFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TOP_N_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.queryCut = parser.intValue();
                    if (builder.queryCut <= 0) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(Locale.ROOT, "[%s] %s must be a positive integer", NAME, TOP_N_FIELD.getPreferredName())
                        );
                    }
                } else if (TOP_K_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.k = parser.intValue();
                    if (builder.k <= 0) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(Locale.ROOT, "[%s] %s must be a positive integer", NAME, TOP_K_FIELD.getPreferredName())
                        );
                    }
                } else if (HEAP_FACTOR_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.heapFactor = parser.floatValue();
                    if (builder.heapFactor <= 0) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(Locale.ROOT, "[%s] %s must be a positive float", NAME, HEAP_FACTOR_FIELD.getPreferredName())
                        );
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] unknown field [%s]", NAME, methodFieldName)
                    );
                }
            } else if (FILTER_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                QueryBuilder filterQueryBuilder = parseInnerQueryBuilder(parser);
                builder.filter(filterQueryBuilder);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "[%s] unknown token [%s] after [%s]", NAME, token, methodFieldName)
                );
            }
        }
        return builder.build();
    }

    public static class SparseAnnQueryBuilderBuilder {
        public SparseAnnQueryBuilderBuilder queryTokens(Map<String, Float> queryTokens) {
            this.queryTokens = preprocessQueryTokens(queryTokens);
            return this;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(this.queryCut);
        out.writeOptionalInt(this.k);
        out.writeOptionalFloat(this.heapFactor);
        out.writeOptionalNamedWriteable(this.filter);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        if (Objects.nonNull(queryCut)) {
            xContentBuilder.field(TOP_N_FIELD.getPreferredName(), queryCut);
        }
        if (Objects.nonNull(k)) {
            xContentBuilder.field(TOP_K_FIELD.getPreferredName(), k);
        }
        if (Objects.nonNull(heapFactor)) {
            xContentBuilder.field(HEAP_FACTOR_FIELD.getPreferredName(), heapFactor);
        }
        if (Objects.nonNull(filter)) {
            xContentBuilder.field(FILTER_FIELD.getPreferredName(), filter);
        }
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
        int n = queryCut == null ? DEFAULT_QUERY_CUT : queryCut;
        n = Math.min(n, queryTokens.size());
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

        Query filterQuery = null;
        if (filter != null) {
            filterQuery = filter.toQuery(context);
        }
        Map<Integer, Float> integerTokens = new HashMap<>();
        for (Map.Entry<String, Float> entry : queryTokens.entrySet()) {
            int token = Integer.parseInt(entry.getKey());
            integerTokens.put(token, entry.getValue());
        }
        return new SparseVectorQuery.SparseVectorQueryBuilder().fieldName(fieldName)
            .queryContext(sparseQueryContext)
            .queryVector(new SparseVector(integerTokens))
            .fallbackQuery(fallbackQuery)
            .filter(filterQuery)
            .build();
    }

    public static void validateFieldType(MappedFieldType fieldType) {
        if (Objects.isNull(fieldType) || !SparseTokensFieldType.isSparseTokensType(fieldType.typeName())) {
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] query with ["
                    + METHOD_PARAMETERS_FIELD.getPreferredName()
                    + "] only works on ["
                    + SparseTokensFieldMapper.CONTENT_TYPE
                    + "] fields"
            );
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
            .append(k, obj.k)
            .append(filter, obj.filter);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(queryCut).append(heapFactor).append(k).append(filter);
        return builder.toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static Map<String, Float> preprocessQueryTokens(Map<String, Float> tokens) {
        if (MapUtils.isEmpty(tokens)) return Collections.emptyMap();
        Map<Integer, Float> intTokens = new HashMap<>();
        try {
            for (Map.Entry<String, Float> entry : tokens.entrySet()) {
                int token = Integer.parseInt(entry.getKey());
                if (token < 0) {
                    throw new IllegalArgumentException("Query tokens should be non-negative integer!");
                }
                float value = entry.getValue();
                int tokenHash = SparseVector.prepareTokenForShortType(token);
                if (intTokens.containsKey(tokenHash)) {
                    intTokens.put(tokenHash, Math.max(intTokens.get(tokenHash), value));
                } else {
                    intTokens.put(tokenHash, value);
                }
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Query tokens should be valid integer");
        }
        return intTokens.entrySet().stream().collect(Collectors.toMap(e -> String.valueOf(e.getKey()), Map.Entry::getValue));
    }

}
