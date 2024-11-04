/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.QueryBuilderVisitor;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery;

/**
 * Class abstract creation of a Query type "hybrid". Hybrid query will allow execution of multiple sub-queries and
 * collects score for each of those sub-query.
 */
@Log4j2
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
public final class HybridQueryBuilder extends AbstractQueryBuilder<HybridQueryBuilder> {
    public static final String NAME = "hybrid";

    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private static final ParseField DEPTH_FIELD = new ParseField("pagination_depth");

    private final List<QueryBuilder> queries = new ArrayList<>();

    private String fieldName;
    private Integer paginationDepth = null;
    static final int MAX_NUMBER_OF_SUB_QUERIES = 5;
    private final static int DEFAULT_PAGINATION_DEPTH = 10;
    private static final int LOWER_BOUND_OF_PAGINATION_DEPTH = 1;
    private static final int UPPER_BOUND_OF_PAGINATION_DEPTH = 10000;

    public HybridQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queries.addAll(readQueries(in));
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            paginationDepth = in.readOptionalInt();
        }
    }

    /**
     * Serialize this query object into input stream
     * @param out stream that we'll be used for serialization
     * @throws IOException
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeQueries(out, queries);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            out.writeOptionalInt(paginationDepth);
        }
    }

    /**
     * Add one sub-query
     * @param queryBuilder
     * @return
     */
    public HybridQueryBuilder add(QueryBuilder queryBuilder) {
        if (queryBuilder == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "inner %s query clause cannot be null", NAME));
        }
        queries.add(queryBuilder);
        return this;
    }

    /**
     * Create builder object with a content of this hybrid query
     * @param builder
     * @param params
     * @throws IOException
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder queryBuilder : queries) {
            queryBuilder.toXContent(builder, params);
        }
        builder.endArray();
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            builder.field(DEPTH_FIELD.getPreferredName(), paginationDepth == null ? DEFAULT_PAGINATION_DEPTH : paginationDepth);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    /**
     * Create query object for current hybrid query using shard context
     * @param queryShardContext context object that used to create hybrid query
     * @return hybrid query object
     * @throws IOException
     */
    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        Collection<Query> queryCollection = toQueries(queries, queryShardContext);
        if (queryCollection.isEmpty()) {
            return Queries.newMatchNoDocsQuery(String.format(Locale.ROOT, "no clauses for %s query", NAME));
        }
        return new HybridQuery(queryCollection, paginationDepth);
    }

    /**
     * Creates HybridQueryBuilder from xContent.
     * Example of a json for Hybrid Query:
     * {
     *      "query": {
     *          "hybrid": {
     *              "queries": [
     *                  {
     *                      "neural": {
     *                          "text_knn": {
     *                                    "query_text": "Hello world",
     *                                    "model_id": "dcsdcasd",
     *                                    "k": 10
     *                                }
     *                            }
     *                   },
     *                   {
     *                      "term": {
     *                          "text": "keyword"
     *                       }
     *                    }
     *               ]
     *          }
     *     }
     * }
     *
     * @param parser parser that has been initialized with the query content
     * @return new instance of HybridQueryBuilder
     * @throws IOException
     */
    public static HybridQueryBuilder fromXContent(XContentParser parser) throws IOException {
        log.info("fromXContent called");
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        Integer paginationDepth = null;
        final List<QueryBuilder> queries = new ArrayList<>();
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queries.add(parseInnerQueryBuilder(parser));
                } else {
                    log.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, currentFieldName));
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME)
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERIES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while (token != XContentParser.Token.END_ARRAY) {
                        if (queries.size() == MAX_NUMBER_OF_SUB_QUERIES) {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                String.format(Locale.ROOT, "Number of sub-queries exceeds maximum supported by [%s] query", NAME)
                            );
                        }
                        queries.add(parseInnerQueryBuilder(parser));
                        token = parser.nextToken();
                    }
                } else {
                    log.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, currentFieldName));
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME)
                    );
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                    // regular boost functionality is not supported, user should use score normalization methods to manipulate with scores
                    if (boost != DEFAULT_BOOST) {
                        log.error("[{}] query does not support provided value {} for [{}]", NAME, boost, BOOST_FIELD);
                        throw new ParsingException(parser.getTokenLocation(), "[{}] query does not support [{}]", NAME, BOOST_FIELD);
                    }
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (DEPTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    paginationDepth = parser.intValue();
                } else {
                    log.error(String.format(Locale.ROOT, "[%s] query does not support [%s]", NAME, currentFieldName));
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "Field is not supported by [%s] query", NAME)
                    );
                }
            }
        }

        if (queries.isEmpty()) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "[%s] requires 'queries' field with at least one clause", NAME)
            );
        }

        HybridQueryBuilder compoundQueryBuilder = new HybridQueryBuilder();
        compoundQueryBuilder.queryName(queryName);
        compoundQueryBuilder.boost(boost);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            validatePaginationDepth(paginationDepth);
            compoundQueryBuilder.paginationDepth(paginationDepth);
        }
        for (QueryBuilder query : queries) {
            compoundQueryBuilder.add(query);
        }
        return compoundQueryBuilder;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        HybridQueryBuilder newBuilder = new HybridQueryBuilder();
        boolean changed = false;
        for (QueryBuilder query : queries) {
            QueryBuilder result = query.rewrite(queryShardContext);
            if (result != query) {
                changed = true;
            }
            newBuilder.add(result);
        }
        if (changed) {
            newBuilder.queryName(queryName);
            newBuilder.boost(boost);
            if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
                newBuilder.paginationDepth(paginationDepth);
            }
            return newBuilder;
        } else {
            return this;
        }
    }

    /**
     * Indicates whether some other QueryBuilder object of the same type is "equal to" this one.
     * @param obj
     * @return true if objects are equal
     */
    @Override
    protected boolean doEquals(HybridQueryBuilder obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(fieldName, obj.fieldName);
        equalsBuilder.append(queries, obj.queries);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            equalsBuilder.append(paginationDepth, obj.paginationDepth);
        }
        return equalsBuilder.isEquals();
    }

    /**
     * Create hash code for current hybrid query builder object
     * @return hash code
     */
    @Override
    protected int doHashCode() {
        return Objects.hash(queries);
    }

    /**
     * Returns the name of the writeable object
     * @return
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    private List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        return in.readNamedWriteableList(QueryBuilder.class);
    }

    private void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeNamedWriteableList(queries);
    }

    private Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, QueryShardContext context) throws QueryShardException {
        List<Query> queries = queryBuilders.stream().map(qb -> {
            try {
                return qb.rewrite(context).toQuery(context);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        return queries;
    }

    private static void validatePaginationDepth(Integer paginationDepth) {
        if (paginationDepth != null
            && (paginationDepth < LOWER_BOUND_OF_PAGINATION_DEPTH || paginationDepth > UPPER_BOUND_OF_PAGINATION_DEPTH)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Pagination depth should lie in the range of 1-1000. Received: %s", paginationDepth)
            );
        }
    }

    /**
     * visit method to parse the HybridQueryBuilder by a visitor
     */
    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        // getChildVisitor of NeuralSearchQueryVisitor return this.
        // therefore any argument can be passed. Here we have used Occcur.MUST as an argument.
        QueryBuilderVisitor subVisitor = visitor.getChildVisitor(Occur.MUST);
        for (QueryBuilder subQueryBuilder : queries) {
            subQueryBuilder.visit(subVisitor);
        }
    }
}
