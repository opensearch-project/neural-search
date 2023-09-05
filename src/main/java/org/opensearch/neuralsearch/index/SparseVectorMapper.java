/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.Query;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A FieldMapper that exposes Lucene's {@link FeatureField}.
 * It is designed for learned sparse vectors, the expected for ingested content is a map of (token,weight) pairs, with String and Float type.
 * In current version, this field doesn't support existing query clauses like "match" or "exists".
 * The ingested documents can only be searched with our "sparse" query clause.
 */
public class SparseVectorMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_vector";

    private static SparseVectorMapper toType(FieldMapper in) {
        return (SparseVectorMapper) in;
    }

    public static class SparseVectorBuilder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        // Both match query and our sparse query use lucene Boolean query to connect all term-level queries.
        // lucene BooleanQuery use WAND (Weak AND) algorithm to accelerate the search, and WAND algorithm
        // uses term's max possible value to skip unnecessary calculations. The max possible value in match clause is term idf value.
        // However, The default behavior of lucene FeatureQuery is returning Float.MAX_VALUE for every term. Which will
        // invalidate WAND algorithm.

        // By setting maxTermScoreForSparseQuery, we'll use it as the term score upperbound to accelerate the search.
        // Users can also overwrite this setting in sparse query. Our experiments show a proper maxTermScoreForSparseQuery
        // value can reduce search latency by 4x while losing precision less than 0.5%.

        // If user doesn't set the value explicitly, we'll degrade to the default behavior in lucene FeatureQuery,
        // i.e. using Float.MAX_VALUE.
        private final Parameter<Float> maxTermScoreForSparseQuery = Parameter.floatParam(
                "max_term_score_for_sparse_query",
                false,
                m -> toType(m).maxTermScoreForSparseQuery,
                Float.MAX_VALUE
        );

        public SparseVectorBuilder(
                String name
        ) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(maxTermScoreForSparseQuery, meta);
        }

        @Override
        public SparseVectorMapper build(BuilderContext context) {
            return new SparseVectorMapper(
                name,
                new SparseVectorFieldType(buildFullName(context), meta.getValue(), maxTermScoreForSparseQuery.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                maxTermScoreForSparseQuery.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new SparseVectorBuilder(n));

    public static final class SparseVectorFieldType extends MappedFieldType {
        private final float maxTermScoreForSparseQuery;

        public SparseVectorFieldType(
                String name,
                Map<String, String> meta,
                float maxTermScoreForSparseQuery
        ) {
            super(name, true, false, true, TextSearchInfo.NONE, meta);
            this.maxTermScoreForSparseQuery = maxTermScoreForSparseQuery;
        }

        public float maxTermScoreForSparseQuery() {
            return maxTermScoreForSparseQuery;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support format");
            }
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new QueryShardException(
                    context,
                    "Field [" + name() + "] of type [" + typeName() + "] does not support exists query for now"
            );
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(
                    context,
                    "Field [" + name() + "] of type [" + typeName() + "] does not support term query for now"
            );
        }
    }

    private final float maxTermScoreForSparseQuery;

    protected SparseVectorMapper(
            String simpleName,
            MappedFieldType mappedFieldType,
            MultiFields multiFields,
            CopyTo copyTo,
            float maxTermScoreForSparseQuery
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.maxTermScoreForSparseQuery = maxTermScoreForSparseQuery;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new SparseVectorBuilder(simpleName()).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (XContentParser.Token.START_OBJECT != context.parser().currentToken()) {
            throw new IllegalArgumentException(
                    "Wrong format for input data. Field type " + typeName() + " can only parse <String, Float> map object."

            );
        }
        final Map<String, Float> termWeight = context.parser().map(HashMap::new, XContentParser::floatValue);
        for (Map.Entry<String, Float> entry: termWeight.entrySet()) {
            context.doc().add(new FeatureField(fieldType().name(), entry.getKey(), entry.getValue()));
        }
    }

    // Users are not supposed to give an array for the input value.
    // Here we set the return value of parsesArrayValue() as true,
    // intercept the request and throw an exception in parseCreateField()
    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
