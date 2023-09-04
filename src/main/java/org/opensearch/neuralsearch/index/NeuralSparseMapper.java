/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.Query;
import org.opensearch.core.xcontent.XContentParser;
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
public class NeuralSparseMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_vector";

    protected NeuralSparseMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
    }

    public static class NeuralSparseBuilder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public NeuralSparseBuilder(
                String name
        ) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(meta);
        }

        @Override
        public NeuralSparseMapper build(BuilderContext context) {
            return new NeuralSparseMapper(
                    name,
                    new NeuralSparseFieldType(buildFullName(context), meta.getValue()),
                    multiFieldsBuilder.build(this, context),
                    copyTo.build()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new NeuralSparseBuilder(n));

    public static final class NeuralSparseFieldType extends MappedFieldType {
        public NeuralSparseFieldType(
                String name,
                boolean isSearchable,
                boolean isStored,
                boolean hasDocValues,
                Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.NONE, meta);
        }

        public NeuralSparseFieldType(
                String name,
                Map<String, String> meta
        ) {
            this(name, true, false, true, meta);
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

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new NeuralSparseBuilder(simpleName()).init(this);
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
