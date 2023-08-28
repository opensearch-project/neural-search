/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.index;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TermBasedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NeuralSparseMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "neural_sparse";

    public NeuralSparseMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
    }

    public static class NeuralSparseBuilder extends ParametrizedFieldMapper.Builder {

        public NeuralSparseBuilder(
                String name
        ) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList();
        }

        @Override
        public NeuralSparseMapper build(BuilderContext context) {
            NeuralSparseFieldType ft = new NeuralSparseFieldType(buildFullName(context));
            return new NeuralSparseMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build());
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new NeuralSparseBuilder(n));

    public static final class NeuralSparseFieldType extends MappedFieldType {
        public NeuralSparseFieldType(
                String name,
                boolean isSearchable,
                boolean isStored,
                boolean hasDocValues,
                Boolean nullValue,
                Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
        }

        public NeuralSparseFieldType(String name) {
            this(name, true, false, true, false, Collections.emptyMap());
        }

        public NeuralSparseFieldType(String name, boolean searchable) {
            this(name, searchable, false, true, false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Now do not support fields search");
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(
                    context,
                    String.format("Neural sparse do not support exact searching now")
            );
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new NeuralSparseBuilder(simpleName());
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {

//        Boolean value = context.parseExternalValue(Boolean.class);
//        if (value == null) {
//            XContentParser.Token token = context.parser().currentToken();
//            if (token == XContentParser.Token.VALUE_NULL) {
//                if (nullValue != null) {
//                    value = nullValue;
//                }
//            } else {
//                value = context.parser().booleanValue();
//            }
//        }
        final var termWeight = context.parser().map(HashMap::new, XContentParser::floatValue);
        for (Map.Entry<String, Float> entry: termWeight.entrySet()) {
            context.doc().add(new FeatureField(fieldType().name(), entry.getKey(), entry.getValue()));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
