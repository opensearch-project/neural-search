/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.neuralsearch.sparse.SparseTokenField;
import org.opensearch.neuralsearch.sparse.SparseTokensField;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

@Getter
public class SparseTokensFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_tokens";

    private static final String METHOD = "method";
    @NonNull
    private final SparseMethodContext sparseMethodContext;
    protected boolean stored;
    protected boolean hasDocValues;
    private FieldType tokenFieldType;

    private SparseTokensFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        SparseMethodContext sparseMethodContext,
        boolean stored,
        boolean hasDocValues
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.sparseMethodContext = sparseMethodContext;
        this.stored = stored;
        this.hasDocValues = hasDocValues;
        this.fieldType = new FieldType(Defaults.FIELD_TYPE);
        this.fieldType.setDocValuesType(DocValuesType.BINARY);
        this.fieldType.freeze();

        this.tokenFieldType = new FieldType(Defaults.TOKEN_FIELD_TYPE);
        this.tokenFieldType.freeze();
    }

    private static SparseTokensFieldType ft(FieldMapper in) {
        return ((SparseTokensFieldMapper) in).fieldType();
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static class Builder extends ParametrizedFieldMapper.Builder {
        protected final Parameter<Boolean> stored = Parameter.storeParam(m -> ft(m).stored, false);
        protected final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> ft(m).hasDocValues, true);
        protected final Parameter<SparseMethodContext> sparseMethodContext = new Parameter<>(
            METHOD,
            false,
            () -> null,
            (n, c, o) -> SparseMethodContext.parse(o),
            m -> ft(m).getSparseMethodContext()
        ).setSerializer(((b, n, v) -> {
            b.startObject(n);
            v.toXContent(b, ToXContent.EMPTY_PARAMS);
            b.endObject();
        }), m -> m.getName());

        /**
         * Creates a new Builder with a field name
         *
         * @param name
         */
        protected Builder(String name) {
            super(name);
            builder = this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(stored, hasDocValues, sparseMethodContext);
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            return new SparseTokensFieldMapper(
                name,
                new SparseTokensFieldType(
                    buildFullName(context),
                    sparseMethodContext.getValue(),
                    stored.getValue(),
                    hasDocValues.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                sparseMethodContext.getValue(),
                stored.getValue(),
                hasDocValues.getValue()
            );
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected SparseTokensFieldMapper clone() {
        return (SparseTokensFieldMapper) super.clone();
    }

    @Override
    public SparseTokensFieldType fieldType() {
        return (SparseTokensFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[" + CONTENT_TYPE + "] fields can't be used in multi-fields");
        }

        if (context.parser().currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            String feature = "";
            for (XContentParser.Token token = context.parser().nextToken(); token != XContentParser.Token.END_OBJECT; token = context
                .parser()
                .nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    feature = context.parser().currentName();
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    // ignore feature, this is consistent with numeric fields
                } else if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
                    final String key = name() + "." + feature;
                    float value = context.parser().floatValue(true);
                    if (context.doc().getByKey(key) != null) {
                        throw new IllegalArgumentException(
                            "["
                                + CONTENT_TYPE
                                + "] fields do not support indexing multiple values for the same "
                                + "rank feature ["
                                + key
                                + "] in the same document"
                        );
                    }
                    SparseTokenField featureField = new SparseTokenField(name(), value, this.tokenFieldType);
                    context.doc().addWithKey(key, featureField);
                    oos.writeObject(key);
                    oos.writeFloat(value);
                } else {
                    throw new IllegalArgumentException(
                        "["
                            + CONTENT_TYPE
                            + "] fields take hashes that map a feature to a strictly positive "
                            + "float, but got unexpected token "
                            + token
                    );
                }
            }
            context.doc().add(new SparseTokensField(name(), baos.toByteArray(), fieldType));
        }
    }

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        public static final FieldType TOKEN_FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.putAttribute(SparseTokensField.SPARSE_FIELD, "true"); // This attribute helps to determine knn field type
            FIELD_TYPE.freeze();
            TOKEN_FIELD_TYPE.setTokenized(false);
            TOKEN_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            TOKEN_FIELD_TYPE.putAttribute(SparseTokensField.SPARSE_FIELD, "true"); // This attribute helps to determine knn field type
            TOKEN_FIELD_TYPE.freeze();
        }
    }
}
