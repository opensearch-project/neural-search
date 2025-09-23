/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.common.ValidationException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.neuralsearch.sparse.algorithm.SparseAlgoType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_APPROXIMATE_THRESHOLD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_CLUSTER_RATIO;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_SUMMARY_PRUNE_RATIO;

/**
 * Field mapper for sparse vector fields with feature-based indexing.
 */
@Getter
public class SparseVectorFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_vector";

    private static final String METHOD = "method";
    @NonNull
    private final SparseMethodContext sparseMethodContext;
    private FieldType tokenFieldType;

    private SparseVectorFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        SparseMethodContext sparseMethodContext
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.sparseMethodContext = sparseMethodContext;
        this.fieldType = new FieldType(Defaults.FIELD_TYPE);
        this.fieldType.setDocValuesType(DocValuesType.BINARY);
        setFieldTypeAttributes(this.fieldType, sparseMethodContext);
        this.fieldType.freeze();

        this.tokenFieldType = new FieldType(Defaults.TOKEN_FIELD_TYPE);
        setFieldTypeAttributes(this.tokenFieldType, sparseMethodContext);
        this.tokenFieldType.freeze();
    }

    private static SparseVectorFieldType ft(FieldMapper in) {
        return ((SparseVectorFieldMapper) in).fieldType();
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {
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

        protected Builder(String name) {
            super(name);
            builder = this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(sparseMethodContext);
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            return new SparseVectorFieldMapper(
                name,
                new SparseVectorFieldType(buildFullName(context), sparseMethodContext.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                sparseMethodContext.getValue()
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
    protected SparseVectorFieldMapper clone() {
        return (SparseVectorFieldMapper) super.clone();
    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
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

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(baos)) {
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
                                + "key ["
                                + key
                                + "] in the same document"
                        );
                    }
                    FeatureField featureField = new FeatureField(name(), feature, value);
                    context.doc().addWithKey(key, featureField);

                    try {
                        int tokenIndex = Integer.parseInt(feature);
                        if (tokenIndex < 0) {
                            throw new IllegalArgumentException("[" + CONTENT_TYPE + "]" + " fields should be text of non-negative integer");
                        }
                        dos.writeInt(tokenIndex);
                        dos.writeFloat(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("[" + CONTENT_TYPE + "]" + " fields should be valid integer");
                    }
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
            dos.flush();
            context.doc().add(new SparseVectorField(name(), baos.toByteArray(), fieldType));
        }
    }

    private void setFieldTypeAttributes(FieldType fieldType, SparseMethodContext sparseMethodContext) {
        if (sparseMethodContext.getName().equals(SEISMIC)) {
            Integer nPostings = (Integer) sparseMethodContext.getMethodComponentContext()
                .getParameter(N_POSTINGS_FIELD, DEFAULT_N_POSTINGS);
            Float clusterRatio = sparseMethodContext.getMethodComponentContext()
                .getFloatParameter(CLUSTER_RATIO_FIELD, DEFAULT_CLUSTER_RATIO);
            Float summaryPruneRatio = sparseMethodContext.getMethodComponentContext()
                .getFloatParameter(SUMMARY_PRUNE_RATIO_FIELD, DEFAULT_SUMMARY_PRUNE_RATIO);
            Integer algoTriggerThreshold = (Integer) sparseMethodContext.getMethodComponentContext()
                .getParameter(APPROXIMATE_THRESHOLD_FIELD, DEFAULT_APPROXIMATE_THRESHOLD);
            fieldType.putAttribute(N_POSTINGS_FIELD, String.valueOf(nPostings));
            fieldType.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(summaryPruneRatio));
            fieldType.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(clusterRatio));
            fieldType.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(algoTriggerThreshold));
        }
    }

    /**
     * Default field type configurations.
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        public static final FieldType TOKEN_FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.putAttribute(SparseVectorField.SPARSE_FIELD, "true"); // This attribute helps to determine knn field type
            FIELD_TYPE.freeze();
            TOKEN_FIELD_TYPE.setTokenized(false);
            TOKEN_FIELD_TYPE.setOmitNorms(true);
            TOKEN_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            TOKEN_FIELD_TYPE.putAttribute(SparseVectorField.SPARSE_FIELD, "true"); // This attribute helps to determine knn field type
            TOKEN_FIELD_TYPE.freeze();
        }
    }

    /**
     * Parser for sparse tokens field type.
     */
    public static class SparseTypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, parserContext, node);
            SparseMethodContext context = builder.sparseMethodContext.getValue();
            if (context == null) {
                throw new MapperParsingException("[" + CONTENT_TYPE + "] requires [method] parameter");
            }
            if (!SparseAlgoType.SEISMIC.getName().equals(context.getName())) {
                throw new MapperParsingException("[method.name]: " + context.getName() + " is not supported");
            }
            ValidationException exception = SparseAlgoType.SEISMIC.validateMethod(context);
            if (exception != null) {
                throw new MapperParsingException(exception.getMessage());
            }
            return builder;
        }
    }
}
