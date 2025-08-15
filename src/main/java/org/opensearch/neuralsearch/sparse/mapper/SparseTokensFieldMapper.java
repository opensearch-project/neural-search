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
import java.nio.charset.StandardCharsets;
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
 * Field mapper implementation for sparse tokens in neural search.
 *
 * <p>This mapper handles the indexing and mapping of sparse token fields,
 * which store sparse vector representations for neural search operations.
 * It processes JSON objects containing token-weight pairs and converts
 * them into appropriate Lucene field structures.
 *
 * <p>Key features:
 * <ul>
 *   <li>Parses JSON objects with token-weight mappings</li>
 *   <li>Creates FeatureField instances for individual tokens</li>
 *   <li>Stores binary representations of sparse vectors</li>
 *   <li>Integrates with sparse method configurations</li>
 *   <li>Supports SEISMIC algorithm parameters</li>
 * </ul>
 *
 * <p>The mapper validates input format and prevents duplicate token indexing
 * within the same document. It also handles field type configuration based
 * on the specified sparse method context.
 *
 * @see ParametrizedFieldMapper
 * @see SparseTokensField
 * @see SparseMethodContext
 */
@Getter
public class SparseTokensFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_tokens";

    private static final String METHOD = "method";
    @NonNull
    private final SparseMethodContext sparseMethodContext;
    protected boolean stored;
    protected boolean hasDocValues;
    private FieldType tokenFieldType;

    /**
     * Constructs a SparseTokensFieldMapper with the specified configuration.
     *
     * @param simpleName the simple field name
     * @param mappedFieldType the mapped field type
     * @param multiFields the multi-fields configuration
     * @param copyTo the copy-to configuration
     * @param sparseMethodContext the sparse method context
     * @param stored whether the field should be stored
     * @param hasDocValues whether the field has doc values
     */
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
        setFieldTypeAttributes(this.fieldType, sparseMethodContext);
        this.fieldType.freeze();

        this.tokenFieldType = new FieldType(Defaults.TOKEN_FIELD_TYPE);
        setFieldTypeAttributes(this.tokenFieldType, sparseMethodContext);
        this.tokenFieldType.freeze();
    }

    private static SparseTokensFieldType ft(FieldMapper in) {
        return ((SparseTokensFieldMapper) in).fieldType();
    }

    /**
     * Builder class for constructing SparseTokensFieldMapper instances.
     *
     * <p>This builder provides a fluent interface for configuring sparse token
     * field mappers with various parameters including storage options, doc values,
     * and sparse method contexts.
     */
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
         * Creates a new Builder with the specified field name.
         *
         * @param name the field name for the mapper
         */
        protected Builder(String name) {
            super(name);
            builder = this;
        }

        /**
         * Returns the list of configurable parameters for this builder.
         *
         * @return list of parameters including stored, hasDocValues, and sparseMethodContext
         */
        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(stored, hasDocValues, sparseMethodContext);
        }

        /**
         * Builds a SparseTokensFieldMapper instance with the configured parameters.
         *
         * @param context the builder context
         * @return a new SparseTokensFieldMapper instance
         */
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

    /**
     * Creates a merge builder for this field mapper.
     *
     * @return a new builder initialized with this mapper's configuration
     */
    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    /**
     * Returns the content type identifier for sparse tokens fields.
     *
     * @return the content type string
     */
    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Creates a clone of this field mapper.
     *
     * @return a cloned SparseTokensFieldMapper instance
     */
    @Override
    protected SparseTokensFieldMapper clone() {
        return (SparseTokensFieldMapper) super.clone();
    }

    /**
     * Returns the field type for this mapper.
     *
     * @return the SparseTokensFieldType instance
     */
    @Override
    public SparseTokensFieldType fieldType() {
        return (SparseTokensFieldType) super.fieldType();
    }

    /**
     * Parses and creates fields from the input JSON object.
     *
     * <p>Expects a JSON object with token-weight pairs. Creates FeatureField
     * instances for each token and stores the complete sparse vector as binary data.
     *
     * @param context the parse context containing the input data
     * @throws IOException if parsing fails or invalid input is encountered
     * @throws IllegalArgumentException if input format is invalid or contains duplicates
     */
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
                                + "rank feature ["
                                + key
                                + "] in the same document"
                        );
                    }
                    FeatureField featureField = new FeatureField(name(), feature, value);// this.tokenFieldType);
                    setFieldTypeAttributes((FieldType) featureField.fieldType(), sparseMethodContext);

                    context.doc().addWithKey(key, featureField);
                    byte[] featureBytes = feature.getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(featureBytes.length);
                    dos.write(featureBytes);
                    dos.writeFloat(value);
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
            context.doc().add(new SparseTokensField(name(), baos.toByteArray(), fieldType));
        }
    }

    private void setFieldTypeAttributes(FieldType fieldType, SparseMethodContext sparseMethodContext) {
        if (sparseMethodContext.getName().equals(SEISMIC)) {
            Integer nPostings = (Integer) sparseMethodContext.getMethodComponentContext()
                .getParameter(N_POSTINGS_FIELD, DEFAULT_N_POSTINGS);
            Float clusterRatio = sparseMethodContext.getMethodComponentContext().getFloat(CLUSTER_RATIO_FIELD, DEFAULT_CLUSTER_RATIO);
            Float summaryPruneRatio = sparseMethodContext.getMethodComponentContext()
                .getFloat(SUMMARY_PRUNE_RATIO_FIELD, DEFAULT_SUMMARY_PRUNE_RATIO);
            Integer algoTriggerThreshold = (Integer) sparseMethodContext.getMethodComponentContext()
                .getParameter(APPROXIMATE_THRESHOLD_FIELD, DEFAULT_APPROXIMATE_THRESHOLD);
            fieldType.putAttribute(N_POSTINGS_FIELD, String.valueOf(nPostings));
            fieldType.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(summaryPruneRatio));
            fieldType.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(clusterRatio));
            fieldType.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(algoTriggerThreshold));
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
            TOKEN_FIELD_TYPE.setOmitNorms(true);
            TOKEN_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            TOKEN_FIELD_TYPE.putAttribute(SparseTokensField.SPARSE_FIELD, "true"); // This attribute helps to determine knn field type
            TOKEN_FIELD_TYPE.freeze();
        }
    }

    public static class SparseTypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, parserContext, node);
            SparseMethodContext context = builder.sparseMethodContext.getValue();
            if (context == null) {
                throw new MapperParsingException("[" + CONTENT_TYPE + "] requires [method] parameter");
            }
            if (context.getName() == null) {
                throw new MapperParsingException("[" + CONTENT_TYPE + "] requires [method.name] parameter");
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
