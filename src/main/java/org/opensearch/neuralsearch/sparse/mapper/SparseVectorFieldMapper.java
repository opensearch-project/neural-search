/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.Getter;
import lombok.NonNull;
import org.opensearch.Version;
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
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseAlgoType;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_SPARSE_NATIVE_ENGINE;
import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isVersionOnOrAfterMinReqVersionForSparseNativeEngine;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTERING_BATCH_SIZE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_APPROXIMATE_THRESHOLD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_CLUSTERING_BATCH_SIZE;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_CLUSTER_RATIO;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_INGEST;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_QUANTIZATION_CEILING_SEARCH;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_SUMMARY_PRUNE_RATIO;

/**
 * Field mapper for sparse vector fields with feature-based indexing.
 */
@Getter
public class SparseVectorFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "sparse_vector";

    public static final String METHOD = "method";

    /** The {@code method.parameters} entries that came in with the native engine. */
    private static final List<String> NATIVE_ENGINE_PARAMETERS = List.of(FORWARD_INDEX_FIELD, CLUSTERING_BATCH_SIZE_FIELD);

    @NonNull
    private final SparseMethodContext sparseMethodContext;

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
    }

    private static SparseVectorFieldType ft(FieldMapper in) {
        return ((SparseVectorFieldMapper) in).fieldType();
    }

    /**
     * The Lucene field type documents are indexed with, carrying the attributes
     * {@link #setFieldTypeAttributes} resolved from the method context. Distinct from
     * {@link #fieldType()}, which returns the mapped (query-side) field type.
     */
    public FieldType getLuceneFieldType() {
        return this.fieldType;
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

        // Reject the document rather than write a native field the cluster cannot read back. The
        // engine check comes first so a non-native field never reads cluster settings per document.
        if (isNativeEngine(sparseMethodContext) && SparseSettings.state().isNativeEngineEnabled() == false) {
            throw new IllegalArgumentException(
                "[" + CONTENT_TYPE + "] field [" + name() + "] cannot be indexed: " + SparseSettings.NATIVE_ENGINE_DISABLED_REASON
            );
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
                    if (!SparseEngine.NATIVE.getName().equalsIgnoreCase(sparseMethodContext.getSparseEngine())) {
                        FeatureField featureField = new FeatureField(name(), feature, value);
                        context.doc().addWithKey(key, featureField);
                    }

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
            Float quantizationCeilIngest = sparseMethodContext.getMethodComponentContext()
                .getFloatParameter(QUANTIZATION_CEILING_INGEST_FIELD, DEFAULT_QUANTIZATION_CEILING_INGEST);
            Float quantizationCeilSearch = sparseMethodContext.getMethodComponentContext()
                .getFloatParameter(QUANTIZATION_CEILING_SEARCH_FIELD, DEFAULT_QUANTIZATION_CEILING_SEARCH);
            fieldType.putAttribute(N_POSTINGS_FIELD, String.valueOf(nPostings));
            fieldType.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, String.valueOf(summaryPruneRatio));
            fieldType.putAttribute(CLUSTER_RATIO_FIELD, String.valueOf(clusterRatio));
            fieldType.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(algoTriggerThreshold));
            fieldType.putAttribute(QUANTIZATION_CEILING_INGEST_FIELD, String.valueOf(quantizationCeilIngest));
            fieldType.putAttribute(QUANTIZATION_CEILING_SEARCH_FIELD, String.valueOf(quantizationCeilSearch));
            fieldType.putAttribute(ENGINE_FIELD, sparseMethodContext.getSparseEngine());
            fieldType.putAttribute(FORWARD_INDEX_FIELD, forwardIndexOf(sparseMethodContext));
            fieldType.putAttribute(CLUSTERING_BATCH_SIZE_FIELD, clusteringBatchSizeOf(sparseMethodContext));
        }
    }

    /**
     * Default field type configurations.
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.putAttribute(SparseVectorField.SPARSE_FIELD, "true"); // This attribute helps to determine knn field type
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Parser for sparse tokens field type.
     */
    public static class SparseTypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            // Before the builder consumes the node: it removes the entries it recognizes as it parses.
            rejectNativeEngineParametersOnAnOlderIndex(node, parserContext.indexVersionCreated());
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
            if (isNativeEngine(context) && SparseSettings.state().isNativeEngineEnabled() == false) {
                throw new MapperParsingException("[" + ENGINE_FIELD + "]: " + SparseSettings.NATIVE_ENGINE_DISABLED_REASON);
            }
            if (isNativeEngine(context) == false) {
                // Only the native engine lays out a forward index or clusters in batches; accepting a
                // non-default value on any other engine would silently do nothing.
                rejectOnNonNativeEngine(FORWARD_INDEX_FIELD, forwardIndexOf(context), SparseForwardIndex.DEFAULT.getName());
                rejectOnNonNativeEngine(
                    CLUSTERING_BATCH_SIZE_FIELD,
                    clusteringBatchSizeOf(context),
                    String.valueOf(DEFAULT_CLUSTERING_BATCH_SIZE)
                );
            }
            return builder;
        }

        /**
         * Rejects the mapping parameters that arrived with the native engine unless every node in the
         * cluster can read them. {@code index.version.created} is what records that: OpenSearch sets it
         * to the smallest node version in the cluster when the index is created, so an index created
         * while an older node was around is refused. The live cluster min version is not an option --
         * mapping parsing also runs inside a cluster state applier on the node receiving the mapping,
         * where reading the applied state is illegal and kills the node -- and the same reasoning is
         * why {@code KNNVectorFieldMapper} gates {@code mode} and {@code compression_level} on the
         * index created version rather than on the cluster.
         *
         * Presence is what is checked, not the value: a node that predates a parameter fails on the
         * key itself, whatever it is set to.
         */
        private static void rejectNativeEngineParametersOnAnOlderIndex(Map<String, Object> node, Version indexCreatedVersion) {
            if (isVersionOnOrAfterMinReqVersionForSparseNativeEngine(indexCreatedVersion)) {
                return;
            }
            // A malformed or absent method is left to the parsing below to report
            if (node.get(METHOD) instanceof Map == false) {
                return;
            }
            Map<?, ?> method = (Map<?, ?>) node.get(METHOD);
            if (method.containsKey(ENGINE_FIELD)) {
                rejectOnAnOlderIndex(ENGINE_FIELD);
            }
            if (method.get(PARAMETERS_FIELD) instanceof Map) {
                Map<?, ?> parameters = (Map<?, ?>) method.get(PARAMETERS_FIELD);
                for (String parameter : NATIVE_ENGINE_PARAMETERS) {
                    if (parameters.containsKey(parameter)) {
                        rejectOnAnOlderIndex(parameter);
                    }
                }
            }
        }

        private static void rejectOnAnOlderIndex(String parameter) {
            throw new MapperParsingException(
                "["
                    + parameter
                    + "] can only be used on indices created on or after version "
                    + MINIMAL_SUPPORTED_VERSION_SPARSE_NATIVE_ENGINE
            );
        }

        private static void rejectOnNonNativeEngine(String parameter, String value, String defaultValue) {
            if (defaultValue.equals(value) == false) {
                throw new MapperParsingException(
                    "[" + parameter + "]: " + value + " is only supported with the " + SparseEngine.NATIVE.getName() + " engine"
                );
            }
        }
    }

    private static boolean isNativeEngine(SparseMethodContext sparseMethodContext) {
        return SparseEngine.NATIVE.getName().equalsIgnoreCase(sparseMethodContext.getSparseEngine());
    }

    /**
     * The two native-only knobs live in {@code method.parameters} alongside the algorithm's, and are
     * read as strings: that is what a field attribute holds, and it lets a value the mapping wrote as
     * a string compare equal to the default it names. {@link SparseAlgoType#validateMethod} has
     * already rejected anything unparseable by the time either is used.
     */
    private static String forwardIndexOf(SparseMethodContext context) {
        return String.valueOf(context.getMethodComponentContext().getParameter(FORWARD_INDEX_FIELD, SparseForwardIndex.DEFAULT.getName()));
    }

    private static String clusteringBatchSizeOf(SparseMethodContext context) {
        return String.valueOf(context.getMethodComponentContext().getParameter(CLUSTERING_BATCH_SIZE_FIELD, DEFAULT_CLUSTERING_BATCH_SIZE));
    }
}
