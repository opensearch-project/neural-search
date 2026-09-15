/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.common.MinClusterVersionUtil;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.MINIMAL_SUPPORTED_VERSION_SPARSE_NATIVE_ENGINE;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

/**
 * Context for sparse method configuration and parameters.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
@EqualsAndHashCode
public class SparseMethodContext implements ToXContentFragment, Writeable {
    private final String name;
    private final String sparseEngine;
    private final MethodComponentContext methodComponentContext;

    /**
     * Constructs from stream input.
     *
     * A peer older than {@link MinClusterVersionUtil#MINIMAL_SUPPORTED_VERSION_SPARSE_NATIVE_ENGINE}
     * wrote no engine, and such a node only ever ran the Lucene engine, so the default is what it
     * meant. Reading it unconditionally would instead consume the component context's bytes.
     */
    public SparseMethodContext(StreamInput in) throws IOException {
        this.name = in.readString();
        if (in.getVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_SPARSE_NATIVE_ENGINE)) {
            this.sparseEngine = in.readOptionalString();
        } else {
            this.sparseEngine = SparseEngine.DEFAULT.getName();
        }
        this.methodComponentContext = new MethodComponentContext(in, name);
    }

    /**
     * Writes to stream output.
     *
     * The engine is dropped for a peer that predates it; it could not act on it anyway, since the
     * native engine does not exist there.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        if (out.getVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_SPARSE_NATIVE_ENGINE)) {
            out.writeOptionalString(this.sparseEngine);
        }
        this.methodComponentContext.writeTo(out);
    }

    /**
     * Converts to XContent format.
     *
     * A default-valued engine is left out. This is what the mapping source is compared against:
     * an index created before the field existed does not have it in the source stored in cluster
     * state, and {@code MapperService#assertMappingVersion} fails the node with an
     * {@link AssertionError} when a re-serialization does not match that source byte for byte.
     * Emitting the default would therefore kill every upgraded node holding such an index.
     * The cost is that an explicit {@code "engine": "lucene"} is not echoed back, the same way
     * the rest of the mapping drops values it resolved to a default.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (sparseEngine != null && SparseEngine.fromName(sparseEngine) != SparseEngine.DEFAULT) {
            builder.field(ENGINE_FIELD, sparseEngine);
        }
        builder = methodComponentContext.toXContent(builder, params);
        return builder;
    }

    /**
     * Parses map to SparseMethodContext.
     */
    @SuppressWarnings("unchecked")
    public static SparseMethodContext parse(Object in) {
        if (!(in instanceof Map<?, ?>)) {
            throw new MapperParsingException("Unable to parse mapping into SparseMethodContext. Object not of type \"Map\"");
        }
        Map<String, Object> methodMap = (Map<String, Object>) in;
        String name = "";
        String engine = SparseEngine.DEFAULT.getName();
        Map<String, Object> parameters = new HashMap<>();
        String key;
        Object value;
        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();
            if (NAME_FIELD.equals(key)) {
                name = (String) value;
            } else if (ENGINE_FIELD.equals(key)) {
                engine = (String) value;
            } else if (PARAMETERS_FIELD.equals(key)) {
                if (value == null) {
                    parameters = null;
                    continue;
                }

                if (!(value instanceof Map)) {
                    throw new MapperParsingException("Unable to parse parameters for main method component");
                }

                // Interpret all map parameters as sub-MethodComponentContexts
                parameters = ((Map<String, Object>) value).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    Object v = e.getValue();
                    if (v instanceof Map) {
                        throw new IllegalArgumentException("Value should not be map");
                    }
                    return v;
                }));
            } else {
                throw new MapperParsingException("Invalid parameter: " + key);
            }
        }
        if (name.isEmpty()) {
            throw new MapperParsingException(NAME_FIELD + " needs to be set");
        }
        SparseEngine sparseEngine = SparseEngine.fromName(engine);
        if (sparseEngine == null) {
            throw new MapperParsingException(ENGINE_FIELD + " needs to be valid engine");
        }
        MethodComponentContext methodComponentContext = new MethodComponentContext(name, parameters);
        return new SparseMethodContext(name, engine, methodComponentContext);
    }
}
