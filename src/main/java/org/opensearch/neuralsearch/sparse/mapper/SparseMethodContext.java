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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

/**
 * Context for sparse method configuration and parameters.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
@EqualsAndHashCode
public class SparseMethodContext implements ToXContentFragment, Writeable {
    private final String name;
    private final MethodComponentContext methodComponentContext;

    /**
     * Constructs from stream input.
     */
    public SparseMethodContext(StreamInput in) throws IOException {
        this.name = in.readString();
        this.methodComponentContext = new MethodComponentContext(in, name);
    }

    /**
     * Writes to stream output.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        this.methodComponentContext.writeTo(out);
    }

    /**
     * Converts to XContent format.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
        Map<String, Object> parameters = new HashMap<>();
        String key;
        Object value;
        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();
            if (NAME_FIELD.equals(key)) {
                name = (String) value;
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
        MethodComponentContext methodComponentContext = new MethodComponentContext(name, parameters);
        return new SparseMethodContext(name, methodComponentContext);
    }
}
