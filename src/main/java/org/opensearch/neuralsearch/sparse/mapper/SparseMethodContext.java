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
 * Context for sparse method configurations in neural search.
 *
 * <p>This class encapsulates the configuration of a sparse method, including
 * its name and associated method component context. It serves as the primary
 * configuration holder for sparse encoding operations and query builders.
 *
 * <p>The context provides serialization support for streaming and XContent
 * operations, enabling persistence and transmission of sparse method
 * configurations across the OpenSearch cluster.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Storing sparse method name and parameters</li>
 *   <li>Providing serialization/deserialization capabilities</li>
 *   <li>Supporting XContent conversion for REST API operations</li>
 *   <li>Initializing SparseEncodingQueryBuilder instances</li>
 * </ul>
 *
 * @see MethodComponentContext
 * @see ToXContentFragment
 * @see Writeable
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
@EqualsAndHashCode
public class SparseMethodContext implements ToXContentFragment, Writeable {
    private final String name;
    private final MethodComponentContext methodComponentContext;

    /**
     * Constructs SparseMethodContext from stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if reading from stream fails
     */
    public SparseMethodContext(StreamInput in) throws IOException {
        this.name = in.readString();
        this.methodComponentContext = new MethodComponentContext(in);
    }

    /**
     * Writes the context to stream output for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if writing to stream fails
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        this.methodComponentContext.writeTo(out);
    }

    /**
     * Converts the context to XContent format.
     *
     * @param builder the XContent builder
     * @param params the parameters for XContent conversion
     * @return the updated XContent builder
     * @throws IOException if XContent conversion fails
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = methodComponentContext.toXContent(builder, params);
        return builder;
    }

    /**
     * Parses a map object into a SparseMethodContext instance.
     *
     * <p>Expects a map containing 'name' and optionally 'parameters' fields.
     * Creates a MethodComponentContext from the parsed data.
     *
     * @param in the map object to parse
     * @return parsed SparseMethodContext instance
     * @throws MapperParsingException if parsing fails or required fields are missing
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
                @SuppressWarnings("unchecked")
                Map<String, Object> parametersTemp = ((Map<String, Object>) value).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                        Object v = e.getValue();
                        if (v instanceof Map) {
                            return MethodComponentContext.parse(v);
                        }
                        return v;
                    }));

                parameters = parametersTemp;
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
