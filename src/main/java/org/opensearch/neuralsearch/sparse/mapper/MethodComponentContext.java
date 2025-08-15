/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

/**
 * Context for method components in sparse neural search configurations.
 *
 * <p>This class encapsulates the configuration of a method component, including
 * its name and associated parameters. It provides serialization support for
 * streaming and XContent operations, enabling persistence and transmission
 * of method configurations.
 *
 * <p>Method components can contain nested configurations, allowing for
 * hierarchical parameter structures. The class handles deep copying and
 * proper serialization of nested components.
 *
 * @see SparseMethodContext
 * @see ToXContentFragment
 * @see Writeable
 */
@RequiredArgsConstructor
@Getter
public class MethodComponentContext implements ToXContentFragment, Writeable {
    private final String name;
    private final Map<String, Object> parameters;

    /**
     * Copy constructor for deep cloning of MethodComponentContext.
     *
     * <p>Creates a deep copy of the provided context, including nested
     * MethodComponentContext instances within parameters.
     *
     * @param methodComponentContext the context to copy
     * @throws IllegalArgumentException if methodComponentContext is null
     */
    public MethodComponentContext(MethodComponentContext methodComponentContext) {
        if (methodComponentContext == null) {
            throw new IllegalArgumentException("MethodComponentContext cannot be null");
        }

        this.name = methodComponentContext.name;
        this.parameters = new HashMap<>();
        if (methodComponentContext.parameters != null) {
            for (Map.Entry<String, Object> entry : methodComponentContext.parameters.entrySet()) {
                if (entry.getValue() instanceof MethodComponentContext) {
                    parameters.put(entry.getKey(), new MethodComponentContext((MethodComponentContext) entry.getValue()));
                } else {
                    parameters.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Constructor from stream.
     *
     * @param in StreamInput
     * @throws IOException on stream failure
     */
    public MethodComponentContext(StreamInput in) throws IOException {
        this.name = in.readString();

        if (in.available() > 0) {
            this.parameters = in.readMap(StreamInput::readString, new ParameterMapValueReader());
        } else {
            this.parameters = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        if (this.parameters != null) {
            out.writeMap(this.parameters, StreamOutput::writeString, new ParameterMapValueWriter());
        }
    }

    /**
     * Parses a map object into a MethodComponentContext instance.
     *
     * <p>Expects a map containing 'name' and optionally 'parameters' fields.
     * Parameters can contain nested maps which are recursively parsed into
     * MethodComponentContext instances.
     *
     * @param in the map object to parse
     * @return parsed MethodComponentContext instance
     * @throws MapperParsingException if parsing fails or required fields are missing
     */
    public static MethodComponentContext parse(Object in) {
        if (!(in instanceof Map<?, ?>)) {
            throw new MapperParsingException("Unable to parse MethodComponent");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> methodMap = (Map<String, Object>) in;
        String name = "";
        Map<String, Object> parameters = new HashMap<>();

        String key;
        Object value;

        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();

            if (NAME_FIELD.equals(key)) {
                if (!(value instanceof String)) {
                    throw new MapperParsingException("Component name should be a string");
                }
                name = (String) value;
            } else if (PARAMETERS_FIELD.equals(key)) {
                if (value == null) {
                    parameters = null;
                    continue;
                }

                if (!(value instanceof Map)) {
                    throw new MapperParsingException("Unable to parse parameters for method component");
                }

                // Check to interpret map parameters as sub-MethodComponentContexts
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
                throw new MapperParsingException("Invalid parameter for MethodComponentContext: " + key);
            }
        }

        if (name.isEmpty()) {
            throw new MapperParsingException(NAME_FIELD + " needs to be set");
        }

        return new MethodComponentContext(name, parameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME_FIELD, name);
        if (parameters == null) {
            builder.field(PARAMETERS_FIELD, (String) null);
        } else {
            builder.startObject(PARAMETERS_FIELD);
            parameters.forEach((key, value) -> {
                try {
                    if (value instanceof MethodComponentContext) {
                        builder.startObject(key);
                        ((MethodComponentContext) value).toXContent(builder, params);
                        builder.endObject();
                    } else {
                        builder.field(key, value);
                    }
                } catch (IOException ioe) {
                    throw new RuntimeException("Unable to generate xcontent for method component");
                }

            });
            builder.endObject();
        }

        return builder;
    }

    public static MethodComponentContext fromXContent(XContentParser xContentParser) throws IOException {
        // If it is a fresh parser, move to the first token
        if (xContentParser.currentToken() == null) {
            xContentParser.nextToken();
        }
        Map<String, Object> parsedMap = xContentParser.map();
        return MethodComponentContext.parse(parsedMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MethodComponentContext other = (MethodComponentContext) obj;

        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(name, other.name);
        equalsBuilder.append(parameters, other.parameters);
        return equalsBuilder.isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(name).append(parameters).toHashCode();
    }

    /**
     * Gets the parameters of the component.
     *
     * <p>Returns an empty map if parameters is null for backwards compatibility.
     * This prevents null pointer exceptions when accessing parameters.
     *
     * @return the parameters map, never null
     */
    public Map<String, Object> getParameters() {
        // Due to backwards compatibility issue, parameters could be null. To prevent any null pointer exceptions,
        // return an empty map if parameters is null. For more information, refer to
        // https://github.com/opensearch-project/k-NN/issues/353.
        if (parameters == null) {
            return Collections.emptyMap();
        }
        return parameters;
    }

    /**
     * Gets a parameter value with a default fallback.
     *
     * @param key the parameter key
     * @param defaultValue the default value if key is not found
     * @return the parameter value or default value
     */
    public Object getParameter(String key, Object defaultValue) {
        return getParameters().getOrDefault(key, defaultValue);
    }

    /**
     * Gets a float parameter value with type conversion.
     *
     * <p>Converts numeric values to float. Returns default value if
     * the parameter is not found or not a number.
     *
     * @param key the parameter key
     * @param defaultValue the default float value
     * @return the float parameter value or default value
     */
    public Float getFloat(String key, Float defaultValue) {
        Object value = getParameter(key, defaultValue);
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return defaultValue;
    }

    private static class ParameterMapValueReader implements Reader<Object> {

        private ParameterMapValueReader() {}

        @Override
        public Object read(StreamInput in) throws IOException {
            boolean isValueMethodComponentContext = in.readBoolean();
            if (isValueMethodComponentContext) {
                return new MethodComponentContext(in);
            }
            return in.readGenericValue();
        }
    }

    private static class ParameterMapValueWriter implements Writer<Object> {

        private ParameterMapValueWriter() {}

        @Override
        public void write(StreamOutput out, Object o) throws IOException {
            if (o instanceof MethodComponentContext) {
                out.writeBoolean(true);
                ((MethodComponentContext) o).writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeGenericValue(o);
            }
        }
    }
}
