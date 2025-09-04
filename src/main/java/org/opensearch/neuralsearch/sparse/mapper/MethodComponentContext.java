/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

/**
 * Configuration context for sparse ANN search method components.
 */
@RequiredArgsConstructor
@Getter
public class MethodComponentContext implements ToXContentFragment, Writeable {
    private final String name;
    private final Map<String, Object> parameters;

    /**
     * Deep copy constructor.
     *
     * @param methodComponentContext context to copy
     * @throws IllegalArgumentException if context is null
     */
    public MethodComponentContext(MethodComponentContext methodComponentContext) {
        if (methodComponentContext == null) {
            throw new IllegalArgumentException("MethodComponentContext cannot be null");
        }

        this.name = methodComponentContext.name;
        this.parameters = new HashMap<>();
        if (methodComponentContext.parameters != null) {
            parameters.putAll(methodComponentContext.parameters);
        }
    }

    /**
     * Creates context from stream input.
     *
     * @param in StreamInput
     * @param in name
     * @throws IOException on stream failure
     */
    public MethodComponentContext(StreamInput in, String name) throws IOException {
        this.name = name;
        if (in.available() > 0) {
            this.parameters = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        } else {
            this.parameters = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this.parameters != null) {
            out.writeMap(this.parameters, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
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
                    builder.field(key, value);
                } catch (IOException ioe) {
                    throw new RuntimeException("Unable to generate xcontent for method component");
                }

            });
            builder.endObject();
        }

        return builder;
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
     * Gets component parameters.
     *
     * @return parameters map, never null
     */
    public Map<String, Object> getParameters() {
        if (parameters == null) {
            return Collections.emptyMap();
        }
        return parameters;
    }

    /**
     * Gets parameter with default fallback.
     *
     * @param key parameter key
     * @param defaultValue default if not found
     * @return parameter value or default
     */
    public Object getParameter(String key, Object defaultValue) {
        return getParameters().getOrDefault(key, defaultValue);
    }

    /**
     * Gets float parameter with type conversion.
     *
     * @param key parameter key
     * @param defaultValue default float value
     * @return float parameter or default
     */
    public Float getFloatParameter(String key, Float defaultValue) {
        Object value = getParameter(key, defaultValue);
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return defaultValue;
    }
}
