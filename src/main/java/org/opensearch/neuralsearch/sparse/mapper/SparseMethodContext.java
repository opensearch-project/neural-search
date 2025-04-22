/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Map;

/**
 * SparseMethodContext is responsible for handling the parameters of sparse method.
 * It will be used to initialize SparseEncodingQueryBuilder
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public class SparseMethodContext implements ToXContentFragment, Writeable {
    private static final String NAME_FIELD = "name";
    private static final String LAMBDA_FIELD = "lambda";
    private static final String ALPHA_FIELD = "alpha";

    private final String name;
    private final int lambda;
    private final float alpha;

    public SparseMethodContext(StreamInput in) throws IOException {
        this.name = in.readString();
        this.lambda = in.readOptionalInt();
        this.alpha = in.readOptionalFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.name);
        out.writeOptionalInt(this.lambda);
        out.writeOptionalFloat(this.alpha);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME_FIELD, this.name);
        builder.field(LAMBDA_FIELD, this.lambda);
        builder.field(ALPHA_FIELD, this.alpha);
        return builder;
    }

    public static SparseMethodContext parse(Object in) {
        if (!(in instanceof Map<?, ?>)) {
            throw new MapperParsingException("Unable to parse mapping into SparseMethodContext. Object not of type \"Map\"");
        }
        Map<String, Object> methodMap = (Map<String, Object>) in;
        String name = "";
        int lambda = 0;
        float alpha = 0;
        String key;
        Object value;
        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();
            if (NAME_FIELD.equals(key)) {
                name = (String) value;
            } else if (LAMBDA_FIELD.equals(key)) {
                lambda = (int) value;
            } else if (ALPHA_FIELD.equals(key)) {
                alpha = ((Double) value).floatValue();
            } else {
                throw new MapperParsingException("Invalid parameter: " + key);
            }
        }
        if (name.isEmpty()) {
            throw new MapperParsingException(NAME_FIELD + " needs to be set");
        }
        return new SparseMethodContext(name, lambda, alpha);
    }
}
