/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.summary;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang.StringUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * This class holds the summary of the results which are returned by OpenSearch and will be sent back to the customer.
 * The summary will be obtained from LLM summary models.
 */
@Getter
@AllArgsConstructor
public class GeneratedText implements ToXContentFragment, Writeable {
    private final String value;
    private final String error;
    @Setter
    private String processorTag;

    @Setter
    private String usecase;

    public GeneratedText(StreamInput in) throws IOException {
        processorTag = in.readString();
        usecase = in.readString();
        value = in.readOptionalString();
        error = in.readOptionalString();
    }

    public GeneratedText(final String value, final String error) {
        this.value = value;
        this.error = error;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (StringUtils.isNotEmpty(value)) {
            builder.field("value", value);
        } else if (StringUtils.isNotEmpty(error)) {
            builder.field("error", error);
        }
        builder.field("processorTag", processorTag);
        builder.field("usecase", usecase);
        builder.endObject();
        return builder;
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorTag);
        out.writeString(usecase);
        out.writeOptionalString(value);
        out.writeOptionalString(error);
    }
}
