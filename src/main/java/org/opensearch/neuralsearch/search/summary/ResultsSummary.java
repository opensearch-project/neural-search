/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.summary;

import java.io.IOException;

import lombok.Getter;

import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * This class holds the summary of the results which are returned by OpenSearch and will be sent back to the customer.
 * The summary will be obtained from LLM summary models.
 */
@Getter
public class ResultsSummary implements ToXContentFragment {
    private final String summary;
    private final String error;

    public ResultsSummary() {
        summary = "This is my summary";
        error = null;
    }

    public ResultsSummary(final String summary, final String error) {
        this.summary = summary;
        this.error = error;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("summary", summary);
        return builder;
    }
}
