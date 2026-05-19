/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.search.SearchExtBuilder;

/**
 * Search request ext builder that opts a request into batch semantic highlighting.
 *
 * <p>When this ext is set to {@code true}, the fetch-phase highlighter yields and the
 * system-generated response processor performs highlighting for every {@code type: semantic}
 * field — including those declared inside {@code inner_hits}. The value is a single boolean.
 *
 * <pre>
 * POST /index/_search
 * {
 *   "query": { ... },
 *   "highlight": { "fields": { "body": { "type": "semantic" } }, "options": { "model_id": "m1" } },
 *   "ext": { "semantic_highlighting_batch": true }
 * }
 * </pre>
 */
public class SemanticHighlighterExtBuilder extends SearchExtBuilder {

    public static final String NAME = SemanticHighlightingConstants.EXT_NAME;

    private final boolean enabled;

    public SemanticHighlighterExtBuilder(boolean enabled) {
        this.enabled = enabled;
    }

    public SemanticHighlighterExtBuilder(StreamInput in) throws IOException {
        this.enabled = in.readBoolean();
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(enabled);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SemanticHighlighterExtBuilder && enabled == ((SemanticHighlighterExtBuilder) obj).enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), enabled);
    }

    /**
     * Parses the ext value as a boolean. Accepts native booleans and the strings
     * {@code "true"}/{@code "false"} for tolerance.
     *
     * @param parser the XContent parser positioned at the value
     * @return a new {@link SemanticHighlighterExtBuilder} carrying the parsed boolean
     * @throws IOException when the value is not a boolean
     */
    public static SemanticHighlighterExtBuilder parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return new SemanticHighlighterExtBuilder(parser.booleanValue());
        }
        if (token == XContentParser.Token.VALUE_STRING) {
            return new SemanticHighlighterExtBuilder(Boolean.parseBoolean(parser.text()));
        }
        throw new IOException(String.format(Locale.ROOT, "ext.%s must be a boolean, got token [%s]", NAME, token));
    }
}
