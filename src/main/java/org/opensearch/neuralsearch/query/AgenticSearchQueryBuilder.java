/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import java.util.Locale;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * AgenticSearchQueryBuilder is responsible for agentic query type. It executes a natural language query with the query fields provided
 */
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Log4j2
public final class AgenticSearchQueryBuilder extends AbstractQueryBuilder<AgenticSearchQueryBuilder> implements WithFieldName {

    public static final String NAME = "agentic";
    public static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    public static final ParseField QUERY_FIELDS = new ParseField("query_fields");

    // Regex patterns for sanitizing query text
    private static final String SYSTEM_INSTRUCTION_PATTERN = "(?i)\\b(system|instruction|prompt)\\s*:";
    private static final String COMMAND_INJECTION_PATTERN = "(?i)\\b(execute|run|eval|script)\\s*[:\\(]";

    private static final int MAX_QUERY_LENGTH = 1000;
    public String queryText;
    public List<String> queryFields;

    // setting accessor to retrieve agentic search feature flag
    private static NeuralSearchSettingsAccessor SETTINGS_ACCESSOR;

    public static void initialize(NeuralSearchSettingsAccessor settingsAccessor) {
        AgenticSearchQueryBuilder.SETTINGS_ACCESSOR = settingsAccessor;
    }

    public AgenticSearchQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queryText = in.readString();
        this.queryFields = in.readOptionalStringList();
    }

    public String getQueryText() {
        return queryText;
    }

    public List<String> getQueryFields() {
        return queryFields;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // feature flag check
        if (!SETTINGS_ACCESSOR.isAgenticSearchEnabled()) {
            throw new IllegalStateException(
                "Agentic search is currently disabled. Enable it using the 'plugins.neural_search.agentic_search_enabled' setting."
            );
        }
        out.writeString(this.queryText);
        out.writeOptionalStringCollection(this.queryFields);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        // feature flag check
        if (!SETTINGS_ACCESSOR.isAgenticSearchEnabled()) {
            throw new IllegalStateException(
                "Agentic search is currently disabled. Enable it using the 'plugins.neural_search.agentic_search_enabled' setting."
            );
        }
        xContentBuilder.startObject(NAME);
        if (Objects.nonNull(QUERY_TEXT_FIELD)) {
            xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        }
        if (Objects.nonNull(queryFields) && !queryFields.isEmpty()) {
            xContentBuilder.field(QUERY_FIELDS.getPreferredName(), queryFields);
        }
        xContentBuilder.endObject();
    }

    /**
     * Creates AgenticSearchQueryBuilder from XContent.
     * The expected parsing form looks like:
     * {
     *  "agentic": {
     *    "query_text": "string",
     *    "query_fields": ["string", "string"..]
     *    }
     * }
     *
     */

    public static AgenticSearchQueryBuilder fromXContent(XContentParser parser) throws IOException {
        EventStatsManager.increment(EventStatName.AGENTIC_QUERY_REQUESTS);
        AgenticSearchQueryBuilder agenticSearchQueryBuilder = new AgenticSearchQueryBuilder();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    agenticSearchQueryBuilder.queryText = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERY_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<String> fieldsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (fieldsList.size() >= 25) {
                            throw new ParsingException(parser.getTokenLocation(), "Too many query fields. Maximum allowed is 25");
                        }
                        fieldsList.add(parser.text());
                    }
                    agenticSearchQueryBuilder.queryFields = fieldsList;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown field [" + currentFieldName + "]");
                }
            }
        }

        // Validate mandatory fields
        if (agenticSearchQueryBuilder.queryText == null || agenticSearchQueryBuilder.queryText.trim().isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "[" + QUERY_TEXT_FIELD.getPreferredName() + "] is required");
        }

        // Sanitize query text to prevent prompt injection
        agenticSearchQueryBuilder.queryText = sanitizeQueryText(agenticSearchQueryBuilder.queryText);

        return agenticSearchQueryBuilder;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // No rewriting needed
        return this;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new IllegalStateException(
            "Agentic search query must be used as top-level query, not nested inside other queries. Should be used with agentic_query_translator search processor"
        );
    }

    @Override
    protected boolean doEquals(AgenticSearchQueryBuilder obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queryText, obj.queryText);
        equalsBuilder.append(queryFields, obj.queryFields);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return new HashCodeBuilder().append(queryText).append(queryFields).toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String fieldName() {
        return NAME;
    }

    /**
     * Sanitizes query text to prevent prompt injection attacks
     */
    private static String sanitizeQueryText(String queryText) {
        if (queryText == null) {
            return null;
        }

        // Remove potential prompt injection patterns
        String sanitized = queryText
            // Remove system/instruction keywords that could manipulate LLM behavior
            .replaceAll(SYSTEM_INSTRUCTION_PATTERN, "")
            // Remove potential command injection patterns
            .replaceAll(COMMAND_INJECTION_PATTERN, "")
            .trim();

        // Validate length to prevent extremely long inputs
        if (sanitized.length() > MAX_QUERY_LENGTH) {
            String errorMessage = String.format(
                Locale.ROOT,
                "Query text too long. Maximum allowed length is [%s] characters",
                MAX_QUERY_LENGTH
            );
            throw new IllegalArgumentException(errorMessage);
        }

        return sanitized;
    }
}
