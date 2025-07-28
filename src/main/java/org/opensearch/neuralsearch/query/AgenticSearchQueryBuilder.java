/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
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
import org.opensearch.index.query.QueryCoordinatorContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * AgenticSearchQueryBuilder is responsible for agentic_search query type. It executes a root agent to extract the DSL query and parse it to innerQueryBuilder for parsing.
 */
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter(AccessLevel.PACKAGE)
@Setter(AccessLevel.PACKAGE)
public final class AgenticSearchQueryBuilder extends AbstractQueryBuilder<AgenticSearchQueryBuilder> implements WithFieldName {

    public static final String NAME = "agentic_search";
    static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    static final ParseField AGENT_ID = new ParseField("agent_id");
    static final ParseField QUERY_FIELDS = new ParseField("query_fields");

    private String queryText;
    private String agentId;
    private List<String> queryFields;

    // client to invoke ml-common APIs
    private static MLCommonsClientAccessor ML_CLIENT;

    public static void initialize(MLCommonsClientAccessor mlClient) {
        AgenticSearchQueryBuilder.ML_CLIENT = mlClient;
    }

    // For testing purposes
    static void resetMLClient() {
        AgenticSearchQueryBuilder.ML_CLIENT = null;
    }

    public AgenticSearchQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.queryText = in.readString();
        this.agentId = in.readString();
        this.queryFields = in.readOptionalStringList();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.queryText);
        out.writeString(this.agentId);
        out.writeOptionalStringCollection(this.queryFields);
    }

    @Override
    protected void doXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject(NAME);
        if (Objects.nonNull(QUERY_TEXT_FIELD)) {
            xContentBuilder.field(QUERY_TEXT_FIELD.getPreferredName(), queryText);
        }
        if (Objects.nonNull(AGENT_ID)) {
            xContentBuilder.field(AGENT_ID.getPreferredName(), agentId);
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
     *  "agentic_search": {
     *    "query_text": "string",
     *    "agent_id": "string",
     *    "query_fields": ["string", "string"..]
     *    }
     * }
     *
     */

    public static AgenticSearchQueryBuilder fromXContent(XContentParser parser) throws IOException {
        AgenticSearchQueryBuilder agenticSearchQueryBuilder = new AgenticSearchQueryBuilder();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QUERY_TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    agenticSearchQueryBuilder.queryText = parser.text();
                } else if (AGENT_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    agenticSearchQueryBuilder.agentId = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERY_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<String> fieldsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
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
        if (agenticSearchQueryBuilder.agentId == null || agenticSearchQueryBuilder.agentId.trim().isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "[" + AGENT_ID.getPreferredName() + "] is required");
        }

        return agenticSearchQueryBuilder;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (ML_CLIENT == null) {
            throw new IllegalStateException("ML client not initialized");
        }

        if (!(queryRewriteContext instanceof QueryCoordinatorContext)) {
            throw new IllegalStateException(
                "Agentic query must be rewritten at the coordinator node. Rewriting at shard level is not supported."
            );
        }

        CompletableFuture<String> future = new CompletableFuture<>();
        // TODO Integrate execute agent API
        /*ML_CLIENT.inferenceSentences(agentId, List.of(queryText), ActionListener.wrap(
            response -> {
                String dslQuery = response.getInferenceResults().get(0).getOutput().get(0).getResult();
                future.complete(dslQuery);
            },
            future::completeExceptionally
        ));*/

        try {
            // String dslQueryString = future.get();
            String dslQueryString = "{\n"
                + "                    \"match\": {\n"
                + "                        \"passage_text\": {\n"
                + "                            \"query\": \"science\"\n"
                + "                        }\n"
                + "                    }\n"
                + "                }";
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(queryRewriteContext.getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, dslQueryString);
            return parseInnerQueryBuilder(parser);
        } catch (Exception e) {
            throw new IOException("Failed to execute agentic search", e);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new IllegalStateException("Agentic search query must be rewritten first");
    }

    @Override
    protected boolean doEquals(AgenticSearchQueryBuilder obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqualsBuilder equalsBuilder = new EqualsBuilder();
        equalsBuilder.append(queryText, obj.queryText);
        equalsBuilder.append(agentId, obj.agentId);
        equalsBuilder.append(queryFields, obj.queryFields);
        return equalsBuilder.isEquals();
    }

    @Override
    protected int doHashCode() {
        return new HashCodeBuilder().append(queryText).append(agentId).append(queryFields).toHashCode();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String fieldName() {
        return NAME;
    }
}
