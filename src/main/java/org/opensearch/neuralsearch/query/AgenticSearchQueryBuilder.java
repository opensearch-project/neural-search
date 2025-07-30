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
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
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
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import com.google.gson.Gson;

import java.util.Objects;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import java.io.IOException;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * AgenticSearchQueryBuilder is responsible for agentic query type. It executes a root agent to extract the DSL query and parse it to innerQueryBuilder for parsing.
 */
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
@Getter(AccessLevel.PACKAGE)
@Setter(AccessLevel.PACKAGE)
@Log4j2
public final class AgenticSearchQueryBuilder extends AbstractQueryBuilder<AgenticSearchQueryBuilder> implements WithFieldName {

    public static final String NAME = "agentic";
    public static final ParseField QUERY_TEXT_FIELD = new ParseField("query_text");
    public static final ParseField AGENT_ID = new ParseField("agent_id");
    public static final ParseField QUERY_FIELDS = new ParseField("query_fields");
    private static final Gson gson = new Gson();

    private String queryText;
    private String agentId;
    private List<String> queryFields;
    private QueryBuilder rewrittenQuery;

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
     *  "agentic": {
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
        // Idempotent check - if already rewritten, return the cached result
        if (rewrittenQuery != null) {
            return rewrittenQuery;
        }

        if (ML_CLIENT == null) {
            throw new IllegalStateException("ML client not initialized");
        }

        // the queryRewrite is expected at QueryCoordinator level
        if (!(queryRewriteContext instanceof QueryCoordinatorContext)) {
            throw new IllegalStateException(
                "Agentic query must be rewritten at the coordinator node. Rewriting at shard level is not supported."
            );
        }

        QueryCoordinatorContext coordinatorContext = (QueryCoordinatorContext) queryRewriteContext;

        // Get index mapping from cluster state
        String indexMapping = NeuralSearchClusterUtil.instance().getIndexMapping(coordinatorContext);
        String indexMappingJson = gson.toJson(indexMapping);

        // Execute agent at coordinator level to avoid multiple calls per shard
        CompletableFuture<String> future = new CompletableFuture<>();
        Map<String, String> parameters = new HashMap<>();
        parameters.put("query_text", queryText);
        parameters.put("index_mapping", indexMappingJson);
        if (queryFields != null && !queryFields.isEmpty()) {
            parameters.put("query_fields", String.join(",", queryFields));
        }

        ML_CLIENT.executeAgent(agentId, parameters, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            String agentResponse = future.get();

            // Extract just the inner query content from the agent response and remove the "query" wrapper
            String innerQueryContent = extractQueryContent(agentResponse);

            log.info("Generated Query: [{}]", innerQueryContent);

            // Parse the inner query
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(queryRewriteContext.getXContentRegistry(), LoggingDeprecationHandler.INSTANCE, innerQueryContent);

            rewrittenQuery = parseInnerQueryBuilder(parser);
            return rewrittenQuery;
        } catch (Exception e) {
            log.error("Failed to execute agentic search for the query text [{}] and index mapping [{}]", queryText, indexMapping);
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

    private String extractQueryContent(String agentResponse) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(null, LoggingDeprecationHandler.INSTANCE, agentResponse);

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME && "query".equals(parser.currentName())) {
                parser.nextToken(); // Move to the query value

                // Use XContentBuilder to extract the inner query as JSON string
                XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
                builder.copyCurrentStructure(parser);
                return builder.toString();
            } else {
                parser.skipChildren();
            }
        }

        throw new IOException("No 'query' field found in agent response");
    }
}
