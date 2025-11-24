/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.AgenticSearchQueryBuilder;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder.AGENT_STEPS_FIELD_NAME;
import static org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder.MEMORY_ID_FIELD_NAME;
import static org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder.DSL_QUERY_FIELD_NAME;

@Log4j2
public class AgenticQueryTranslatorProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "agentic_query_translator";
    private static final int MAX_AGENT_RESPONSE_SIZE = 10_000;
    private static final int MAX_AGENT_ID_LENGTH = 100;
    private static final String AGENT_ID_PATTERN = "^[a-zA-Z0-9_-]+$";
    private final MLCommonsClientAccessor mlClient;
    private final String agentId;
    private final NamedXContentRegistry xContentRegistry;

    AgenticQueryTranslatorProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        MLCommonsClientAccessor mlClient,
        String agentId,
        NamedXContentRegistry xContentRegistry
    ) {
        super(tag, description, ignoreFailure);
        this.mlClient = mlClient;
        this.agentId = agentId;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    public void processRequestAsync(
        SearchRequest request,
        PipelineProcessingContext requestContext,
        ActionListener<SearchRequest> requestListener
    ) {
        EventStatsManager.increment(EventStatName.AGENTIC_QUERY_TRANSLATOR_PROCESSOR_EXECUTIONS);
        SearchSourceBuilder sourceBuilder = request.source();
        if (sourceBuilder == null || sourceBuilder.query() == null) {
            requestListener.onResponse(request);
            return;
        }

        QueryBuilder query = sourceBuilder.query();
        if (!(query instanceof AgenticSearchQueryBuilder)) {
            requestListener.onResponse(request);
            return;
        }

        AgenticSearchQueryBuilder agenticQuery = (AgenticSearchQueryBuilder) query;

        // Validate that agentic query is used alone without other search features
        if (hasOtherSearchFeatures(sourceBuilder)) {
            String errorMessage = String.format(
                Locale.ROOT,
                "Invalid usage with other search features like aggregation, sort, filters, collapse - Agent ID: [%s]",
                agentId
            );
            agenticQuery.setAgentFailureReason(errorMessage);
            requestListener.onFailure(new IllegalArgumentException("Agentic search blocked - " + errorMessage));
            return;
        }

        executeAgentAsync(agenticQuery, request, requestContext, requestListener);
    }

    private boolean hasOtherSearchFeatures(SearchSourceBuilder sourceBuilder) {
        return sourceBuilder.aggregations() != null
            || sourceBuilder.sorts() != null && !sourceBuilder.sorts().isEmpty()
            || sourceBuilder.highlighter() != null
            || sourceBuilder.postFilter() != null
            || sourceBuilder.suggest() != null
            || sourceBuilder.rescores() != null && !sourceBuilder.rescores().isEmpty()
            || sourceBuilder.collapse() != null;
    }

    private void executeAgentAsync(
        AgenticSearchQueryBuilder agenticQuery,
        SearchRequest request,
        PipelineProcessingContext requestContext,
        ActionListener<SearchRequest> requestListener
    ) {
        // First get agent type and prompts info
        mlClient.getAgentDetails(agentId, ActionListener.wrap(agentInfo -> {
            mlClient.executeAgent(request, agenticQuery, agentId, agentInfo, xContentRegistry, ActionListener.wrap(agentResponse -> {
                try {
                    String dslQuery = agentResponse.getDslQuery();
                    String agentStepsSummary = agentResponse.getAgentStepsSummary();
                    String memoryId = agentResponse.getMemoryId();
                    // Validate response size to prevent memory exhaustion
                    if (dslQuery == null) {
                        String errorMessage = String.format(Locale.ROOT, "Null response from agent - Agent ID: [%s]", agentId);
                        agenticQuery.setAgentFailureReason(errorMessage);
                        throw new IllegalArgumentException("Agentic search failed - " + errorMessage);
                    }

                    if (dslQuery.length() > MAX_AGENT_RESPONSE_SIZE) {
                        String errorMessage = String.format(
                            Locale.ROOT,
                            "Response size exceeded limit - Agent ID: [%s], Size: [%d]. Maximum allowed size is %d characters.",
                            agentId,
                            dslQuery.length(),
                            MAX_AGENT_RESPONSE_SIZE
                        );
                        agenticQuery.setAgentFailureReason(errorMessage);
                        throw new IllegalArgumentException("Agentic search blocked - " + errorMessage);
                    }

                    // Store agent steps summary in request context for response processing
                    if (agentStepsSummary != null && !agentStepsSummary.trim().isEmpty()) {
                        requestContext.setAttribute(AGENT_STEPS_FIELD_NAME, agentStepsSummary);
                    }

                    if (memoryId != null && !memoryId.trim().isEmpty()) {
                        requestContext.setAttribute(MEMORY_ID_FIELD_NAME, memoryId);
                    }

                    requestContext.setAttribute(DSL_QUERY_FIELD_NAME, dslQuery);

                    // Parse the agent response to get the new search source
                    BytesReference bytes = new BytesArray(dslQuery);
                    SearchSourceBuilder originalSourceBuilder = request.source();
                    List<SearchExtBuilder> originalExtBuilders = originalSourceBuilder != null ? originalSourceBuilder.ext() : null;
                    try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, null, bytes.streamInput())) {
                        SearchSourceBuilder newSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                        if (originalExtBuilders != null && !originalExtBuilders.isEmpty()) {
                            newSourceBuilder.ext(originalExtBuilders);
                        }
                        // Preserve original request parameters
                        preserveOriginalParameters(originalSourceBuilder, newSourceBuilder);
                        request.source(newSourceBuilder);
                    }

                    requestListener.onResponse(request);
                } catch (IOException e) {
                    String errorMessage = String.format(Locale.ROOT, "Parse error - Agent ID: [%s], Error: [%s]", agentId, e.getMessage());
                    agenticQuery.setAgentFailureReason(errorMessage);
                    requestListener.onFailure(new IOException("Agentic search failed - " + errorMessage, e));
                }
            }, e -> {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "Agent execution error - Agent ID: [%s], Error: [%s]",
                    agentId,
                    e.getMessage()
                );
                agenticQuery.setAgentFailureReason(errorMessage);
                requestListener.onFailure(new IllegalArgumentException("Agentic search failed - " + errorMessage, e));
            }));
        }, e -> {
            String errorMessage = String.format(
                Locale.ROOT,
                "Failed to get agent info - Agent ID: [%s], Error: [%s]",
                agentId,
                e.getMessage()
            );
            agenticQuery.setAgentFailureReason(errorMessage);
            requestListener.onFailure(new IllegalArgumentException("Agentic search failed - " + errorMessage, e));
        }));
    }

    private void preserveOriginalParameters(SearchSourceBuilder original, SearchSourceBuilder target) {
        if (original == null) return;

        // Core pagination and limits
        if (original.size() != -1) target.size(original.size());
        if (original.from() != -1) target.from(original.from());
        if (original.timeout() != null) target.timeout(original.timeout());
        if (original.terminateAfter() != -1) target.terminateAfter(original.terminateAfter());

        // Source and field selection
        if (original.fetchSource() != null) target.fetchSource(original.fetchSource());
        if (original.storedFields() != null) target.storedFields(original.storedFields());
        if (original.docValueFields() != null && !original.docValueFields().isEmpty()) {
            original.docValueFields().forEach(field -> target.docValueField(field.field, field.format));
        }
        if (original.fetchFields() != null && !original.fetchFields().isEmpty()) {
            original.fetchFields().forEach(field -> target.fetchField(field.field, field.format));
        }
        if (original.scriptFields() != null && !original.scriptFields().isEmpty()) {
            original.scriptFields().forEach(sf -> target.scriptField(sf.fieldName(), sf.script(), sf.ignoreFailure()));
        }

        // Scoring and metadata
        target.trackScores(original.trackScores());
        if (original.trackTotalHitsUpTo() != null) target.trackTotalHitsUpTo(original.trackTotalHitsUpTo());
        if (original.minScore() != null) target.minScore(original.minScore());
        if (original.explain() != null) target.explain(original.explain());
        if (original.version() != null) target.version(original.version());
        if (original.seqNoAndPrimaryTerm() != null) target.seqNoAndPrimaryTerm(original.seqNoAndPrimaryTerm());
        target.includeNamedQueriesScores(original.includeNamedQueriesScore());

        // Search behavior (excluding blocked features)
        if (original.searchAfter() != null) target.searchAfter(original.searchAfter());
        if (original.slice() != null) target.slice(original.slice());
        if (original.indexBoosts() != null && !original.indexBoosts().isEmpty()) {
            original.indexBoosts().forEach(ib -> target.indexBoost(ib.getIndex(), ib.getBoost()));
        }
        if (original.stats() != null) target.stats(original.stats());
        target.profile(original.profile());

        // Advanced features
        if (original.pointInTimeBuilder() != null) target.pointInTimeBuilder(original.pointInTimeBuilder());
        if (original.pipeline() != null) target.pipeline(original.pipeline());
        if (original.verbosePipeline() != null) target.verbosePipeline(original.verbosePipeline());
        if (original.getDerivedFields() != null && !original.getDerivedFields().isEmpty()) {
            original.getDerivedFields()
                .forEach(
                    df -> target.derivedField(
                        df.getName(),
                        df.getType(),
                        df.getScript(),
                        df.getProperties(),
                        df.getPrefilterField(),
                        df.getFormat(),
                        df.getIgnoreMalformed()
                    )
                );
        }
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        throw new UnsupportedOperationException("Use processRequestAsync for agentic search processor");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private final MLCommonsClientAccessor mlClient;
        private final NamedXContentRegistry xContentRegistry;
        private final NeuralSearchSettingsAccessor settingsAccessor;

        public Factory(
            MLCommonsClientAccessor mlClient,
            NamedXContentRegistry xContentRegistry,
            NeuralSearchSettingsAccessor settingsAccessor
        ) {
            this.mlClient = mlClient;
            this.xContentRegistry = xContentRegistry;
            this.settingsAccessor = settingsAccessor;
        }

        @Override
        public AgenticQueryTranslatorProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException, IllegalStateException {
            String agentId = readStringProperty(TYPE, tag, config, "agent_id");
            if (agentId == null || agentId.trim().isEmpty()) {
                throw new IllegalArgumentException("agent_id is required for agentic_query_translator processor");
            }

            // Validate agent ID length
            if (agentId.length() > MAX_AGENT_ID_LENGTH) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "agent_id exceeds maximum length of %d characters", MAX_AGENT_ID_LENGTH)
                );
            }

            // Validate agent ID format
            if (!agentId.matches(AGENT_ID_PATTERN)) {
                throw new IllegalArgumentException("agent_id must contain only alphanumeric characters, hyphens, and underscores");
            }
            return new AgenticQueryTranslatorProcessor(tag, description, ignoreFailure, mlClient, agentId, xContentRegistry);
        }
    }
}
