/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.gson.Gson;
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
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

@Log4j2
public class AgenticQueryTranslatorProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "agentic_query_translator";
    private static final int MAX_AGENT_RESPONSE_SIZE = 10_000;
    private final MLCommonsClientAccessor mlClient;
    private final String agentId;
    private final NamedXContentRegistry xContentRegistry;
    private static final Gson gson = new Gson();

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
                "Agentic search blocked - Invalid usage with other search features like aggregation, sort, filters, collapse - Agent ID: [%s]",
                agentId
            );
            requestListener.onFailure(new IllegalArgumentException(errorMessage));
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
                        String errorMessage = String.format(
                            Locale.ROOT,
                            "Agentic search failed - Null response from agent - Agent ID: [%s]",
                            agentId
                        );
                        throw new IllegalArgumentException(errorMessage);
                    }

                    if (dslQuery.length() > MAX_AGENT_RESPONSE_SIZE) {
                        String errorMessage = String.format(
                            Locale.ROOT,
                            "Agentic search blocked - Response size exceeded limit - Agent ID: [%s], Size: [%d]. Maximum allowed size is %d characters.",
                            agentId,
                            dslQuery.length(),
                            MAX_AGENT_RESPONSE_SIZE
                        );
                        throw new IllegalArgumentException(errorMessage);
                    }

                    // Store agent steps summary in request context for response processing
                    if (agentStepsSummary != null && !agentStepsSummary.trim().isEmpty()) {
                        requestContext.setAttribute("agent_steps_summary", agentStepsSummary);
                    }

                    if (memoryId != null && !memoryId.trim().isEmpty()) {
                        requestContext.setAttribute("memory_id", memoryId);
                    }

                    // Parse the agent response to get the new search source
                    BytesReference bytes = new BytesArray(dslQuery);
                    try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, null, bytes.streamInput())) {
                        SearchSourceBuilder newSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                        request.source(newSourceBuilder);
                    }

                    requestListener.onResponse(request);
                } catch (IOException e) {
                    String errorMessage = String.format(
                        Locale.ROOT,
                        "Agentic search failed - Parse error - Agent ID: [%s], Error: [%s]",
                        agentId,
                        e.getMessage()
                    );
                    requestListener.onFailure(new IOException(errorMessage, e));
                }
            }, e -> {
                String errorMessage = String.format(
                    Locale.ROOT,
                    "Agentic search failed - Agent execution error - Agent ID: [%s], Error: [%s]",
                    agentId,
                    e.getMessage()
                );
                requestListener.onFailure(new RuntimeException(errorMessage, e));
            }));
        }, e -> {
            String errorMessage = String.format(
                Locale.ROOT,
                "Agentic search failed - Failed to get agent info - Agent ID: [%s], Error: [%s]",
                agentId,
                e.getMessage()
            );
            requestListener.onFailure(new RuntimeException(errorMessage, e));
        }));
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
            // feature flag check
            if (!settingsAccessor.isAgenticSearchEnabled()) {
                throw new IllegalStateException(
                    "Agentic search is currently disabled. Enable it using the 'plugins.neural_search.agentic_search_enabled' setting."
                );
            }
            String agentId = readStringProperty(TYPE, tag, config, "agent_id");
            if (agentId == null || agentId.trim().isEmpty()) {
                throw new IllegalArgumentException("agent_id is required for agentic_query_translator processor");
            }
            return new AgenticQueryTranslatorProcessor(tag, description, ignoreFailure, mlClient, agentId, xContentRegistry);
        }
    }
}
