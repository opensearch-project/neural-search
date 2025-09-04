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
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.HashMap;
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
    private static final Gson gson = new Gson();;

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
                "Agentic search blocked - Invalid usage with other search features - Agent ID: [%s], Query: [%s]",
                agentId,
                agenticQuery.getQueryText()
            );
            requestListener.onFailure(new IllegalArgumentException(errorMessage));
            return;
        }

        executeAgentAsync(agenticQuery, request, requestListener);
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
        ActionListener<SearchRequest> requestListener
    ) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("query_text", agenticQuery.getQueryText());

        // Get index mapping from the search request
        if (request.indices() != null && request.indices().length > 0) {
            try {
                Map<String, String> indexMappings = NeuralSearchClusterUtil.instance().getIndexMapping(request.indices());
                parameters.put("index_mapping", indexMappings.toString());
            } catch (Exception e) {
                log.warn("Failed to get index mapping", e);
            }
        }

        if (agenticQuery.getQueryFields() != null && !agenticQuery.getQueryFields().isEmpty()) {
            parameters.put("query_fields", gson.toJson(agenticQuery.getQueryFields()));
        }

        mlClient.executeAgent(agentId, parameters, ActionListener.wrap(agentResponse -> {
            try {
                log.debug("Generated Query: [{}]", agentResponse);

                // Validate response size to prevent memory exhaustion
                if (agentResponse == null) {
                    String errorMessage = String.format(
                        Locale.ROOT,
                        "Agentic search failed - Null response from agent - Agent ID: [%s], Query: [%s]",
                        agentId,
                        agenticQuery.getQueryText()
                    );
                    throw new IllegalArgumentException(errorMessage);
                }

                if (agentResponse.length() > MAX_AGENT_RESPONSE_SIZE) {
                    String errorMessage = String.format(
                        Locale.ROOT,
                        "Agentic search blocked - Response size exceeded limit - Agent ID: [%s], Size: [%d], Query: [%s]. Maximum allowed size is %d characters.",
                        agentId,
                        agentResponse.length(),
                        agenticQuery.getQueryText(),
                        MAX_AGENT_RESPONSE_SIZE
                    );
                    throw new IllegalArgumentException(errorMessage);
                }

                // Parse the agent response to get the new search source
                BytesReference bytes = new BytesArray(agentResponse);
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
                "Agentic search failed - Agent execution error - Agent ID: [%s], Query: [%s], Error: [%s]",
                agentId,
                agenticQuery.getQueryText(),
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
