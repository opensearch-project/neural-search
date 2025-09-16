/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
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
import org.opensearch.search.pipeline.SystemGeneratedProcessor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Log4j2
public class AgenticQueryTranslatorProcessor implements SearchRequestProcessor, SystemGeneratedProcessor {

    public static final String TYPE = "agentic_query_translator";
    private static final int MAX_AGENT_RESPONSE_SIZE = 10_000;
    private final MLCommonsClientAccessor mlClient;
    private final NamedXContentRegistry xContentRegistry;
    private final String tag;
    private final boolean ignoreFailure;
    private static final String DESCRIPTION =
        "This is a system generated search request processor which will be executed before agentic search request to execute an agent";
    private static final Gson gson = new Gson();

    AgenticQueryTranslatorProcessor(
        String tag,
        boolean ignoreFailure,
        MLCommonsClientAccessor mlClient,
        NamedXContentRegistry xContentRegistry
    ) {
        this.tag = tag;
        this.ignoreFailure = ignoreFailure;
        this.mlClient = mlClient;
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
            String errorMessage =
                "Agentic search blocked - Invalid usage with other search features like aggregation, sort, filters, collapse";
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
        String agentId = agenticQuery.getAgentId();
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
                    throw new IllegalArgumentException("Agentic search failed - Null response from agent");
                }

                if (agentResponse.length() > MAX_AGENT_RESPONSE_SIZE) {
                    String errorMessage = String.format(
                        Locale.ROOT,
                        "Agentic search blocked - Response size exceeded limit. Size: [%d], Maximum allowed size is %d characters.",
                        agentResponse.length(),
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
                String errorMessage = String.format(Locale.ROOT, "Agentic search failed - Parse error: [%s]", e.getMessage());
                requestListener.onFailure(new IOException(errorMessage, e));
            }
        }, e -> {
            String errorMessage = String.format(Locale.ROOT, "Agentic search failed - Agent execution error: [%s]", e.getMessage());
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

    @Override
    public String getTag() {
        return this.tag;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public boolean isIgnoreFailure() {
        return this.ignoreFailure;
    }

    @Override
    public ExecutionStage getExecutionStage() {
        // Execute before user-defined processors as agentic query would be replaced by the new DSL
        return ExecutionStage.PRE_USER_DEFINED;
    }

    @AllArgsConstructor
    public static class Factory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor> {
        private final MLCommonsClientAccessor mlClient;
        private final NamedXContentRegistry xContentRegistry;
        private final NeuralSearchSettingsAccessor settingsAccessor;

        @Override
        public boolean shouldGenerate(ProcessorGenerationContext context) {
            SearchRequest searchRequest = context.searchRequest();
            if (searchRequest == null || searchRequest.source() == null) {
                return false;
            }

            boolean hasAgenticQuery = searchRequest.source().query() instanceof AgenticSearchQueryBuilder;
            log.debug("Query type: {}, hasAgenticQuery: {}", searchRequest.source().query().getClass().getSimpleName(), hasAgenticQuery);

            return hasAgenticQuery;
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
            return new AgenticQueryTranslatorProcessor(tag, ignoreFailure, mlClient, xContentRegistry);
        }
    }
}
