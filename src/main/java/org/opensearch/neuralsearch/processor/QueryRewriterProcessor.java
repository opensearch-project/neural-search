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
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

@Log4j2
public class QueryRewriterProcessor extends AbstractProcessor implements SearchRequestProcessor {

    public static final String TYPE = "query_rewriter";
    private final MLCommonsClientAccessor mlClient;
    private final String agentId;
    private final NamedXContentRegistry xContentRegistry;
    private static final Gson gson = new Gson();
    private static final Map<String, String> indexMappingCache = new ConcurrentHashMap<>();

    QueryRewriterProcessor(
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
        executeAgentAsync(agenticQuery, request, requestListener);
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
                String cacheKey = String.join(",", request.indices());
                String cachedMappingJson = indexMappingCache.get(cacheKey);

                if (cachedMappingJson == null) {
                    List<String> indexMappings = NeuralSearchClusterUtil.instance().getIndexMapping(request.indices());
                    String combinedMapping = String.join(", ", indexMappings);
                    cachedMappingJson = gson.toJson(combinedMapping);
                    indexMappingCache.put(cacheKey, cachedMappingJson);
                }

                parameters.put("index_mapping", cachedMappingJson);
            } catch (Exception e) {
                log.warn("Failed to get index mapping", e);
            }
        }

        if (agenticQuery.getQueryFields() != null && !agenticQuery.getQueryFields().isEmpty()) {
            parameters.put("query_fields", String.join(",", agenticQuery.getQueryFields()));
        }

        mlClient.executeAgent(agentId, parameters, ActionListener.wrap(agentResponse -> {
            try {
                log.info("Generated Query: [{}]", agentResponse);

                // Parse the agent response to get the new search source
                BytesReference bytes = new BytesArray(agentResponse);
                try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry, null, bytes.streamInput())) {
                    SearchSourceBuilder newSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                    request.source(newSourceBuilder);
                }

                requestListener.onResponse(request);
            } catch (IOException e) {
                log.error("Failed to parse agent response", e);
                requestListener.onFailure(e);
            }
        }, e -> {
            log.error("Failed to execute agent", e);
            requestListener.onFailure(new IOException("Failed to execute agentic search", e));
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

        public Factory(MLCommonsClientAccessor mlClient, NamedXContentRegistry xContentRegistry) {
            this.mlClient = mlClient;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public QueryRewriterProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws IllegalArgumentException {
            String agentId = readStringProperty(TYPE, tag, config, "agent_id");
            if (agentId == null || agentId.trim().isEmpty()) {
                throw new IllegalArgumentException("agent_id is required for query_rewriter processor");
            }
            return new QueryRewriterProcessor(tag, description, ignoreFailure, mlClient, agentId, xContentRegistry);
        }
    }
}
