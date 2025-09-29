/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.neuralsearch.query.ext.AgentStepsSearchExtBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;

/**
 * Response processor that adds agent context information to search response extensions
 */
@Getter
@AllArgsConstructor
@Log4j2
public class AgenticContextResponseProcessor implements SearchResponseProcessor {

    public static final String TYPE = "agentic_context";
    @Getter
    private final String tag;
    @Getter
    private final String description;
    @Getter
    private final boolean ignoreFailure;
    @Getter
    private final boolean includeAgentSteps;
    @Getter
    private final boolean includeDslQuery;

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        return response;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        EventStatsManager.increment(EventStatName.AGENTIC_CONTEXT_PROCESSOR_EXECUTIONS);
        if (requestContext == null) {
            return response;
        }

        Object agentStepsSummary = requestContext.getAttribute("agent_steps_summary");
        Object memoryId = requestContext.getAttribute("memory_id");
        Object dslQuery = requestContext.getAttribute("dsl_query");

        // Validate types when attributes are present
        if (agentStepsSummary != null && !(agentStepsSummary instanceof String)) {
            throw new RuntimeException("agent_steps_summary must be a String, but got: " + agentStepsSummary.getClass().getSimpleName());
        }

        if (memoryId != null && !(memoryId instanceof String)) {
            throw new RuntimeException("memory_id must be a String, but got: " + memoryId.getClass().getSimpleName());
        }

        if (dslQuery != null && !(dslQuery instanceof String)) {
            throw new RuntimeException("dsl_query must be a String, but got: " + dslQuery.getClass().getSimpleName());
        }

        String agentStepsStr = null;
        if (includeAgentSteps && agentStepsSummary != null && !((String) agentStepsSummary).trim().isEmpty()) {
            agentStepsStr = (String) agentStepsSummary;
        }

        // Always include memory_id when available
        String memoryIdStr = null;
        if (memoryId != null && !((String) memoryId).trim().isEmpty()) {
            memoryIdStr = (String) memoryId;
        }

        String dslQueryStr = null;
        if (includeDslQuery && dslQuery != null && !((String) dslQuery).trim().isEmpty()) {
            dslQueryStr = (String) dslQuery;
        }

        if (agentStepsStr == null && memoryIdStr == null && dslQueryStr == null) {
            return response;
        }

        List<SearchExtBuilder> newExtensions = new ArrayList<>();
        List<SearchExtBuilder> existingExtensions = response.getInternalResponse().getSearchExtBuilders();
        if (existingExtensions != null) {
            newExtensions.addAll(existingExtensions);
        }
        newExtensions.add(new AgentStepsSearchExtBuilder(agentStepsStr, memoryIdStr, dslQueryStr));

        SearchResponseSections newInternalResponse = new SearchResponseSections(
            response.getHits(),
            response.getAggregations(),
            response.getSuggest(),
            response.isTimedOut(),
            response.isTerminatedEarly(),
            null,
            response.getNumReducePhases(),
            newExtensions
        );

        return new SearchResponse(
            newInternalResponse,
            response.getScrollId(),
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getTook().millis(),
            response.getPhaseTook(),
            response.getShardFailures(),
            response.getClusters(),
            response.pointInTimeId()
        );
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public AgenticContextResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            boolean includeAgentSteps = readBooleanProperty(TYPE, tag, config, "agent_steps_summary", false);
            boolean includeDslQuery = readBooleanProperty(TYPE, tag, config, "dsl_query", false);

            return new AgenticContextResponseProcessor(tag, description, ignoreFailure, includeAgentSteps, includeDslQuery);
        }
    }
}
