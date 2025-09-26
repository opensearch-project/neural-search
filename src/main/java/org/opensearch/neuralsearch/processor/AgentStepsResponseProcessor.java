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
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

/**
 * Response processor that adds agent steps summary to search response extensions
 */
@Getter
@AllArgsConstructor
@Log4j2
public class AgentStepsResponseProcessor implements SearchResponseProcessor {

    public static final String TYPE = "agent_steps";
    @Getter
    private final String description;
    @Getter
    private final String tag;
    @Getter
    private final boolean ignoreFailure;

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        return response;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        if (requestContext == null) {
            return response;
        }

        Object agentStepsSummary = requestContext.getAttribute("agent_steps_summary");
        Object memoryId = requestContext.getAttribute("memory_id");

        if (!(agentStepsSummary instanceof String) || ((String) agentStepsSummary).trim().isEmpty()) {
            return response;
        }

        String memoryIdStr = (memoryId instanceof String) ? (String) memoryId : null;

        List<SearchExtBuilder> newExtensions = new ArrayList<>();
        List<SearchExtBuilder> existingExtensions = response.getInternalResponse().getSearchExtBuilders();
        if (existingExtensions != null) {
            newExtensions.addAll(existingExtensions);
        }
        newExtensions.add(new AgentStepsSearchExtBuilder((String) agentStepsSummary, memoryIdStr));

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
        public AgentStepsResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            return new AgentStepsResponseProcessor(tag, description, ignoreFailure);
        }
    }
}
