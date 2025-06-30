/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

import java.util.Map;
import java.util.stream.Collectors;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.opensearch.neuralsearch.processor.combination.RRFScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.query.QuerySearchResult;

import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.SearchContext;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Processor for implementing reciprocal rank fusion technique on post
 * query search results. Updates query results with
 * normalized and combined scores for next phase (typically it's FETCH)
 * by using ranks from individual subqueries to calculate 'normalized'
 * scores before combining results from subqueries into final results
 */
@Log4j2
@AllArgsConstructor
public class RRFProcessor extends AbstractScoreHybridizationProcessor {
    public static final String TYPE = "score-ranker-processor";

    @Getter
    private final String tag;
    @Getter
    private final String description;
    private final ScoreNormalizationTechnique normalizationTechnique;
    private final ScoreCombinationTechnique combinationTechnique;
    private final NormalizationProcessorWorkflow normalizationWorkflow;
    private final boolean subQueryScores;

    private final Map<String, Runnable> combTechniqueIncrementers = Map.of(
        RRFScoreCombinationTechnique.TECHNIQUE_NAME,
        () -> EventStatsManager.increment(EventStatName.COMB_TECHNIQUE_RRF_EXECUTIONS)
    );

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     */
    @Override
    <Result extends SearchPhaseResult> void hybridizeScores(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext searchPhaseContext,
        Optional<PipelineProcessingContext> requestContextOptional
    ) {
        if (shouldSkipProcessor(searchPhaseResult)) {
            log.debug("Query results are not compatible with RRF processor");
            return;
        }
        List<QuerySearchResult> querySearchResults = getQueryPhaseSearchResults(searchPhaseResult);
        Optional<FetchSearchResult> fetchSearchResult = getFetchSearchResults(searchPhaseResult);
        boolean explain = Objects.nonNull(searchPhaseContext.getRequest().source().explain())
            && searchPhaseContext.getRequest().source().explain();
        recordStats(combinationTechnique);
        // make data transfer object to pass in, execute will get object with 4 or 5 fields, depending
        // on coming from NormalizationProcessor or RRFProcessor
        NormalizationProcessorWorkflowExecuteRequest normalizationExecuteDTO = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(fetchSearchResult)
            .normalizationTechnique(normalizationTechnique)
            .combinationTechnique(combinationTechnique)
            .subQueryScores(subQueryScores)
            .explain(explain)
            .pipelineProcessingContext(requestContextOptional.orElse(null))
            .searchPhaseContext(searchPhaseContext)
            .build();
        normalizationWorkflow.execute(normalizationExecuteDTO);
    }

    @Override
    public SearchPhaseName getBeforePhase() {
        return SearchPhaseName.QUERY;
    }

    @Override
    public SearchPhaseName getAfterPhase() {
        return SearchPhaseName.FETCH;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean isIgnoreFailure() {
        return false;
    }

    @VisibleForTesting
    <Result extends SearchPhaseResult> boolean shouldSkipProcessor(SearchPhaseResults<Result> searchPhaseResult) {
        if (Objects.isNull(searchPhaseResult) || !(searchPhaseResult instanceof QueryPhaseResultConsumer queryPhaseResultConsumer)) {
            return true;
        }

        return queryPhaseResultConsumer.getAtomicArray().asList().stream().filter(Objects::nonNull).noneMatch(this::isHybridQuery);
    }

    /**
     * Return true if results are from hybrid query.
     * @param searchPhaseResult
     * @return true if results are from hybrid query
     */
    @VisibleForTesting
    boolean isHybridQuery(final SearchPhaseResult searchPhaseResult) {
        // check for delimiter at the end of the score docs.
        return Objects.nonNull(searchPhaseResult.queryResult())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs)
            && searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs.length > 0
            && isHybridQueryStartStopElement(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs[0]);
    }

    <Result extends SearchPhaseResult> List<QuerySearchResult> getQueryPhaseSearchResults(final SearchPhaseResults<Result> results) {
        return results.getAtomicArray()
            .asList()
            .stream()
            .map(result -> result == null ? null : result.queryResult())
            .collect(Collectors.toList());
    }

    @VisibleForTesting
    <Result extends SearchPhaseResult> Optional<FetchSearchResult> getFetchSearchResults(
        final SearchPhaseResults<Result> searchPhaseResults
    ) {
        Optional<Result> optionalFirstSearchPhaseResult = searchPhaseResults.getAtomicArray().asList().stream().findFirst();
        return optionalFirstSearchPhaseResult.map(SearchPhaseResult::fetchResult);
    }

    private void recordStats(ScoreCombinationTechnique combinationTechnique) {
        EventStatsManager.increment(EventStatName.RRF_PROCESSOR_EXECUTIONS);
        Optional.of(combTechniqueIncrementers.get(combinationTechnique.techniqueName())).ifPresent(Runnable::run);
    }
}
