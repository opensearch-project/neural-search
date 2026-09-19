/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

/**
 * Base class for all score hybridization processors. This class is responsible for executing the score hybridization process.
 * It is a pipeline processor that is executed after the query phase and before the fetch phase.
 * <p>
 * Subclasses supply the techniques to apply and the stats to record; the harvesting of query phase results and
 * the handoff to {@link NormalizationProcessorWorkflow} are identical for all of them and are implemented here.
 */
@Log4j2
public abstract class AbstractScoreHybridizationProcessor implements SearchPhaseResultsProcessor {
    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor. This method is called when there is no pipeline context
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     */
    @Override
    public <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    ) {
        hybridizeScores(searchPhaseResult, searchPhaseContext, Optional.empty());
    }

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor. This method is called when there is pipeline context
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     * @param requestContext {@link PipelineProcessingContext} processing context of search pipeline
     * @param <Result>
     */
    @Override
    public <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final PipelineProcessingContext requestContext
    ) {
        hybridizeScores(searchPhaseResult, searchPhaseContext, Optional.ofNullable(requestContext));
    }

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor
     * @param searchPhaseResult
     * @param searchPhaseContext
     * @param requestContextOptional
     * @param <Result>
     */
    <Result extends SearchPhaseResult> void hybridizeScores(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext searchPhaseContext,
        Optional<PipelineProcessingContext> requestContextOptional
    ) {
        if (shouldSkipProcessor(searchPhaseResult)) {
            log.debug("Query results are not compatible with processor of type [{}]", getType());
            return;
        }
        List<QuerySearchResult> querySearchResults = getQueryPhaseSearchResults(searchPhaseResult);
        Optional<FetchSearchResult> fetchSearchResult = getFetchSearchResults(searchPhaseResult);
        boolean explain = Objects.nonNull(searchPhaseContext.getRequest().source().explain())
            && searchPhaseContext.getRequest().source().explain();
        recordStats();
        NormalizationProcessorWorkflowExecuteRequest request = NormalizationProcessorWorkflowExecuteRequest.builder()
            .querySearchResults(querySearchResults)
            .fetchSearchResultOptional(fetchSearchResult)
            .normalizationTechnique(getNormalizationTechnique())
            .combinationTechnique(getCombinationTechnique())
            .explain(explain)
            .pipelineProcessingContext(requestContextOptional.orElse(null))
            .searchPhaseContext(searchPhaseContext)
            .build();
        getNormalizationWorkflow().execute(request);
    }

    /**
     * @return technique used to normalize scores of individual subqueries
     */
    protected abstract ScoreNormalizationTechnique getNormalizationTechnique();

    /**
     * @return technique used to combine normalized scores into a single score per document
     */
    protected abstract ScoreCombinationTechnique getCombinationTechnique();

    /**
     * @return workflow that applies the normalization and combination techniques to the query phase results
     */
    protected abstract NormalizationProcessorWorkflow getNormalizationWorkflow();

    /**
     * Records that this processor executed. Called once per hybrid query, before the workflow runs.
     */
    protected abstract void recordStats();

    @Override
    public SearchPhaseName getBeforePhase() {
        return SearchPhaseName.QUERY;
    }

    @Override
    public SearchPhaseName getAfterPhase() {
        return SearchPhaseName.FETCH;
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
        // Two kinds of result carry nothing to read, and QuerySearchResult#topDocs() throws on both. A shard whose request
        // was built after the first shard answered may reply with QuerySearchResult.nullInstance() when its query cannot
        // match (core's can-match shortcut for a match_none rewrite — the fused fast path's round 2, or any plain query
        // rewritten to match_none). And a partial reduce may already have consumed a shard's topDocs; that happens to a
        // hybrid result only when the request's hybrid was not visible to HybridQuerySearchRequestFilter, which otherwise
        // disables batched reduction for it. For THIS decision — is there a hybrid result to process at all — both count
        // as not hybrid, which is exact: a null instance never held one, and a consumed result cannot be told apart from a
        // non-hybrid one here. It is not a claim that a consumed hybrid result is processable: the workflow refuses those
        // (see NormalizationProcessorWorkflow#filterValidResults). Nor can every hybrid result be consumed when this runs:
        // core buffers the result whose arrival triggers a partial reduce and reduces the rest only after the phase-results
        // processors, so at least one non-null result is always still readable here.
        if (Objects.isNull(searchPhaseResult.queryResult())
            || searchPhaseResult.queryResult().isNull()
            || searchPhaseResult.queryResult().hasConsumedTopDocs()) {
            return false;
        }
        // check for delimiter at the end of the score docs.
        return Objects.nonNull(searchPhaseResult.queryResult().topDocs())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs)
            && searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs.length > 0
            && isHybridQueryStartStopElement(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs[0]);
    }

    @VisibleForTesting
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
}
