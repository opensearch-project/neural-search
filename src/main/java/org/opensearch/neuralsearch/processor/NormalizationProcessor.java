/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.processor.combination.DedupCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MaxNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QuerySearchResult;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Processor for score normalization and combination on post query search results. Updates query results with
 * normalized and combined scores for next phase (typically it's FETCH)
 */
@Log4j2
@AllArgsConstructor
public class NormalizationProcessor implements SearchPhaseResultsProcessor {
    public static final String TYPE = "normalization-processor";

    private final String tag;
    private final String description;
    private final ScoreNormalizationTechnique normalizationTechnique;
    private final ScoreCombinationTechnique combinationTechnique;
    private final NormalizationProcessorWorkflow normalizationWorkflow;

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     */
    @Override
    public <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    ) {
        if (shouldSkipProcessor(searchPhaseResult)) {
            log.debug("Query results are not compatible with normalization processor");
            return;
        }
        List<QuerySearchResult> querySearchResults = getQueryPhaseSearchResults(searchPhaseResult);
        Optional<FetchSearchResult> fetchSearchResult = getFetchSearchResults(searchPhaseResult);

        if (normalizationTechnique instanceof MaxNormalizationTechnique && combinationTechnique instanceof DedupCombinationTechnique){
            HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) searchPhaseContext.getRequest().source().query();
            List<QueryBuilder> subQueries = hybridQueryBuilder.queries();
            if (subQueries.size()>2 || subQueries.size() < 2){
                throw new IllegalArgumentException("Number of subqueries cannot be greater or lesser than 2");
            }
            if (subQueries.get(0) instanceof KNNQueryBuilder || subQueries.get(0) instanceof  NeuralQueryBuilder){
                throw new IllegalArgumentException("Knn query cannot be 1st subquery");
            }

            if(subQueries.get(1) instanceof KNNQueryBuilder == false || subQueries.get(1) instanceof NeuralQueryBuilder==false){
                throw new IllegalArgumentException("Knn query should always be 2nd subquery");
            }
        }
        normalizationWorkflow.execute(querySearchResults, fetchSearchResult, normalizationTechnique, combinationTechnique);
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
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isIgnoreFailure() {
        return false;
    }

    private <Result extends SearchPhaseResult> boolean shouldSkipProcessor(SearchPhaseResults<Result> searchPhaseResult) {
        if (Objects.isNull(searchPhaseResult) || !(searchPhaseResult instanceof QueryPhaseResultConsumer)) {
            return true;
        }

        QueryPhaseResultConsumer queryPhaseResultConsumer = (QueryPhaseResultConsumer) searchPhaseResult;
        return queryPhaseResultConsumer.getAtomicArray().asList().stream().filter(Objects::nonNull).noneMatch(this::isHybridQuery);
    }

    /**
     * Return true if results are from hybrid query.
     * @param searchPhaseResult
     * @return true if results are from hybrid query
     */
    private boolean isHybridQuery(final SearchPhaseResult searchPhaseResult) {
        // check for delimiter at the end of the score docs.
        return Objects.nonNull(searchPhaseResult.queryResult())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs())
            && Objects.nonNull(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs)
            && searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs.length > 0
            && isHybridQueryStartStopElement(searchPhaseResult.queryResult().topDocs().topDocs.scoreDocs[0]);
    }

    private <Result extends SearchPhaseResult> List<QuerySearchResult> getQueryPhaseSearchResults(
        final SearchPhaseResults<Result> results
    ) {
        return results.getAtomicArray()
            .asList()
            .stream()
            .map(result -> result == null ? null : result.queryResult())
            .collect(Collectors.toList());
    }

    private <Result extends SearchPhaseResult> Optional<FetchSearchResult> getFetchSearchResults(
        final SearchPhaseResults<Result> searchPhaseResults
    ) {
        Optional<Result> optionalFirstSearchPhaseResult = searchPhaseResults.getAtomicArray().asList().stream().findFirst();
        return optionalFirstSearchPhaseResult.map(SearchPhaseResult::fetchResult);
    }
}
