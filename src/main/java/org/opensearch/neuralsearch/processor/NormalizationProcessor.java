/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QuerySearchResult;

import com.google.common.annotations.VisibleForTesting;

/**
 * Processor for score normalization and combination on post query search results. Updates query results with
 * normalized and combined scores for next phase (typically it's FETCH)
 */
@Log4j2
@AllArgsConstructor
public class NormalizationProcessor implements SearchPhaseResultsProcessor {
    public static final String TYPE = "normalization-processor";
    public static final String NORMALIZATION_CLAUSE = "normalization";
    public static final String COMBINATION_CLAUSE = "combination";
    public static final String TECHNIQUE = "technique";

    private final String tag;
    private final String description;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    final ScoreNormalizationTechnique normalizationTechnique;
    @Getter(AccessLevel.PACKAGE)
    final ScoreCombinationTechnique combinationTechnique;

    /**
     * Method abstracts functional aspect of score normalization and score combination. Exact methods for each processing stage
     * are set as part of class constructor
     * @param searchPhaseResult {@link SearchPhaseResults} DTO that has query search results. Results will be mutated as part of this method execution
     * @param searchPhaseContext {@link SearchContext}
     * @param <Result>
     */
    @Override
    public <Result extends SearchPhaseResult> void process(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext
    ) {
        if (shouldSearchResultsBeIgnored(searchPhaseResult)) {
            return;
        }

        TopDocsAndMaxScore[] topDocsAndMaxScores = getCompoundQueryTopDocsFromSearchPhaseResult(searchPhaseResult);
        List<CompoundTopDocs> queryTopDocs = Arrays.stream(topDocsAndMaxScores)
            .map(td -> td != null ? (CompoundTopDocs) td.topDocs : null)
            .collect(Collectors.toList());

        ScoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

        List<Float> combinedMaxScores = ScoreCombiner.combineScores(queryTopDocs, combinationTechnique);

        updateOriginalQueryResults(searchPhaseResult, queryTopDocs, topDocsAndMaxScores, combinedMaxScores);
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
        return true;
    }

    private <Result extends SearchPhaseResult> boolean shouldSearchResultsBeIgnored(SearchPhaseResults<Result> searchPhaseResult) {
        if (Objects.isNull(searchPhaseResult) || !(searchPhaseResult instanceof QueryPhaseResultConsumer)) {
            return true;
        }

        QueryPhaseResultConsumer queryPhaseResultConsumer = (QueryPhaseResultConsumer) searchPhaseResult;
        Optional<SearchPhaseResult> maybeResult = queryPhaseResultConsumer.getAtomicArray()
            .asList()
            .stream()
            .filter(Objects::nonNull)
            .findFirst();
        return isNotHybridQuery(maybeResult);
    }

    private boolean isNotHybridQuery(final Optional<SearchPhaseResult> maybeResult) {
        return maybeResult.isEmpty()
            || Objects.isNull(maybeResult.get().queryResult())
            || Objects.isNull(maybeResult.get().queryResult().topDocs())
            || !(maybeResult.get().queryResult().topDocs().topDocs instanceof CompoundTopDocs);
    }

    private <Result extends SearchPhaseResult> TopDocsAndMaxScore[] getCompoundQueryTopDocsFromSearchPhaseResult(
        final SearchPhaseResults<Result> results
    ) {
        List<Result> preShardResultList = results.getAtomicArray().asList();
        TopDocsAndMaxScore[] result = new TopDocsAndMaxScore[preShardResultList.size()];
        for (int idx = 0; idx < preShardResultList.size(); idx++) {
            Result shardResult = preShardResultList.get(idx);
            if (shardResult == null) {
                continue;
            }
            QuerySearchResult querySearchResult = shardResult.queryResult();
            TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
            if (!(topDocsAndMaxScore.topDocs instanceof CompoundTopDocs)) {
                continue;
            }
            result[idx] = topDocsAndMaxScore;
        }
        return result;
    }

    @VisibleForTesting
    protected <Result extends SearchPhaseResult> void updateOriginalQueryResults(
        final SearchPhaseResults<Result> results,
        final List<CompoundTopDocs> queryTopDocs,
        TopDocsAndMaxScore[] topDocsAndMaxScores,
        List<Float> combinedMaxScores
    ) {
        List<Result> preShardResultList = results.getAtomicArray().asList();
        for (int i = 0; i < preShardResultList.size(); i++) {
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(i);
            if (Objects.isNull(updatedTopDocs)) {
                continue;
            }
            float maxScore = updatedTopDocs.totalHits.value > 0 ? updatedTopDocs.scoreDocs[0].score : 0.0f;
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(updatedTopDocs, maxScore);
            QuerySearchResult querySearchResult = preShardResultList.get(i).queryResult();
            querySearchResult.topDocs(topDocsAndMaxScore, null);
            if (topDocsAndMaxScores[i] != null) {
                topDocsAndMaxScores[i].maxScore = combinedMaxScores.get(i);
            }
        }
    }
}
