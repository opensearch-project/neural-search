/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import joptsimple.internal.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.EnumUtils;
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
     * Need all args constructor to validate parameters and fail fast
     * @param tag
     * @param description
     * @param normalizationTechnique
     * @param combinationTechnique
     */
    public NormalizationProcessor(
        final String tag,
        final String description,
        final String normalizationTechnique,
        final String combinationTechnique
    ) {
        this.tag = tag;
        this.description = description;
        validateParameters(normalizationTechnique, combinationTechnique);
        this.normalizationTechnique = ScoreNormalizationTechnique.valueOf(normalizationTechnique);
        this.combinationTechnique = ScoreCombinationTechnique.valueOf(combinationTechnique);
    }

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
        if (searchPhaseResult instanceof QueryPhaseResultConsumer) {
            QueryPhaseResultConsumer queryPhaseResultConsumer = (QueryPhaseResultConsumer) searchPhaseResult;
            Optional<SearchPhaseResult> maybeResult = queryPhaseResultConsumer.getAtomicArray()
                .asList()
                .stream()
                .filter(Objects::nonNull)
                .findFirst();
            if (isNotHybridQuery(maybeResult)) {
                return;
            }

            TopDocsAndMaxScore[] topDocsAndMaxScores = getCompoundQueryTopDocsForResult(searchPhaseResult);
            CompoundTopDocs[] queryTopDocs = Arrays.stream(topDocsAndMaxScores)
                .map(td -> td != null ? (CompoundTopDocs) td.topDocs : null)
                .collect(Collectors.toList())
                .toArray(CompoundTopDocs[]::new);

            ScoreNormalizer scoreNormalizer = new ScoreNormalizer();
            scoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

            ScoreCombiner scoreCombinator = new ScoreCombiner();
            List<Float> combinedMaxScores = scoreCombinator.combineScores(queryTopDocs, combinationTechnique);

            updateOriginalQueryResults(searchPhaseResult, queryTopDocs, topDocsAndMaxScores, combinedMaxScores);
        }
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

    protected void validateParameters(final String normalizationTechniqueName, final String combinationTechniqueName) {
        if (Strings.isNullOrEmpty(normalizationTechniqueName)) {
            throw new IllegalArgumentException("normalization technique cannot be empty");
        }
        if (Strings.isNullOrEmpty(combinationTechniqueName)) {
            throw new IllegalArgumentException("combination technique cannot be empty");
        }
        if (!EnumUtils.isValidEnum(ScoreNormalizationTechnique.class, normalizationTechniqueName)) {
            log.error(String.format(Locale.ROOT, "provided normalization technique [%s] is not supported", normalizationTechniqueName));
            throw new IllegalArgumentException("provided normalization technique is not supported");
        }
        if (!EnumUtils.isValidEnum(ScoreCombinationTechnique.class, combinationTechniqueName)) {
            log.error(String.format(Locale.ROOT, "provided combination technique [%s] is not supported", combinationTechniqueName));
            throw new IllegalArgumentException("provided combination technique is not supported");
        }
    }

    private boolean isNotHybridQuery(final Optional<SearchPhaseResult> maybeResult) {
        return maybeResult.isEmpty()
            || Objects.isNull(maybeResult.get().queryResult())
            || !(maybeResult.get().queryResult().topDocs().topDocs instanceof CompoundTopDocs);
    }

    private <Result extends SearchPhaseResult> TopDocsAndMaxScore[] getCompoundQueryTopDocsForResult(
        final SearchPhaseResults<Result> results
    ) {
        List<Result> preShardResultList = results.getAtomicArray().asList();
        TopDocsAndMaxScore[] result = new TopDocsAndMaxScore[results.getAtomicArray().length()];
        int idx = 0;
        for (Result shardResult : preShardResultList) {
            if (shardResult == null) {
                idx++;
                continue;
            }
            QuerySearchResult querySearchResult = shardResult.queryResult();
            TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
            if (!(topDocsAndMaxScore.topDocs instanceof CompoundTopDocs)) {
                idx++;
                continue;
            }
            result[idx++] = topDocsAndMaxScore;
        }
        return result;
    }

    @VisibleForTesting
    protected <Result extends SearchPhaseResult> void updateOriginalQueryResults(
        final SearchPhaseResults<Result> results,
        final CompoundTopDocs[] queryTopDocs,
        TopDocsAndMaxScore[] topDocsAndMaxScores,
        List<Float> combinedMaxScores
    ) {
        List<Result> preShardResultList = results.getAtomicArray().asList();
        for (int i = 0; i < preShardResultList.size(); i++) {
            QuerySearchResult querySearchResult = preShardResultList.get(i).queryResult();
            CompoundTopDocs updatedTopDocs = queryTopDocs[i];
            if (updatedTopDocs == null) {
                continue;
            }
            float maxScore = updatedTopDocs.totalHits.value > 0 ? updatedTopDocs.scoreDocs[0].score : 0.0f;
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(updatedTopDocs, maxScore);
            querySearchResult.topDocs(topDocsAndMaxScore, null);
            if (topDocsAndMaxScores[i] != null) {
                topDocsAndMaxScores[i].maxScore = combinedMaxScores.get(i);
            }
        }
    }
}
