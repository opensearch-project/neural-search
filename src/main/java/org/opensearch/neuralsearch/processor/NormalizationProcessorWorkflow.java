/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.query.QuerySearchResult;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class abstracts steps required for score normalization and combination, this includes pre-processing of income data
 * and post-processing for final results
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NormalizationProcessorWorkflow {

    /**
     * Return instance of workflow class. Making default constructor private for now
     * as we may use singleton pattern here and share one instance among processors
     * @return instance of NormalizationProcessorWorkflow
     */
    public static NormalizationProcessorWorkflow create() {
        return new NormalizationProcessorWorkflow();
    }

    /**
     * Start execution of this workflow
     * @param querySearchResults input data with QuerySearchResult from multiple shards
     * @param normalizationTechnique technique for score normalization
     * @param combinationTechnique technique for score combination
     */
    public void execute(
        final List<QuerySearchResult> querySearchResults,
        final ScoreNormalizationTechnique normalizationTechnique,
        final ScoreCombinationTechnique combinationTechnique
    ) {
        // pre-process data
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        // normalize
        ScoreNormalizer scoreNormalizer = new ScoreNormalizer();
        scoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

        // combine
        ScoreCombiner scoreCombiner = new ScoreCombiner();
        List<Float> combinedMaxScores = scoreCombiner.combineScores(queryTopDocs, combinationTechnique);

        // post-process data
        updateOriginalQueryResults(querySearchResults, queryTopDocs, combinedMaxScores);
    }

    private List<CompoundTopDocs> getQueryTopDocs(List<QuerySearchResult> querySearchResults) {
        List<CompoundTopDocs> queryTopDocs = querySearchResults.stream()
            .filter(searchResult -> searchResult.topDocs().topDocs instanceof CompoundTopDocs)
            .map(searchResult -> (CompoundTopDocs) searchResult.topDocs().topDocs)
            .collect(Collectors.toList());
        return queryTopDocs;
    }

    @VisibleForTesting
    protected void updateOriginalQueryResults(
        List<QuerySearchResult> querySearchResults,
        final List<CompoundTopDocs> queryTopDocs,
        List<Float> combinedMaxScores
    ) {
        TopDocsAndMaxScore[] topDocsAndMaxScores = new TopDocsAndMaxScore[querySearchResults.size()];
        for (int idx = 0; idx < querySearchResults.size(); idx++) {
            QuerySearchResult querySearchResult = querySearchResults.get(idx);
            TopDocsAndMaxScore topDocsAndMaxScore = querySearchResult.topDocs();
            if (!(topDocsAndMaxScore.topDocs instanceof CompoundTopDocs)) {
                continue;
            }
            topDocsAndMaxScores[idx] = topDocsAndMaxScore;
        }
        for (int i = 0; i < querySearchResults.size(); i++) {
            QuerySearchResult querySearchResult = querySearchResults.get(i);
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(i);
            if (Objects.isNull(updatedTopDocs)) {
                continue;
            }
            float maxScore = updatedTopDocs.totalHits.value > 0 ? updatedTopDocs.scoreDocs[0].score : 0.0f;
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(updatedTopDocs, maxScore);
            if (querySearchResult == null) {
                continue;
            }
            querySearchResult.topDocs(topDocsAndMaxScore, null);
            if (topDocsAndMaxScores[i] != null) {
                topDocsAndMaxScores[i].maxScore = combinedMaxScores.get(i);
            }
        }
    }
}
