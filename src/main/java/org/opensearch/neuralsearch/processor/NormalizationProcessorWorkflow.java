/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;

import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.query.QuerySearchResult;

/**
 * Class abstracts steps required for score normalization and combination, this includes pre-processing of incoming data
 * and post-processing of final results
 */
@AllArgsConstructor
public class NormalizationProcessorWorkflow {

    private final ScoreNormalizer scoreNormalizer;
    private final ScoreCombiner scoreCombiner;

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
        scoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

        // combine
        scoreCombiner.combineScores(queryTopDocs, combinationTechnique);

        // post-process data
        updateOriginalQueryResults(querySearchResults, queryTopDocs);
    }

    /**
     * Getting list of CompoundTopDocs from list of QuerySearchResult. Each CompoundTopDocs is for individual shard
     * @param querySearchResults collection of QuerySearchResult for all shards
     * @return collection of CompoundTopDocs, one object for each shard
     */
    private List<CompoundTopDocs> getQueryTopDocs(final List<QuerySearchResult> querySearchResults) {
        List<CompoundTopDocs> queryTopDocs = querySearchResults.stream()
            .filter(searchResult -> Objects.nonNull(searchResult.topDocs()))
            .filter(searchResult -> searchResult.topDocs().topDocs instanceof CompoundTopDocs)
            .map(searchResult -> (CompoundTopDocs) searchResult.topDocs().topDocs)
            .collect(Collectors.toList());
        return queryTopDocs;
    }

    private void updateOriginalQueryResults(final List<QuerySearchResult> querySearchResults, final List<CompoundTopDocs> queryTopDocs) {
        for (int i = 0; i < querySearchResults.size(); i++) {
            QuerySearchResult querySearchResult = querySearchResults.get(i);
            if (!(querySearchResult.topDocs().topDocs instanceof CompoundTopDocs) || Objects.isNull(queryTopDocs.get(i))) {
                continue;
            }
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(i);
            float maxScore = updatedTopDocs.totalHits.value > 0 ? updatedTopDocs.scoreDocs[0].score : 0.0f;
            TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(updatedTopDocs, maxScore);
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, null);
        }
    }
}
