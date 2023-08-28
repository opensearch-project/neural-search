/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;

/**
 * Class abstracts steps required for score normalization and combination, this includes pre-processing of incoming data
 * and post-processing of final results
 */
@AllArgsConstructor
@Log4j2
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
        final FetchSearchResult fetchSearchResult,
        final ScoreNormalizationTechnique normalizationTechnique,
        final ScoreCombinationTechnique combinationTechnique
    ) {
        // pre-process data
        log.debug("Pre-process query results");
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        // normalize
        log.debug("Do score normalization");
        scoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

        // combine
        log.debug("Do score combination");
        scoreCombiner.combineScores(queryTopDocs, combinationTechnique);

        // post-process data
        log.debug("Post-process query results after score normalization and combination");
        updateOriginalQueryResults(querySearchResults, queryTopDocs);
        updateOriginalFetchResults(querySearchResults, Optional.ofNullable(fetchSearchResult));
    }

    /**
     * Getting list of CompoundTopDocs from list of QuerySearchResult. Each CompoundTopDocs is for individual shard
     * @param querySearchResults collection of QuerySearchResult for all shards
     * @return collection of CompoundTopDocs, one object for each shard
     */
    private List<CompoundTopDocs> getQueryTopDocs(final List<QuerySearchResult> querySearchResults) {
        List<CompoundTopDocs> queryTopDocs = querySearchResults.stream()
            .filter(searchResult -> Objects.nonNull(searchResult.topDocs()))
            .map(querySearchResult -> querySearchResult.topDocs().topDocs)
            .map(CompoundTopDocs::new)
            .collect(Collectors.toList());
        if (queryTopDocs.size() != querySearchResults.size()) {
            log.warn("Some of querySearchResults are not produced by hybrid query");
        }
        return queryTopDocs;
    }

    private void updateOriginalQueryResults(final List<QuerySearchResult> querySearchResults, final List<CompoundTopDocs> queryTopDocs) {
        if (querySearchResults.size() != queryTopDocs.size()) {
            log.error(
                String.format(
                    Locale.ROOT,
                    "sizes of querySearchResults [%d] and queryTopDocs [%d] must match",
                    querySearchResults.size(),
                    queryTopDocs.size()
                )
            );
            throw new IllegalStateException("found inconsistent system state while processing score normalization and combination");
        }
        for (int index = 0; index < querySearchResults.size(); index++) {
            QuerySearchResult querySearchResult = querySearchResults.get(index);
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(index);
            float maxScore = updatedTopDocs.getTotalHits().value > 0 ? updatedTopDocs.getScoreDocs().get(0).score : 0.0f;

            // create final version of top docs with all updated values
            TopDocs topDocs = new TopDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new ScoreDoc[0]));

            TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, null);
        }
    }

    /**
     * A workaround for a single shard case, fetch has happened, and we need to update both fetch and query results
     */
    private void updateOriginalFetchResults(
        final List<QuerySearchResult> querySearchResults,
        final Optional<FetchSearchResult> fetchSearchResultOptional
    ) {
        if (fetchSearchResultOptional.isEmpty()) {
            return;
        }
        // fetch results have list of document content, that includes start/stop and
        // delimiter elements. list is in original order from query searcher. We need to:
        // 1. filter out start/stop and delimiter elements
        // 2. filter out duplicates from different sub-queries
        // 3. update original scores to normalized and combined values
        // 4. order scores based on normalized and combined values
        FetchSearchResult fetchSearchResult = fetchSearchResultOptional.get();
        SearchHits searchHits = fetchSearchResult.hits();

        // create map of docId to index of search hits, handles (2)
        Map<Integer, SearchHit> docIdToSearchHit = Arrays.stream(searchHits.getHits())
            .collect(Collectors.toMap(SearchHit::docId, Function.identity(), (a1, a2) -> a1));

        QuerySearchResult querySearchResult = querySearchResults.get(0);
        TopDocs topDocs = querySearchResult.topDocs().topDocs;
        // iterate over the normalized/combined scores, that solves (1) and (3)
        SearchHit[] updatedSearchHitArray = Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> {
            // get fetched hit content by doc_id
            SearchHit searchHit = docIdToSearchHit.get(scoreDoc.doc);
            // update score to normalized/combined value (3)
            searchHit.score(scoreDoc.score);
            return searchHit;
        }).toArray(SearchHit[]::new);
        SearchHits updatedSearchHits = new SearchHits(
            updatedSearchHitArray,
            querySearchResult.getTotalHits(),
            querySearchResult.getMaxScore()
        );
        fetchSearchResult.hits(updatedSearchHits);
    }
}
