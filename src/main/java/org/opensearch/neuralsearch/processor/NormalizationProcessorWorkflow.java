/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.FieldDoc;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.combination.CombineScoresDto;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import static org.opensearch.neuralsearch.processor.combination.ScoreCombiner.MAX_SCORE_WHEN_NO_HITS_FOUND;
import static org.opensearch.neuralsearch.search.util.HybridSearchSortUtil.evaluateSortCriteria;

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
        final Optional<FetchSearchResult> fetchSearchResultOptional,
        final ScoreNormalizationTechnique normalizationTechnique,
        final ScoreCombinationTechnique combinationTechnique,
        final int fromValueForSingleShard,
        final boolean isSingleShard
    ) {
        // save original state
        List<Integer> unprocessedDocIds = unprocessedDocIds(querySearchResults);

        // pre-process data
        log.debug("Pre-process query results");
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        // normalize
        log.debug("Do score normalization");
        scoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

        CombineScoresDto combineScoresDTO = CombineScoresDto.builder()
            .queryTopDocs(queryTopDocs)
            .scoreCombinationTechnique(combinationTechnique)
            .querySearchResults(querySearchResults)
            .sort(evaluateSortCriteria(querySearchResults, queryTopDocs))
            .fromValueForSingleShard(fromValueForSingleShard)
            .isSingleShard(isSingleShard)
            .build();

        // combine
        log.debug("Do score combination");
        scoreCombiner.combineScores(combineScoresDTO);

        // post-process data
        log.debug("Post-process query results after score normalization and combination");
        updateOriginalQueryResults(combineScoresDTO);
        updateOriginalFetchResults(querySearchResults, fetchSearchResultOptional, unprocessedDocIds, fromValueForSingleShard);
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
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "query results were not formatted correctly by the hybrid query; sizes of querySearchResults [%d] and queryTopDocs [%d] must match",
                    querySearchResults.size(),
                    queryTopDocs.size()
                )
            );
        }
        return queryTopDocs;
    }

    private void updateOriginalQueryResults(final CombineScoresDto combineScoresDTO) {
        final List<QuerySearchResult> querySearchResults = combineScoresDTO.getQuerySearchResults();
        final List<CompoundTopDocs> queryTopDocs = getCompoundTopDocs(combineScoresDTO, querySearchResults);
        final Sort sort = combineScoresDTO.getSort();
        final int from = querySearchResults.get(0).from();
        int totalScoreDocsCount = 0;
        for (int index = 0; index < querySearchResults.size(); index++) {
            QuerySearchResult querySearchResult = querySearchResults.get(index);
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(index);
            totalScoreDocsCount += updatedTopDocs.getScoreDocs().size();
            TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(
                buildTopDocs(updatedTopDocs, sort),
                maxScoreForShard(updatedTopDocs, sort != null)
            );
            if (combineScoresDTO.isSingleShard()) {
                querySearchResult.from(combineScoresDTO.getFromValueForSingleShard());
            }
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, querySearchResult.sortValueFormats());
        }

        if ((from > 0 || combineScoresDTO.getFromValueForSingleShard() > 0)
            && (from > totalScoreDocsCount || combineScoresDTO.getFromValueForSingleShard() > totalScoreDocsCount)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Reached end of search result, increase pagination_depth value to see more results")
            );
        }
    }

    private List<CompoundTopDocs> getCompoundTopDocs(CombineScoresDto combineScoresDTO, List<QuerySearchResult> querySearchResults) {
        final List<CompoundTopDocs> queryTopDocs = combineScoresDTO.getQueryTopDocs();
        if (querySearchResults.size() != queryTopDocs.size()) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "query results were not formatted correctly by the hybrid query; sizes of querySearchResults [%d] and queryTopDocs [%d] must match",
                    querySearchResults.size(),
                    queryTopDocs.size()
                )
            );
        }
        return queryTopDocs;
    }

    /**
     * Get Max score on Shard
     * @param updatedTopDocs updatedTopDocs compound top docs on a shard
     * @param isSortEnabled if sort is enabled or disabled
     * @return  max score
     */
    private float maxScoreForShard(CompoundTopDocs updatedTopDocs, boolean isSortEnabled) {
        if (updatedTopDocs.getTotalHits().value == 0 || updatedTopDocs.getScoreDocs().isEmpty()) {
            return MAX_SCORE_WHEN_NO_HITS_FOUND;
        }
        if (isSortEnabled) {
            float maxScore = MAX_SCORE_WHEN_NO_HITS_FOUND;
            // In case of sorting iterate over score docs and deduce the max score
            for (ScoreDoc scoreDoc : updatedTopDocs.getScoreDocs()) {
                maxScore = Math.max(maxScore, scoreDoc.score);
            }
            return maxScore;
        }
        // If it is a normal hybrid query then first entry of score doc will have max score
        return updatedTopDocs.getScoreDocs().get(0).score;
    }

    /**
     * Get Top Docs on Shard
     * @param updatedTopDocs compound top docs on a shard
     * @param sort  sort criteria
     * @return TopDocs which will be instance of TopFieldDocs  if sort is enabled.
     */
    private TopDocs buildTopDocs(CompoundTopDocs updatedTopDocs, Sort sort) {
        if (sort != null) {
            return new TopFieldDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new FieldDoc[0]), sort.getSort());
        }
        return new TopDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new ScoreDoc[0]));
    }

    /**
     * A workaround for a single shard case, fetch has happened, and we need to update both fetch and query results
     */
    private void updateOriginalFetchResults(
        final List<QuerySearchResult> querySearchResults,
        final Optional<FetchSearchResult> fetchSearchResultOptional,
        final List<Integer> docIds,
        final int fromValueForSingleShard
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
        // checking case when results are cached
        boolean requestCache = Objects.nonNull(querySearchResults)
            && !querySearchResults.isEmpty()
            && Objects.nonNull(querySearchResults.get(0).getShardSearchRequest().requestCache())
            && querySearchResults.get(0).getShardSearchRequest().requestCache();

        SearchHit[] searchHitArray = getSearchHits(docIds, fetchSearchResult, requestCache);

        // create map of docId to index of search hits. This solves (2), duplicates are from
        // delimiter and start/stop elements, they all have same valid doc_id. For this map
        // we use doc_id as a key, and all those special elements are collapsed into a single
        // key-value pair.
        Map<Integer, SearchHit> docIdToSearchHit = new HashMap<>();
        for (int i = 0; i < searchHitArray.length; i++) {
            int originalDocId = docIds.get(i);
            docIdToSearchHit.put(originalDocId, searchHitArray[i]);
        }

        QuerySearchResult querySearchResult = querySearchResults.get(0);
        TopDocs topDocs = querySearchResult.topDocs().topDocs;

        // iterate over the normalized/combined scores, that solves (1) and (3)
        SearchHit[] updatedSearchHitArray = new SearchHit[topDocs.scoreDocs.length - fromValueForSingleShard];
        for (int i = fromValueForSingleShard; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            // get fetched hit content by doc_id
            SearchHit searchHit = docIdToSearchHit.get(scoreDoc.doc);
            // update score to normalized/combined value (3)
            searchHit.score(scoreDoc.score);
            updatedSearchHitArray[i - fromValueForSingleShard] = searchHit;
        }

        // iterate over the normalized/combined scores, that solves (1) and (3)
        // SearchHit[] updatedSearchHitArray = Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> {
        // // get fetched hit content by doc_id
        // SearchHit searchHit = docIdToSearchHit.get(scoreDoc.doc);
        // // update score to normalized/combined value (3)
        // searchHit.score(scoreDoc.score);
        // return searchHit;
        // }).toArray(SearchHit[]::new);
        SearchHits updatedSearchHits = new SearchHits(
            updatedSearchHitArray,
            querySearchResult.getTotalHits(),
            querySearchResult.getMaxScore()
        );
        fetchSearchResult.hits(updatedSearchHits);
    }

    private SearchHit[] getSearchHits(final List<Integer> docIds, final FetchSearchResult fetchSearchResult, final boolean requestCache) {
        SearchHits searchHits = fetchSearchResult.hits();
        SearchHit[] searchHitArray = searchHits.getHits();
        // validate the both collections are of the same size
        if (Objects.isNull(searchHitArray)) {
            throw new IllegalStateException(
                "score normalization processor cannot produce final query result, fetch query phase returns empty results"
            );
        }
        // in case of cached request results of fetch and query may be different, only restriction is
        // that number of query results size is greater or equal size of fetch results
        if ((!requestCache && searchHitArray.length != docIds.size()) || requestCache && docIds.size() < searchHitArray.length) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "score normalization processor cannot produce final query result, the number of documents after fetch phase [%d] is different from number of documents from query phase [%d]",
                    searchHitArray.length,
                    docIds.size()
                )
            );
        }
        return searchHitArray;
    }

    private List<Integer> unprocessedDocIds(final List<QuerySearchResult> querySearchResults) {
        List<Integer> docIds = querySearchResults.isEmpty()
            ? List.of()
            : Arrays.stream(querySearchResults.get(0).topDocs().topDocs.scoreDocs)
                .map(scoreDoc -> scoreDoc.doc)
                .collect(Collectors.toList());
        return docIds;
    }
}
