/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombiner.MAX_SCORE_WHEN_NO_HITS_FOUND;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchSortUtil.evaluateSortCriteria;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.lucene.search.*;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.combination.CombineScoresDto;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QuerySearchResult;

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
public class RRFProcessor implements SearchPhaseResultsProcessor {
    public static final String TYPE = "score-ranker-processor";

    private final String tag;
    private final String description;
    private final int rankConstant;
    private final int x;
    private final int y;
    //private final ScoreNormalizationTechnique normalizationTechnique;
    //private final ScoreCombinationTechnique combinationTechnique;
    //private final NormalizationProcessorWorkflow normalizationWorkflow;
    private static final int DEFAULT_RANK_CONSTANT = 60;






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
            log.debug("Query results are not compatible with RRF processor");
            return;
        }
        List<QuerySearchResult> querySearchResults = getQueryPhaseSearchResults(searchPhaseResult);
        Optional<FetchSearchResult> fetchSearchResult = getFetchSearchResults(searchPhaseResult);
        //this is for rare cases of 1 shard return from query phase, ignore this for now
        final Optional<FetchSearchResult> fetchSearchResultOptional = fetchSearchResult;
        //normalizationWorkflow.execute(querySearchResults, fetchSearchResult, normalizationTechnique, combinationTechnique);
        //implement RRF algorithm using querySearchResults(may need more or different parameters)
        /**
         * This is from NormalizationProcessorWorkflow and ScoreNormalizer
         */

        // save original state
        List<Integer> unprocessedDocIds = unprocessedDocIds(querySearchResults);

        // pre-process data
        log.debug("Pre-process query results");
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        // normalize
        log.debug("Do score normalization");
        getRankScores(queryTopDocs);

        CombineScoresDto combineScoresDTO = CombineScoresDto.builder()
                .queryTopDocs(queryTopDocs)
                .scoreCombinationTechnique(combinationTechnique)
                .querySearchResults(querySearchResults)
                .sort(evaluateSortCriteria(querySearchResults, queryTopDocs))
                .build();

        // combine
        log.debug("Do score combination");
        ScoreCombiner(combineScoresDTO);

        // post-process data
        log.debug("Post-process query results after score normalization and combination");
        updateOriginalQueryResults(combineScoresDTO);
        updateOriginalFetchResults(querySearchResults, fetchSearchResultOptional, unprocessedDocIds);

        /**
         * This is from NormalizationProcessorWorkflow
         */
    }

    //@Override
    //public void normalize(final List<CompoundTopDocs> queryTopDocs) {
        // get l2 norms for each sub-query
        //List<Float> rankScoresPerSubquery = getRankScores(queryTopDocs);

        // do normalization using actual score and l2 norm
        //for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            //if (Objects.isNull(compoundQueryTopDocs)) {
                //continue;
            //}
            //List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            //for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                //TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                //for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    //scoreDoc.score = rankScoresPerSubquery.get(j);
                //}
            //}
        //}
    //}

    public void getRankScores(final List<CompoundTopDocs> queryTopDocs) {
        // find any non-empty compound top docs, it's either empty if shard does not have any results for all of sub-queries,
        // or it has results for all the sub-queries. In edge case of shard having results only for one sub-query, there will be TopDocs for
        // rest of sub-queries with zero total hits
        int numOfSubqueries = queryTopDocs.stream()
                .filter(Objects::nonNull)
                .filter(topDocs -> topDocs.getTopDocs().size() > 0)
                .findAny()
                .get()
                .getTopDocs()
                .size();
        //float[] rankScores = new float[numOfSubqueries];
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int numSubQueriesBound = topDocsPerSubQuery.size();
            for (int index = 0; index < numSubQueriesBound; index++) {
                int numDocsPerSubQueryBound = topDocsPerSubQuery.get(index).scoreDocs.length;
                for (int j = 0; j < numDocsPerSubQueryBound; j++) {
                    topDocsPerSubQuery.get(index).scoreDocs[j].score = (float) (1/(rankConstant + j + 1));
                }

            }
        }
    }

    /**
     * This section is copied and modified from L2ScoreNormalizationTechnique
     */

    /**
     * This is from NormalizationProcessorWorkflow
     */

    private void updateOriginalQueryResults(final CombineScoresDto combineScoresDTO) {
        final List<QuerySearchResult> querySearchResults = combineScoresDTO.getQuerySearchResults();
        final List<CompoundTopDocs> queryTopDocs = getCompoundTopDocs(combineScoresDTO, querySearchResults);
        final Sort sort = combineScoresDTO.getSort();
        for (int index = 0; index < querySearchResults.size(); index++) {
            QuerySearchResult querySearchResult = querySearchResults.get(index);
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(index);
            TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(
                    buildTopDocs(updatedTopDocs, sort),
                    maxScoreForShard(updatedTopDocs, sort != null)
            );
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, querySearchResult.sortValueFormats());
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
            final List<Integer> docIds
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
    /**
     * This is from NormalizationProcessorWorkflow
     */


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
