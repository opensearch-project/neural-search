/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
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
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.neuralsearch.processor.collapse.CollapseDTO;
import org.opensearch.neuralsearch.processor.collapse.CollapseExecutor;
import org.opensearch.neuralsearch.processor.combination.CombineScoresDto;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.explain.CombinedExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;
import org.opensearch.neuralsearch.processor.explain.ExplanationPayload;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.query.QuerySearchResult;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForSubQuerySupport;
import static org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY;
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
    private static final String SUB_QUERY_SCORES_NAME = "hybridization_sub_query_scores";

    /**
     * Start execution of this workflow
     * @param request contains querySearchResults input data with QuerySearchResult
     * from multiple shards, fetchSearchResultOptional, normalizationTechnique technique for score normalization
     *  combinationTechnique technique for score combination, and nullable rankConstant only used in RRF technique
     */
    public void execute(final NormalizationProcessorWorkflowExecuteRequest request) {
        List<QuerySearchResult> querySearchResults = request.getQuerySearchResults();
        Optional<FetchSearchResult> fetchSearchResultOptional = request.getFetchSearchResultOptional();
        List<Integer> unprocessedDocIds = unprocessedDocIds(querySearchResults);

        // pre-process data
        log.debug("Pre-process query results");
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        explain(request, queryTopDocs);

        // Data transfer object for score normalization used to pass nullable rankConstant which is only used in RRF
        NormalizeScoresDTO normalizeScoresDTO = NormalizeScoresDTO.builder()
            .queryTopDocs(queryTopDocs)
            .normalizationTechnique(request.getNormalizationTechnique())
            .subQueryScores(request.isSubQueryScores())
            .searchPhaseContext(request.getSearchPhaseContext())
            .build();

        // normalize
        log.debug("Do score normalization");
        scoreNormalizer.normalizeScores(normalizeScoresDTO);

        CombineScoresDto combineScoresDTO = CombineScoresDto.builder()
            .queryTopDocs(queryTopDocs)
            .scoreCombinationTechnique(request.getCombinationTechnique())
            .querySearchResults(querySearchResults)
            .sort(evaluateSortCriteria(querySearchResults, queryTopDocs))
            .fromValueForSingleShard(getFromValueIfSingleShard(request))
            .isSingleShard(getIsSingleShard(request))
            .build();

        // combine
        log.debug("Do score combination");
        scoreCombiner.combineScores(combineScoresDTO);

        // post-process data
        log.debug("Post-process query results after score normalization and combination");
        updateOriginalQueryResults(combineScoresDTO, fetchSearchResultOptional.isPresent());
        updateOriginalFetchResults(
            request.getSearchPhaseContext(),
            querySearchResults,
            fetchSearchResultOptional,
            unprocessedDocIds,
            combineScoresDTO.getFromValueForSingleShard()
        );
    }

    private boolean getIsSingleShard(final NormalizationProcessorWorkflowExecuteRequest request) {
        final SearchPhaseContext searchPhaseContext = request.getSearchPhaseContext();
        return searchPhaseContext.getNumShards() == 1 || request.fetchSearchResultOptional.isEmpty() == false;
    }

    /**
     * Get value of from parameter when there is a single shard
     * and fetch phase is already executed
     * Ref https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/SearchService.java#L715
     */
    private int getFromValueIfSingleShard(final NormalizationProcessorWorkflowExecuteRequest request) {
        final SearchPhaseContext searchPhaseContext = request.getSearchPhaseContext();
        if (getIsSingleShard(request) == false) {
            return -1;
        }
        int from = searchPhaseContext.getRequest().source().from();
        // for the initial searchRequest, it creates a default search context which sets the value of
        // from to 0 if it's -1. That's not the case with SearchPhaseContext, that's why need to
        // explicitly set to 0 for the single shard case
        // Ref:
        // https://github.com/opensearch-project/OpenSearch/blob/2.18/server/src/main/java/org/opensearch/search/DefaultSearchContext.java#L288
        if (from == -1) {
            return 0;
        }
        return from;
    }

    /**
     * Collects explanations from normalization and combination techniques and save thme into pipeline context. Later that
     * information will be read by the response processor to add it to search response
     */
    private void explain(NormalizationProcessorWorkflowExecuteRequest request, List<CompoundTopDocs> queryTopDocs) {
        if (!request.isExplain()) {
            return;
        }
        // build final result object with all explain related information
        if (Objects.nonNull(request.getPipelineProcessingContext())) {
            Sort sortForQuery = evaluateSortCriteria(request.getQuerySearchResults(), queryTopDocs);
            Map<DocIdAtSearchShard, ExplanationDetails> normalizationExplain = scoreNormalizer.explain(
                queryTopDocs,
                (ExplainableTechnique) request.getNormalizationTechnique()
            );
            Map<SearchShard, List<ExplanationDetails>> combinationExplain = scoreCombiner.explain(
                queryTopDocs,
                request.getCombinationTechnique(),
                sortForQuery
            );
            Map<SearchShard, List<CombinedExplanationDetails>> combinedExplanations = new HashMap<>();
            for (Map.Entry<SearchShard, List<ExplanationDetails>> entry : combinationExplain.entrySet()) {
                List<CombinedExplanationDetails> combinedDetailsList = new ArrayList<>();
                for (ExplanationDetails explainDetail : entry.getValue()) {
                    DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(explainDetail.getDocId(), entry.getKey());
                    CombinedExplanationDetails combinedDetail = CombinedExplanationDetails.builder()
                        .normalizationExplanations(normalizationExplain.get(docIdAtSearchShard))
                        .combinationExplanations(explainDetail)
                        .build();
                    combinedDetailsList.add(combinedDetail);
                }
                combinedExplanations.put(entry.getKey(), combinedDetailsList);
            }

            ExplanationPayload explanationPayload = ExplanationPayload.builder()
                .explainPayload(Map.of(ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR, combinedExplanations))
                .build();
            // store explain object to pipeline context
            PipelineProcessingContext pipelineProcessingContext = request.getPipelineProcessingContext();
            pipelineProcessingContext.setAttribute(EXPLANATION_RESPONSE_KEY, explanationPayload);
        }
    }

    /**
     * Getting list of CompoundTopDocs from list of QuerySearchResult. Each CompoundTopDocs is for individual shard
     * @param querySearchResults collection of QuerySearchResult for all shards
     * @return collection of CompoundTopDocs, one object for each shard
     */
    private List<CompoundTopDocs> getQueryTopDocs(final List<QuerySearchResult> querySearchResults) {
        List<CompoundTopDocs> queryTopDocs = querySearchResults.stream()
            .filter(searchResult -> Objects.nonNull(searchResult.topDocs()))
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

    private void updateOriginalQueryResults(final CombineScoresDto combineScoresDTO, final boolean isFetchPhaseExecuted) {
        final List<QuerySearchResult> querySearchResults = combineScoresDTO.getQuerySearchResults();
        final List<CompoundTopDocs> queryTopDocs = getCompoundTopDocs(combineScoresDTO, querySearchResults);
        final Sort sort = combineScoresDTO.getSort();
        int totalScoreDocsCount = 0;

        // Get index of first non-empty CompoundTopDocs to check if collapse is enabled
        boolean isCollapseEnabled = false;
        int firstNonEmptyIndex = -1;
        for (int queryTopDocIndex = 0; queryTopDocIndex < queryTopDocs.size(); queryTopDocIndex++) {
            List<TopDocs> topDocsList = queryTopDocs.get(queryTopDocIndex).getTopDocs();
            if (!topDocsList.isEmpty() && topDocsList.getFirst() instanceof CollapseTopFieldDocs) {
                isCollapseEnabled = true;
                firstNonEmptyIndex = queryTopDocIndex;
                break;
            }
        }
        if (isCollapseEnabled) {
            CollapseExecutor collapseExecutor = new CollapseExecutor();
            CollapseDTO collapseDTO = new CollapseDTO(
                queryTopDocs,
                querySearchResults,
                sort,
                firstNonEmptyIndex,
                isFetchPhaseExecuted,
                combineScoresDTO
            );

            totalScoreDocsCount = collapseExecutor.executeCollapse(collapseDTO);
        } else {
            for (int shardIndex = 0; shardIndex < querySearchResults.size(); shardIndex++) {
                QuerySearchResult querySearchResult = querySearchResults.get(shardIndex);
                CompoundTopDocs updatedTopDocs = queryTopDocs.get(shardIndex);
                totalScoreDocsCount += updatedTopDocs.getScoreDocs().size();
                TopDocsAndMaxScore updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(
                    buildTopDocs(updatedTopDocs, sort),
                    NormalizationProcessorWorkflowUtil.maxScoreForShard(updatedTopDocs, sort != null)
                );
                // Fetch Phase had ran before the normalization phase, therefore update the from value in result of each shard.
                // This will ensure the trimming of the search results.
                if (isFetchPhaseExecuted) {
                    querySearchResult.from(combineScoresDTO.getFromValueForSingleShard());
                }
                querySearchResult.topDocs(updatedTopDocsAndMaxScore, querySearchResult.sortValueFormats());
            }
        }

        final int from = querySearchResults.get(0).from();
        if (from > totalScoreDocsCount) {
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
        final SearchPhaseContext searchPhaseContext,
        final List<QuerySearchResult> querySearchResults,
        final Optional<FetchSearchResult> fetchSearchResultOptional,
        final List<Integer> docIds,
        final int fromValueForSingleShard
    ) {
        if (fetchSearchResultOptional.isEmpty()) {
            return;
        }
        Map<String, float[]> scoreMap = HybridScoreRegistry.get(searchPhaseContext);
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
        // Scenario to handle when calculating the trimmed length of updated search hits
        // When normalization process runs after fetch phase, then search hits already fetched. Therefore, use the from value sent in the
        // search request to calculate the effective length of updated search hits array.
        int trimmedLengthOfSearchHits = topDocs.scoreDocs.length - fromValueForSingleShard;
        // iterate over the normalized/combined scores, that solves (1) and (3)
        SearchHit[] updatedSearchHitArray = new SearchHit[trimmedLengthOfSearchHits];
        for (int i = 0; i < trimmedLengthOfSearchHits; i++) {
            // Read topDocs after the desired from length
            ScoreDoc scoreDoc = topDocs.scoreDocs[i + fromValueForSingleShard];
            // get fetched hit content by doc_id
            SearchHit searchHit = docIdToSearchHit.get(scoreDoc.doc);
            // update score to normalized/combined value (3)
            searchHit.score(scoreDoc.score);

            // Check if inner hits are present
            boolean hasInnerHits = searchHit.getInnerHits() != null && !searchHit.getInnerHits().isEmpty();
            int shardIndex = querySearchResult.getShardIndex();
            String key = shardIndex + "_" + scoreDoc.doc;
            float[] subqueryScores = scoreMap != null ? scoreMap.get(key) : null;

            Map<String, DocumentField> documentFields = searchHit.getDocumentFields();

            // Check for all the conditions
            boolean shouldAddHybridScores = subqueryScores != null
                && documentFields.containsKey(SUB_QUERY_SCORES_NAME) == false
                && isClusterOnOrAfterMinReqVersionForSubQuerySupport()
                && hasInnerHits == false;

            if (shouldAddHybridScores) {
                // Add it as a field rather than modifying _source
                List<Object> hybridScores = new ArrayList<>(subqueryScores.length);
                for (float score : subqueryScores) {
                    hybridScores.add(score);
                }
                searchHit.setDocumentField(SUB_QUERY_SCORES_NAME, new DocumentField(SUB_QUERY_SCORES_NAME, hybridScores));
            }
            updatedSearchHitArray[i] = searchHit;
        }
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
