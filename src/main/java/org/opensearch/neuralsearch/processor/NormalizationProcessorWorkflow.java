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
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortedNumericSortField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
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
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.MAX_SCORE_WHEN_NO_HITS_FOUND;
import org.opensearch.search.sort.SortedWiderNumericSortField;

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
        final ScoreCombinationTechnique combinationTechnique
    ) {
        // save original state
        List<Integer> unprocessedDocIds = unprocessedDocIds(querySearchResults);

        // pre-process data
        log.debug("Pre-process query results");
        List<CompoundTopDocs> queryTopDocs = getQueryTopDocs(querySearchResults);

        // normalize
        log.debug("Do score normalization");
        scoreNormalizer.normalizeScores(queryTopDocs, normalizationTechnique);

        // Check if sort is enabled
        Sort sort = evaluateSortCriteria(queryTopDocs);

        // combine
        log.debug("Do score combination");
        scoreCombiner.combineScores(queryTopDocs, combinationTechnique, sort);

        // post-process data
        log.debug("Post-process query results after score normalization and combination");
        updateOriginalQueryResults(querySearchResults, queryTopDocs, sort);
        updateOriginalFetchResults(querySearchResults, fetchSearchResultOptional, unprocessedDocIds);
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

    private void updateOriginalQueryResults(
        final List<QuerySearchResult> querySearchResults,
        final List<CompoundTopDocs> queryTopDocs,
        final Sort sort
    ) {
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
        for (int index = 0; index < querySearchResults.size(); index++) {
            QuerySearchResult querySearchResult = querySearchResults.get(index);
            CompoundTopDocs updatedTopDocs = queryTopDocs.get(index);
            float maxScore = 0.0f;
            if (updatedTopDocs.getTotalHits().value > 0 && !updatedTopDocs.getScoreDocs().isEmpty()) {
                maxScore = updatedTopDocs.getScoreDocs().get(0).score;
            }
            TopDocsAndMaxScore updatedTopDocsAndMaxScore;
            if (sort == null) {
                // create final version of top docs with all updated values
                TopDocs topDocs = new TopDocs(updatedTopDocs.getTotalHits(), updatedTopDocs.getScoreDocs().toArray(new ScoreDoc[0]));
                updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
            } else {
                final FieldDoc[] fieldDocs = new FieldDoc[updatedTopDocs.getScoreDocs().size()];
                int i = 0;
                if (updatedTopDocs.getTotalHits().value > 0) {
                    for (ScoreDoc scoreDoc : updatedTopDocs.getScoreDocs()) {
                        maxScore = Math.max(maxScore, scoreDoc.score);
                        fieldDocs[i++] = (FieldDoc) scoreDoc;
                    }
                } else {
                    maxScore = MAX_SCORE_WHEN_NO_HITS_FOUND;
                }

                TopFieldDocs topFieldDocs = new TopFieldDocs(updatedTopDocs.getTotalHits(), fieldDocs, sort.getSort());
                updatedTopDocsAndMaxScore = new TopDocsAndMaxScore(topFieldDocs, maxScore);
            }
            querySearchResult.topDocs(updatedTopDocsAndMaxScore, querySearchResult.sortValueFormats());
        }
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

    private Sort evaluateSortCriteria(List<CompoundTopDocs> queryTopDocs) {
        for (CompoundTopDocs compoundTopDocs : queryTopDocs) {
            if (compoundTopDocs != null && compoundTopDocs.getTotalHits().value > 0) {
                List<TopDocs> topDocs = compoundTopDocs.getTopDocs();
                TopDocs foundTopDoc = null;
                for (TopDocs topDoc : topDocs) {
                    if (topDoc != null && topDoc.scoreDocs.length > 0) {
                        foundTopDoc = topDoc;
                        break;
                    }
                }

                if (foundTopDoc != null && foundTopDoc.scoreDocs[0] instanceof FieldDoc) {
                    return createSort(topDocs.toArray(new TopFieldDocs[0]));
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    private static Sort createSort(TopFieldDocs[] topFieldDocs) {
        final SortField[] firstTopDocFields = topFieldDocs[0].fields;
        final SortField[] newFields = new SortField[firstTopDocFields.length];

        for (int i = 0; i < firstTopDocFields.length; i++) {
            final SortField delegate = firstTopDocFields[i];
            final SortField.Type type = delegate instanceof SortedNumericSortField
                ? ((SortedNumericSortField) delegate).getNumericType()
                : delegate.getType();

            if (SortedWiderNumericSortField.isTypeSupported(type) && isSortWideningRequired(topFieldDocs, i)) {
                newFields[i] = new SortedWiderNumericSortField(delegate.getField(), type, delegate.getReverse());
            } else {
                newFields[i] = firstTopDocFields[i];
            }
        }
        return new Sort(newFields);
    }

    private static boolean isSortWideningRequired(TopFieldDocs[] topFieldDocs, int sortFieldindex) {
        for (int i = 0; i < topFieldDocs.length - 1; i++) {
            if (!topFieldDocs[i].fields[sortFieldindex].equals(topFieldDocs[i + 1].fields[sortFieldindex])) {
                return true;
            }
        }
        return false;
    }
}
