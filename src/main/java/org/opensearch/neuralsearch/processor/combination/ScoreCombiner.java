/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.Comparator;
import java.util.LinkedHashSet;

import java.util.stream.Collectors;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import lombok.extern.log4j.Log4j2;

/**
 * Abstracts combination of scores in query search results.
 */
@Log4j2
public class ScoreCombiner {

    /**
     * Performs score combination based on input combination technique. Mutates input object by updating combined scores
     * Main steps we're doing for combination:
     * - create map of normalized scores per doc id
     * - using normalized scores create another map of combined scores per doc id
     * - count max number of hits among sub-queries
     * - sort documents by scores and take first "max number" of docs
     * - update query search results with normalized scores
     * Different score combination techniques are different in step 2, where we create map of "doc id" - "combined score",
     * other steps are same for all techniques.
     *
     * @param queryTopDocs              query results that need to be normalized, mutated by method execution
     * @param scoreCombinationTechnique exact combination method that should be applied
     * @param sort                      sort criteria
     */
    public void combineScores(
        final List<CompoundTopDocs> queryTopDocs,
        final ScoreCombinationTechnique scoreCombinationTechnique,
        Sort sort
    ) {
        // iterate over results from each shard. Every CompoundTopDocs object has results from
        // multiple sub queries, doc ids may repeat for each sub query results
        queryTopDocs.forEach(compoundQueryTopDocs -> combineShardScores(scoreCombinationTechnique, compoundQueryTopDocs, sort));
    }

    private void combineShardScores(
        final ScoreCombinationTechnique scoreCombinationTechnique,
        final CompoundTopDocs compoundQueryTopDocs,
        final Sort sort
    ) {
        if (Objects.isNull(compoundQueryTopDocs) || compoundQueryTopDocs.getTotalHits().value == 0) {
            return;
        }
        List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();

        // - create map of normalized scores results returned from the single shard
        Map<Integer, float[]> normalizedScoresPerDoc = getNormalizedScoresPerDocument(topDocsPerSubQuery);

        // - create map of combined scores per doc id
        Map<Integer, Float> combinedNormalizedScoresByDocId = combineScoresAndGetCombinedNormalizedScoresPerDocument(
            normalizedScoresPerDoc,
            scoreCombinationTechnique
        );

        // If sorting is enabled then create the map of sort field values per docId
        Map<Integer, Object[]> docIdSortFieldMap = null;
        List<TopFieldDocs> topFieldDocs = null;
        if (sort != null) {
            final boolean isSortByScore = checkIfSortOrderByScore(sort);
            topFieldDocs = new ArrayList<>();
            for (int i = 0; i < topDocsPerSubQuery.size(); i++) {
                if (topDocsPerSubQuery.get(i).scoreDocs.length != 0) {
                    topFieldDocs.add((TopFieldDocs) topDocsPerSubQuery.get(i));
                }
            }
            docIdSortFieldMap = getDocIdSortFieldsMap(compoundQueryTopDocs, isSortByScore, combinedNormalizedScoresByDocId);
        }

        // - sort documents by scores and take first "max number" of docs
        // create a collection of doc ids that are sorted by their combined scores
        List<Integer> sortedDocsIds = getSortedDocIds(combinedNormalizedScoresByDocId, topFieldDocs, sort);

        // - update query search results with normalized scores
        updateQueryTopDocsWithCombinedScores(
            compoundQueryTopDocs,
            topDocsPerSubQuery,
            combinedNormalizedScoresByDocId,
            sortedDocsIds,
            docIdSortFieldMap,
            sort != null ? true : false
        );
    }

    private boolean checkIfSortOrderByScore(Sort sort) {
        if (sort != null) {
            for (SortField sortField : sort.getSort()) {
                if (sortField.getType().equals(SortField.Type.SCORE)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Map<Integer, Object[]> getDocIdSortFieldsMap(
        final CompoundTopDocs compoundTopDocs,
        final boolean isSortByScore,
        Map<Integer, Float> combinedNormalizedScoresByDocId
    ) {
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        Map<Integer, Object[]> docIdSortFieldMap = new HashMap<>();
        List<TopDocs> topFieldDocs = compoundTopDocs.getTopDocs();

        for (TopDocs topDocs : topFieldDocs) {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                FieldDoc fieldDoc = (FieldDoc) scoreDoc;

                if (docIdSortFieldMap.get(fieldDoc.doc) == null) {
                    // If sort by score then replace sort field value with normalized score.
                    if (isSortByScore) {
                        docIdSortFieldMap.put(fieldDoc.doc, new Object[] { combinedNormalizedScoresByDocId.get(fieldDoc.doc) });
                    } else {
                        docIdSortFieldMap.put(fieldDoc.doc, fieldDoc.fields);
                    }
                }
            }
        }
        return docIdSortFieldMap;
    }

    private List<Integer> getSortedDocIds(
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final List<TopFieldDocs> topFieldDocs,
        final Sort sort
    ) {
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        List<Integer> sortedDocsIds;
        if (Objects.isNull(sort)) {
            sortedDocsIds = new ArrayList<>(combinedNormalizedScoresByDocId.keySet());
            sortedDocsIds.sort((a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a)));
        } else {
            if (Objects.isNull(topFieldDocs)) {
                throw new IllegalArgumentException("topFieldDocs cannot be null when sorting is enabled.");
            }
            int topN = 0;
            for (TopFieldDocs topFieldDoc : topFieldDocs) {
                topN += topFieldDoc.scoreDocs.length;
            }

            // Merge the sorted results of individual queries to form a one final result per shard which is sorted.
            final TopDocs sortedTopDocs = TopDocs.merge(sort, 0, topN, topFieldDocs.toArray(new TopFieldDocs[0]), getTieBreaker());

            // Remove duplicates from the sorted top docs.
            Set<Integer> uniqueDocIds = new LinkedHashSet<>();
            for (ScoreDoc scoreDoc : sortedTopDocs.scoreDocs) {
                uniqueDocIds.add(scoreDoc.doc);
            }
            sortedDocsIds = new ArrayList<>(uniqueDocIds);
        }
        return sortedDocsIds;
    }

    private List<ScoreDoc> getCombinedScoreDocs(
        final CompoundTopDocs compoundQueryTopDocs,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final List<Integer> sortedScores,
        final long maxHits,
        Map<Integer, Object[]> docIdSortFieldMap,
        boolean isSortingEnabled
    ) {
        int shardId = -1;
        if (!compoundQueryTopDocs.getScoreDocs().isEmpty()) {
            shardId = compoundQueryTopDocs.getScoreDocs().get(0).shardIndex;
        }
        List<ScoreDoc> scoreDocs = new ArrayList<>();
        if (isSortingEnabled) {
            for (int j = 0; j < maxHits && j < sortedScores.size(); j++) {
                int docId = sortedScores.get(j);
                scoreDocs.add(new FieldDoc(docId, combinedNormalizedScoresByDocId.get(docId), docIdSortFieldMap.get(docId), shardId));
            }
        } else {
            for (int j = 0; j < maxHits && j < sortedScores.size(); j++) {
                int docId = sortedScores.get(j);
                scoreDocs.add(new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId));
            }
        }
        return scoreDocs;
    }

    public Map<Integer, float[]> getNormalizedScoresPerDocument(final List<TopDocs> topDocsPerSubQuery) {
        Map<Integer, float[]> normalizedScoresPerDoc = new HashMap<>();
        for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
            TopDocs topDocs = topDocsPerSubQuery.get(j);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                normalizedScoresPerDoc.computeIfAbsent(scoreDoc.doc, key -> {
                    float[] scores = new float[topDocsPerSubQuery.size()];
                    // we initialize with -1.0, as after normalization it's possible that score is 0.0
                    return scores;
                });
                normalizedScoresPerDoc.get(scoreDoc.doc)[j] = scoreDoc.score;
            }
        }
        return normalizedScoresPerDoc;
    }

    private Map<Integer, Float> combineScoresAndGetCombinedNormalizedScoresPerDocument(
        final Map<Integer, float[]> normalizedScoresPerDocument,
        final ScoreCombinationTechnique scoreCombinationTechnique
    ) {
        return normalizedScoresPerDocument.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
    }

    private void updateQueryTopDocsWithCombinedScores(
        final CompoundTopDocs compoundQueryTopDocs,
        final List<TopDocs> topDocsPerSubQuery,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final List<Integer> sortedScores,
        Map<Integer, Object[]> docIdSortFieldMap,
        boolean isSortingEnabled
    ) {
        // - max number of hits will be the same which are passed from QueryPhase
        long maxHits = compoundQueryTopDocs.getTotalHits().value;
        // - update query search results with normalized scores
        compoundQueryTopDocs.setScoreDocs(
            getCombinedScoreDocs(
                compoundQueryTopDocs,
                combinedNormalizedScoresByDocId,
                sortedScores,
                maxHits,
                docIdSortFieldMap,
                isSortingEnabled
            )
        );
        compoundQueryTopDocs.setTotalHits(getTotalHits(topDocsPerSubQuery, maxHits));
    }

    private TotalHits getTotalHits(final List<TopDocs> topDocsPerSubQuery, final long maxHits) {
        TotalHits.Relation totalHits = TotalHits.Relation.EQUAL_TO;
        if (topDocsPerSubQuery.stream().anyMatch(topDocs -> topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)) {
            totalHits = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        return new TotalHits(maxHits, totalHits);
    }

    // Tie-breaker to merge multiple top docs
    private Comparator<ScoreDoc> getTieBreaker() {
        final Comparator<ScoreDoc> SORTING_TIE_BREAKER = (o1, o2) -> {
            int scoreComparison = Double.compare(o1.score, o2.score);
            if (scoreComparison != 0) {
                return scoreComparison;
            }

            int docIdComparison = Integer.compare(o1.doc, o2.doc);
            if (docIdComparison != 0) {
                return docIdComparison;
            }

            // When duplicate result found then both score and doc ID are equal, return 1
            return (o1.score == o2.score && o1.doc == o2.doc) ? 1 : 0;
        };
        return SORTING_TIE_BREAKER;
    }
}
