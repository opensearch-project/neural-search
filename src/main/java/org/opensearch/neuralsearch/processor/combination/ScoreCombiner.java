/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
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
import org.apache.lucene.search.SortedNumericSortField;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import lombok.extern.log4j.Log4j2;
import org.opensearch.search.sort.SortedWiderNumericSortField;

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
     */
    public void combineScores(final List<CompoundTopDocs> queryTopDocs, final ScoreCombinationTechnique scoreCombinationTechnique) {
        // iterate over results from each shard. Every CompoundTopDocs object has results from
        // multiple sub queries, doc ids may repeat for each sub query results
        queryTopDocs.forEach(compoundQueryTopDocs -> combineShardScores(scoreCombinationTechnique, compoundQueryTopDocs));
    }

    private void combineShardScores(final ScoreCombinationTechnique scoreCombinationTechnique, final CompoundTopDocs compoundQueryTopDocs) {
        if (Objects.isNull(compoundQueryTopDocs) || compoundQueryTopDocs.getTotalHits().value == 0) {
            return;
        }
        List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
        boolean isSortingEnabled = false;
        TopDocs topDoc = topDocsPerSubQuery.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> topDocs.scoreDocs.length > 0)
            .findFirst()
            .orElse(null);
        if (topDoc.scoreDocs[0] instanceof FieldDoc) {
            isSortingEnabled = true;
        }
        // - create map of normalized scores results returned from the single shard
        Map<Integer, float[]> normalizedScoresPerDoc = getNormalizedScoresPerDocument(topDocsPerSubQuery);

        // - create map of combined scores per doc id
        Map<Integer, Float> combinedNormalizedScoresByDocId = combineScoresAndGetCombinedNormalizedScoresPerDocument(
            normalizedScoresPerDoc,
            scoreCombinationTechnique
        );

        Map<Integer, Object[]> docIdSortFieldMap = null;
        if (isSortingEnabled) {
            docIdSortFieldMap = getDocIdFieldMap(compoundQueryTopDocs);
        }

        // - sort documents by scores and take first "max number" of docs
        // create a collection of doc ids that are sorted by their combined scores
        List<Integer> sortedDocsIds = getSortedDocIds(combinedNormalizedScoresByDocId, isSortingEnabled, topDocsPerSubQuery);

        // - update query search results with normalized scores
        updateQueryTopDocsWithCombinedScores(
            compoundQueryTopDocs,
            topDocsPerSubQuery,
            combinedNormalizedScoresByDocId,
            sortedDocsIds,
            docIdSortFieldMap,
            isSortingEnabled
        );
    }

    private Map<Integer, Object[]> getDocIdFieldMap(final CompoundTopDocs compoundTopDocs) {
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        Map<Integer, Object[]> docIdSortFieldMap = new HashMap<>();
        List<TopDocs> topFieldDocs = compoundTopDocs.getTopDocs();

        for (TopDocs topDocs : topFieldDocs) {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                FieldDoc fieldDoc = (FieldDoc) scoreDoc;

                if (docIdSortFieldMap.get(fieldDoc.doc) == null) {
                    docIdSortFieldMap.put(fieldDoc.doc, fieldDoc.fields);
                }
            }
        }
        return docIdSortFieldMap;
    }

    private List<Integer> getSortedDocIds(
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        boolean isSortingEnabled,
        List<TopDocs> topDocsPerSubQuery
    ) {
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        List<Integer> sortedDocsIds;
        if (!isSortingEnabled) {
            sortedDocsIds = new ArrayList<>(combinedNormalizedScoresByDocId.keySet());
            sortedDocsIds.sort((a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a)));
        } else {

            final List<TopFieldDocs> topFieldDocs = topDocsPerSubQuery.stream()
                .filter(topDocs -> topDocs.scoreDocs.length != 0)
                .map(topDocs -> (TopFieldDocs) topDocs)
                .collect(Collectors.toList());

            int topN = topFieldDocs.stream().mapToInt(topDocs -> topDocs.scoreDocs.length).sum();

            final Sort sort = createSort(topFieldDocs.toArray(new TopFieldDocs[0]));

            final Comparator<ScoreDoc> Sorting_TIE_BREAKER = (o1, o2) -> {
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

            final TopDocs sortedTopDocs = TopDocs.merge(sort, 0, topN, topFieldDocs.toArray(new TopFieldDocs[0]), Sorting_TIE_BREAKER);
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
        int shardId = compoundQueryTopDocs.getScoreDocs().get(0).shardIndex;

        if (isSortingEnabled) {
            return sortedScores.stream()
                .limit(maxHits)
                .map(docId -> new FieldDoc(docId, combinedNormalizedScoresByDocId.get(docId), docIdSortFieldMap.get(docId), shardId))
                .collect(Collectors.toList());
        } else {
            return sortedScores.stream()
                .limit(maxHits)
                .map(docId -> new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId))
                .collect(Collectors.toList());
        }

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
        // When `search_after` is applied then totalHits are greater than maxHits
        // long totalHitsWhenPagingApplied = 0;
        // if (isSortingEnabled) {
        // totalHitsWhenPagingApplied = compoundQueryTopDocs.getTotalHits().value;
        // }

        // - count max number of hits among sub-queries
        // If `search_after is applied then count of score docs will be lower than totalHits found in Query Phase
        // int maxHits = getMaxHits(topDocsPerSubQuery);
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
        // if (isPagingApplied) {
        // compoundQueryTopDocs.setTotalHits(getTotalHits(topDocsPerSubQuery, totalHitsWhenPagingApplied));
        // } else {
        compoundQueryTopDocs.setTotalHits(getTotalHits(topDocsPerSubQuery, maxHits));
        // }
    }

    /**
     * Get max hits as number of unique doc ids from results of all sub-queries
     * @param topDocsPerSubQuery list of topDocs objects for one shard
     * @return number of unique doc ids
     */
    protected int getMaxHits(final List<TopDocs> topDocsPerSubQuery) {
        Set<Integer> docIds = topDocsPerSubQuery.stream()
            .filter(topDocs -> Objects.nonNull(topDocs.scoreDocs))
            .flatMap(topDocs -> Arrays.stream(topDocs.scoreDocs))
            .map(scoreDoc -> scoreDoc.doc)
            .collect(Collectors.toSet());
        return docIds.size();
    }

    private TotalHits getTotalHits(final List<TopDocs> topDocsPerSubQuery, long maxHits) {
        TotalHits.Relation totalHits = TotalHits.Relation.EQUAL_TO;
        if (topDocsPerSubQuery.stream().anyMatch(topDocs -> topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)) {
            totalHits = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        return new TotalHits(maxHits, totalHits);
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

    private static void setShardIndex(TopDocs topDocs, int shardIndex) {
        assert topDocs.scoreDocs.length == 0 || topDocs.scoreDocs[0].shardIndex == -1 : "shardIndex is already set";
        for (ScoreDoc doc : topDocs.scoreDocs) {
            doc.shardIndex = shardIndex;
        }
    }
}
