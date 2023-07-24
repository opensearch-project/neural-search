/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.search.CompoundTopDocs;

import com.google.common.annotations.VisibleForTesting;

/**
 * Abstracts combination of scores in query search results.
 */
@Log4j2
public class ScoreCombiner {

    private static final Float ZERO_SCORE = 0.0f;

    /**
     * Performs score combination based on input combination technique. Mutates input object by updating combined scores
     * Main steps we're doing for combination:
     *  - create map of normalized scores per doc id
     *  - using normalized scores create another map of combined scores per doc id
     *  - count max number of hits among sub-queries
     *  - sort documents by scores and take first "max number" of docs
     *  - update query search results with normalized scores
     *  Different score combination techniques are different in step 2, where we create map of "doc id" - "combined score",
     *  other steps are same for all techniques.
     * @param queryTopDocs query results that need to be normalized, mutated by method execution
     * @param scoreCombinationTechnique exact combination technique that should be applied
     * @return list of max combined scores for each shard
     */
    public List<Float> combineScores(final List<CompoundTopDocs> queryTopDocs, final ScoreCombinationTechnique scoreCombinationTechnique) {
        // iterate over results from each shard. Every CompoundTopDocs object has results from
        // multiple sub queries, doc ids may repeat for each sub query results
        return queryTopDocs.stream()
            .map(compoundQueryTopDocs -> combineShardScores(scoreCombinationTechnique, compoundQueryTopDocs))
            .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected float combineShardScores(
        final ScoreCombinationTechnique scoreCombinationTechnique,
        final CompoundTopDocs compoundQueryTopDocs
    ) {
        if (Objects.isNull(compoundQueryTopDocs) || compoundQueryTopDocs.totalHits.value == 0) {
            return ZERO_SCORE;
        }
        List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getCompoundTopDocs();
        // - create map of normalized scores results returned from the single shard
        Map<Integer, float[]> normalizedScoresPerDoc = getNormalizedScoresPerDocument(topDocsPerSubQuery);

        // - create map of combined scores per doc id
        Map<Integer, Float> combinedNormalizedScoresByDocId = combineScoresAndGetCombinedNormilizedScoresPerDocument(
            normalizedScoresPerDoc,
            scoreCombinationTechnique
        );

        // - sort documents by scores and take first "max number" of docs
        // create a collection of doc ids that are sorted by their combined scores
        List<Integer> sortedDocsIds = getPriorityQueueOfDocIds(combinedNormalizedScoresByDocId);

        // - update query search results with normalized scores
        updateQueryTopDocsWithCombinedScores(compoundQueryTopDocs, topDocsPerSubQuery, combinedNormalizedScoresByDocId, sortedDocsIds);

        // return max score
        if (sortedDocsIds.isEmpty()) {
            return ZERO_SCORE;
        }
        return combinedNormalizedScoresByDocId.get(sortedDocsIds.get(0));
    }

    @VisibleForTesting
    protected List<Integer> getPriorityQueueOfDocIds(final Map<Integer, Float> combinedNormalizedScoresByDocId) {
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        List<Integer> sortedDocsIds = new ArrayList<>(combinedNormalizedScoresByDocId.keySet());
        sortedDocsIds.sort((a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a)));
        return sortedDocsIds;
    }

    @VisibleForTesting
    protected ScoreDoc[] getCombinedScoreDocs(
        final CompoundTopDocs compoundQueryTopDocs,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final List<Integer> sortedScores,
        final int maxHits
    ) {
        ScoreDoc[] finalScoreDocs = new ScoreDoc[maxHits];

        int shardId = compoundQueryTopDocs.scoreDocs[0].shardIndex;
        for (int j = 0; j < maxHits && j < sortedScores.size(); j++) {
            int docId = sortedScores.get(j);
            finalScoreDocs[j] = new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId);
        }
        return finalScoreDocs;
    }

    @VisibleForTesting
    public Map<Integer, float[]> getNormalizedScoresPerDocument(final List<TopDocs> topDocsPerSubQuery) {
        Map<Integer, float[]> normalizedScoresPerDoc = new HashMap<>();
        for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
            TopDocs topDocs = topDocsPerSubQuery.get(j);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                normalizedScoresPerDoc.computeIfAbsent(scoreDoc.doc, key -> {
                    float[] scores = new float[topDocsPerSubQuery.size()];
                    // we initialize with -1.0, as after normalization it's possible that score is 0.0
                    Arrays.fill(scores, -1.0f);
                    return scores;
                });
                normalizedScoresPerDoc.get(scoreDoc.doc)[j] = scoreDoc.score;
            }
        }
        return normalizedScoresPerDoc;
    }

    @VisibleForTesting
    protected Map<Integer, Float> combineScoresAndGetCombinedNormilizedScoresPerDocument(
        final Map<Integer, float[]> normalizedScoresPerDocument,
        final ScoreCombinationTechnique scoreCombinationTechnique
    ) {
        return normalizedScoresPerDocument.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
    }

    @VisibleForTesting
    protected void updateQueryTopDocsWithCombinedScores(
        final CompoundTopDocs compoundQueryTopDocs,
        final List<TopDocs> topDocsPerSubQuery,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final List<Integer> sortedScores
    ) {
        // - count max number of hits among sub-queries
        int maxHits = getMaxHits(topDocsPerSubQuery);
        // - update query search results with normalized scores
        compoundQueryTopDocs.scoreDocs = getCombinedScoreDocs(compoundQueryTopDocs, combinedNormalizedScoresByDocId, sortedScores, maxHits);
        compoundQueryTopDocs.totalHits = getTotalHits(topDocsPerSubQuery, maxHits);
    }

    @VisibleForTesting
    protected int getMaxHits(final List<TopDocs> topDocsPerSubQuery) {
        int maxHits = 0;
        for (TopDocs topDocs : topDocsPerSubQuery) {
            int hits = topDocs.scoreDocs.length;
            maxHits = Math.max(maxHits, hits);
        }
        return maxHits;
    }

    @VisibleForTesting
    protected TotalHits getTotalHits(final List<TopDocs> topDocsPerSubQuery, int maxHits) {
        TotalHits.Relation totalHits = TotalHits.Relation.EQUAL_TO;
        if (topDocsPerSubQuery.stream().anyMatch(topDocs -> topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)) {
            totalHits = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        return new TotalHits(maxHits, totalHits);
    }
}
