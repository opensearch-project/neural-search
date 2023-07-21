/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.search.CompoundTopDocs;

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

    private float combineShardScores(ScoreCombinationTechnique scoreCombinationTechnique, CompoundTopDocs compoundQueryTopDocs) {
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
        // create a priority queue of doc ids that are sorted by their combined scores
        PriorityQueue<Integer> scoreQueue = getPriorityQueueOfDocIds(normalizedScoresPerDoc, combinedNormalizedScoresByDocId);
        // store max score to resulting list, call it now as priority queue will change after combining scores
        float maxScore = combinedNormalizedScoresByDocId.get(scoreQueue.peek());

        // - update query search results with normalized scores
        updateQueryTopDocsWithCombinedScores(compoundQueryTopDocs, topDocsPerSubQuery, combinedNormalizedScoresByDocId, scoreQueue);
        return maxScore;
    }

    private PriorityQueue<Integer> getPriorityQueueOfDocIds(
        Map<Integer, float[]> normalizedScoresPerDoc,
        Map<Integer, Float> combinedNormalizedScoresByDocId
    ) {
        PriorityQueue<Integer> pq = new PriorityQueue<>(
            (a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a))
        );
        // we're merging docs with normalized and combined scores. we need to have only maxHits results
        pq.addAll(normalizedScoresPerDoc.keySet());
        return pq;
    }

    private ScoreDoc[] getCombinedScoreDocs(
        final CompoundTopDocs compoundQueryTopDocs,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final PriorityQueue<Integer> scoreQueue,
        final int maxHits
    ) {
        ScoreDoc[] finalScoreDocs = new ScoreDoc[maxHits];

        int shardId = compoundQueryTopDocs.scoreDocs[0].shardIndex;
        for (int j = 0; j < maxHits; j++) {
            int docId = scoreQueue.poll();
            finalScoreDocs[j] = new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId);
        }
        return finalScoreDocs;
    }

    private Map<Integer, float[]> getNormalizedScoresPerDocument(List<TopDocs> topDocsPerSubQuery) {
        Map<Integer, float[]> normalizedScoresPerDoc = new HashMap<>();
        for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
            TopDocs topDocs = topDocsPerSubQuery.get(j);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                normalizedScoresPerDoc.putIfAbsent(scoreDoc.doc, normalizedScoresPerDoc.computeIfAbsent(scoreDoc.doc, key -> {
                    float[] scores = new float[topDocsPerSubQuery.size()];
                    // we initialize with -1.0, as after normalization it's possible that score is 0.0
                    Arrays.fill(scores, -1.0f);
                    return scores;
                }));
                normalizedScoresPerDoc.get(scoreDoc.doc)[j] = scoreDoc.score;
            }
        }
        return normalizedScoresPerDoc;
    }

    private Map<Integer, Float> combineScoresAndGetCombinedNormilizedScoresPerDocument(
        Map<Integer, float[]> normalizedScoresPerDocument,
        final ScoreCombinationTechnique scoreCombinationTechnique
    ) {
        return normalizedScoresPerDocument.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> scoreCombinationTechnique.combine(entry.getValue())));
    }

    private void updateQueryTopDocsWithCombinedScores(
        CompoundTopDocs compoundQueryTopDocs,
        List<TopDocs> topDocsPerSubQuery,
        Map<Integer, Float> combinedNormalizedScoresByDocId,
        PriorityQueue<Integer> scoreQueue
    ) {
        // - count max number of hits among sub-queries
        int maxHits = getMaxHits(topDocsPerSubQuery);
        // - update query search results with normalized scores
        compoundQueryTopDocs.scoreDocs = getCombinedScoreDocs(compoundQueryTopDocs, combinedNormalizedScoresByDocId, scoreQueue, maxHits);
        compoundQueryTopDocs.totalHits = getTotalHits(topDocsPerSubQuery, maxHits);
    }

    private int getMaxHits(List<TopDocs> topDocsPerSubQuery) {
        int maxHits = 0;
        for (TopDocs topDocs : topDocsPerSubQuery) {
            int hits = topDocs.scoreDocs.length;
            maxHits = Math.max(maxHits, hits);
        }
        return maxHits;
    }

    private TotalHits getTotalHits(List<TopDocs> topDocsPerSubQuery, int maxHits) {
        TotalHits.Relation totalHits = TotalHits.Relation.EQUAL_TO;
        if (topDocsPerSubQuery.stream().anyMatch(topDocs -> topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)) {
            totalHits = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }
        return new TotalHits(maxHits, totalHits);
    }
}
