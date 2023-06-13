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
     * @param queryTopDocs query results that need to be normalized, mutated by method execution
     * @param combinationTechnique exact combination method that should be applied
     * @return list of max combined scores for each shard
     */
    public List<Float> combineScores(final CompoundTopDocs[] queryTopDocs, final ScoreCombinationTechnique combinationTechnique) {
        List<Float> maxScores = new ArrayList<>();
        for (int i = 0; i < queryTopDocs.length; i++) {
            CompoundTopDocs compoundQueryTopDocs = queryTopDocs[i];
            if (Objects.isNull(compoundQueryTopDocs) || compoundQueryTopDocs.totalHits.value == 0) {
                maxScores.add(ZERO_SCORE);
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getCompoundTopDocs();
            int shardId = compoundQueryTopDocs.scoreDocs[0].shardIndex;
            Map<Integer, float[]> normalizedScoresPerDoc = new HashMap<>();
            int maxHits = 0;
            TotalHits.Relation totalHits = TotalHits.Relation.EQUAL_TO;
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs topDocs = topDocsPerSubQuery.get(j);
                int hits = 0;
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    if (!normalizedScoresPerDoc.containsKey(scoreDoc.doc)) {
                        float[] scores = new float[topDocsPerSubQuery.size()];
                        // we initialize with -1.0, as after normalization it's possible that score is 0.0
                        Arrays.fill(scores, -1.0f);
                        normalizedScoresPerDoc.put(scoreDoc.doc, scores);
                    }
                    normalizedScoresPerDoc.get(scoreDoc.doc)[j] = scoreDoc.score;
                    hits++;
                }
                maxHits = Math.max(maxHits, hits);
            }
            if (topDocsPerSubQuery.stream()
                .anyMatch(topDocs -> topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)) {
                totalHits = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
            Map<Integer, Float> combinedNormalizedScoresByDocId = normalizedScoresPerDoc.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> combinationTechnique.combine(entry.getValue())));
            // create priority queue, make it max heap by the score
            PriorityQueue<Integer> pq = new PriorityQueue<>(
                (a, b) -> Float.compare(combinedNormalizedScoresByDocId.get(b), combinedNormalizedScoresByDocId.get(a))
            );
            // we're merging docs with normalized and combined scores. we need to have only maxHits results
            for (int docId : normalizedScoresPerDoc.keySet()) {
                pq.add(docId);
            }

            ScoreDoc[] finalScoreDocs = new ScoreDoc[maxHits];
            float maxScore = combinedNormalizedScoresByDocId.get(pq.peek());

            for (int j = 0; j < maxHits; j++) {
                int docId = pq.poll();
                finalScoreDocs[j] = new ScoreDoc(docId, combinedNormalizedScoresByDocId.get(docId), shardId);
            }
            compoundQueryTopDocs.scoreDocs = finalScoreDocs;
            compoundQueryTopDocs.totalHits = new TotalHits(maxHits, totalHits);
            log.info(String.format(Locale.ROOT, "update top docs maxScore, updated value %f", maxScore));
            maxScores.add(maxScore);
        }
        return maxScores;
    }
}
