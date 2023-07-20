/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.search.CompoundTopDocs;

/**
 * Abstracts normalization of scores in query search results.
 */
@Log4j2
public class ScoreNormalizer {

    /**
     * Performs score normalization based on input normalization technique. Mutates input object by updating normalized scores.
     * @param queryTopDocs original query results from multiple shards and multiple sub-queries
     * @param normalizationTechnique exact normalization method that should be applied
     */
    public static void normalizeScores(final List<CompoundTopDocs> queryTopDocs, final ScoreNormalizationTechnique normalizationTechnique) {
        Optional<CompoundTopDocs> maybeCompoundQuery = queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> topDocs.getCompoundTopDocs().size() > 0)
            .findAny();
        if (maybeCompoundQuery.isEmpty()) {
            return;
        }

        // init scores per sub-query
        float[][] minMaxScores = new float[maybeCompoundQuery.get().getCompoundTopDocs().size()][];
        for (int i = 0; i < minMaxScores.length; i++) {
            minMaxScores[i] = new float[] { Float.MAX_VALUE, Float.MIN_VALUE };
        }

        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (compoundQueryTopDocs == null) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getCompoundTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                // get min and max scores
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    minMaxScores[j][0] = Math.min(minMaxScores[j][0], scoreDoc.score);
                    minMaxScores[j][1] = Math.max(minMaxScores[j][1], scoreDoc.score);
                }
            }
        }
        // do the normalization
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (compoundQueryTopDocs == null) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getCompoundTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    ScoreNormalizationRequest normalizationRequest = ScoreNormalizationRequest.builder()
                        .score(scoreDoc.score)
                        .minScore(minMaxScores[j][0])
                        .maxScore(minMaxScores[j][1])
                        .build();
                    scoreDoc.score = normalizationTechnique.normalize(normalizationRequest);
                }
            }
        }
    }
}
