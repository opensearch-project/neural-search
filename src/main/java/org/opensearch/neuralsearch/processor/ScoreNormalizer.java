/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
     * Performs score normalization based on input combination technique. Mutates input object by updating normalized scores.
     * @param queryTopDocs original query results from multiple shards and multiple sub-queries
     * @param normalizationTechnique exact normalization method that should be applied
     */
    public void normalizeScores(final CompoundTopDocs[] queryTopDocs, final ScoreNormalizationTechnique normalizationTechnique) {
        Optional<CompoundTopDocs> maybeCompoundQuery = Arrays.stream(queryTopDocs)
            .filter(topDocs -> Objects.nonNull(topDocs) && !topDocs.getCompoundTopDocs().isEmpty())
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
                    ScoreNormalizationTechnique.ScoreNormalizationRequest normalizationRequest =
                        ScoreNormalizationTechnique.ScoreNormalizationRequest.builder()
                            .score(scoreDoc.score)
                            .minScore(minMaxScores[j][0])
                            .maxScore(minMaxScores[j][1])
                            .build();
                    float originalScore = scoreDoc.score;
                    scoreDoc.score = normalizationTechnique.normalize(normalizationRequest);
                    log.info(
                        String.format(
                            Locale.ROOT,
                            "update doc [%d] score, original value: %f, updated value %f",
                            scoreDoc.doc,
                            originalScore,
                            scoreDoc.score
                        )
                    );
                }
            }
        }
    }
}
