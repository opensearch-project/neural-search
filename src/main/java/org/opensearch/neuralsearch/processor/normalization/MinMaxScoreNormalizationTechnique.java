/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import com.google.common.primitives.Floats;

import lombok.ToString;
import org.opensearch.neuralsearch.processor.explain.DocIdAtSearchShard;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.getDocIdAtQueryForNormalization;

/**
 * Abstracts normalization of scores based on min-max method
 */
@ToString(onlyExplicitlyIncluded = true)
public class MinMaxScoreNormalizationTechnique implements ScoreNormalizationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "min_max";
    private static final float MIN_SCORE = 0.001f;
    private static final float SINGLE_RESULT_SCORE = 1.0f;

    /**
     * Min-max normalization method.
     * nscore = (score - min_score)/(max_score - min_score)
     * Main algorithm steps:
     * - calculate min and max scores for each sub query
     * - iterate over each result and update score as per formula above where "score" is raw score returned by Hybrid query
     */
    @Override
    public void normalize(final List<CompoundTopDocs> queryTopDocs) {
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);
        // do normalization using actual score and min and max scores for corresponding sub query
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    scoreDoc.score = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.minScoresPerSubquery()[j],
                        minMaxScores.maxScoresPerSubquery()[j]
                    );
                }
            }
        }
    }

    private MinMaxScores getMinMaxScoresResult(final List<CompoundTopDocs> queryTopDocs) {
        int numOfSubqueries = getNumOfSubqueries(queryTopDocs);
        // get min scores for each sub query
        float[] minScoresPerSubquery = getMinScores(queryTopDocs, numOfSubqueries);
        // get max scores for each sub query
        float[] maxScoresPerSubquery = getMaxScores(queryTopDocs, numOfSubqueries);
        return new MinMaxScores(minScoresPerSubquery, maxScoresPerSubquery);
    }

    @Override
    public String describe() {
        return String.format(Locale.ROOT, "%s", TECHNIQUE_NAME);
    }

    @Override
    public Map<DocIdAtSearchShard, ExplanationDetails> explain(final List<CompoundTopDocs> queryTopDocs) {
        MinMaxScores minMaxScores = getMinMaxScoresResult(queryTopDocs);

        Map<DocIdAtSearchShard, List<Float>> normalizedScores = new HashMap<>();
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    DocIdAtSearchShard docIdAtSearchShard = new DocIdAtSearchShard(scoreDoc.doc, compoundQueryTopDocs.getSearchShard());
                    float normalizedScore = normalizeSingleScore(
                        scoreDoc.score,
                        minMaxScores.minScoresPerSubquery()[j],
                        minMaxScores.maxScoresPerSubquery()[j]
                    );
                    normalizedScores.computeIfAbsent(docIdAtSearchShard, k -> new ArrayList<>()).add(normalizedScore);
                    scoreDoc.score = normalizedScore;
                }
            }
        }
        return getDocIdAtQueryForNormalization(normalizedScores, this);
    }

    private int getNumOfSubqueries(final List<CompoundTopDocs> queryTopDocs) {
        return queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> !topDocs.getTopDocs().isEmpty())
            .findAny()
            .get()
            .getTopDocs()
            .size();
    }

    private float[] getMaxScores(final List<CompoundTopDocs> queryTopDocs, final int numOfSubqueries) {
        float[] maxScores = new float[numOfSubqueries];
        Arrays.fill(maxScores, Float.MIN_VALUE);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                maxScores[j] = Math.max(
                    maxScores[j],
                    Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                        .map(scoreDoc -> scoreDoc.score)
                        .max(Float::compare)
                        .orElse(Float.MIN_VALUE)
                );
            }
        }
        return maxScores;
    }

    private float[] getMinScores(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        float[] minScores = new float[numOfScores];
        Arrays.fill(minScores, Float.MAX_VALUE);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                minScores[j] = Math.min(
                    minScores[j],
                    Arrays.stream(topDocsPerSubQuery.get(j).scoreDocs)
                        .map(scoreDoc -> scoreDoc.score)
                        .min(Float::compare)
                        .orElse(Float.MAX_VALUE)
                );
            }
        }
        return minScores;
    }

    private float normalizeSingleScore(final float score, final float minScore, final float maxScore) {
        // edge case when there is only one score and min and max scores are same
        if (Floats.compare(maxScore, minScore) == 0 && Floats.compare(maxScore, score) == 0) {
            return SINGLE_RESULT_SCORE;
        }
        float normalizedScore = (score - minScore) / (maxScore - minScore);
        return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore;
    }

    /**
     * Result class to hold min and max scores for each sub query
     */
    private record MinMaxScores(float[] minScoresPerSubquery, float[] maxScoresPerSubquery) {
    }
}
