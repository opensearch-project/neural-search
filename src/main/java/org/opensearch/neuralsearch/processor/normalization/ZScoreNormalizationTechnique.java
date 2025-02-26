/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.NormalizeScoresDTO;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import com.google.common.primitives.Floats;

import java.util.List;
import java.util.Objects;

public class ZScoreNormalizationTechnique implements ScoreNormalizationTechnique {

    public static final String TECHNIQUE_NAME = "z_score";
    private static final float SINGLE_RESULT_SCORE = 1.0f;

    @Override
    public void normalize(NormalizeScoresDTO normalizeScoresDTO) {
        List<CompoundTopDocs> queryTopDocs = normalizeScoresDTO.getQueryTopDocs();
        int numOfSubqueries = getNumOfSubqueries(queryTopDocs);

        // to be done for each subquery
        float[] sumPerSubquery = findScoreSumPerSubQuery(queryTopDocs, numOfSubqueries);
        long[] elementsPerSubquery = findNumberOfElementsPerSubQuery(queryTopDocs, numOfSubqueries);
        float[] meanPerSubQuery = findMeanPerSubquery(sumPerSubquery, elementsPerSubquery);
        float[] stdPerSubquery = findStdPerSubquery(queryTopDocs, meanPerSubQuery, elementsPerSubquery, numOfSubqueries);

        // do normalization using actual score and z-scores for corresponding sub query
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                TopDocs subQueryTopDoc = topDocsPerSubQuery.get(j);
                for (ScoreDoc scoreDoc : subQueryTopDoc.scoreDocs) {
                    scoreDoc.score = normalizeSingleScore(scoreDoc.score, stdPerSubquery[j], meanPerSubQuery[j]);
                }
            }
        }
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

    static private float[] findScoreSumPerSubQuery(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        final float[] sumOfScorePerSubQuery = new float[numOfScores];
        Arrays.fill(sumOfScorePerSubQuery, 0);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int subQueryIndex = 0;
            for (TopDocs topDocs : topDocsPerSubQuery) {
                sumOfScorePerSubQuery[subQueryIndex++] += sumScoreDocsArray(topDocs.scoreDocs);
            }
        }

        return sumOfScorePerSubQuery;
    }

    static private long[] findNumberOfElementsPerSubQuery(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        final long[] numberOfElementsPerSubQuery = new long[numOfScores];
        Arrays.fill(numberOfElementsPerSubQuery, 0);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int subQueryIndex = 0;
            for (TopDocs topDocs : topDocsPerSubQuery) {
                numberOfElementsPerSubQuery[subQueryIndex++] += topDocs.totalHits.value();
            }
        }

        return numberOfElementsPerSubQuery;
    }

    static private float[] findMeanPerSubquery(final float[] sumPerSubquery, final long[] elementsPerSubquery) {
        final float[] meanPerSubQuery = new float[elementsPerSubquery.length];
        for (int i = 0; i < elementsPerSubquery.length; i++) {
            if (elementsPerSubquery[i] == 0) {
                meanPerSubQuery[i] = 0;
            } else {
                meanPerSubQuery[i] = sumPerSubquery[i] / elementsPerSubquery[i];
            }
        }

        return meanPerSubQuery;
    }

    static private float[] findStdPerSubquery(
        final List<CompoundTopDocs> queryTopDocs,
        final float[] meanPerSubQuery,
        final long[] elementsPerSubquery,
        final int numOfScores
    ) {
        final double[] deltaSumPerSubquery = new double[numOfScores];
        Arrays.fill(deltaSumPerSubquery, 0);
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            int subQueryIndex = 0;
            for (TopDocs topDocs : topDocsPerSubQuery) {
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    deltaSumPerSubquery[subQueryIndex] += Math.pow(scoreDoc.score - meanPerSubQuery[subQueryIndex], 2);
                }
                subQueryIndex++;
            }
        }

        final float[] stdPerSubQuery = new float[numOfScores];
        for (int i = 0; i < deltaSumPerSubquery.length; i++) {
            if (elementsPerSubquery[i] == 0) {
                stdPerSubQuery[i] = 0;
            } else {
                stdPerSubQuery[i] = (float) Math.sqrt(deltaSumPerSubquery[i] / elementsPerSubquery[i]);
            }
        }

        return stdPerSubQuery;
    }

    static private float sumScoreDocsArray(final ScoreDoc[] scoreDocs) {
        float sum = 0;
        for (ScoreDoc scoreDoc : scoreDocs) {
            sum += scoreDoc.score;
        }

        return sum;
    }

    private static float normalizeSingleScore(final float score, final float standardDeviation, final float mean) {
        // edge case when there is only one score and z scores are same
        if (Floats.compare(mean, score) == 0) {
            return SINGLE_RESULT_SCORE;
        }
        // if sd == 0: return 0
        return (score - mean) / standardDeviation;
        // if nscore < 0: return 0
    }

}
