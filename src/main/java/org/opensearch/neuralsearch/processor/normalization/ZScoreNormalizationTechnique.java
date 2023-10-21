/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.normalization;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.ToString;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import com.google.common.primitives.Floats;

/**
 * Implementation of z-score normalization technique for hybrid query
 * This is currently modeled based on the existing normalization techniques {@link L2ScoreNormalizationTechnique} and {@link MinMaxScoreNormalizationTechnique}
 * However, this class as well as the original ones require a significant work to improve style and ease of use, see TODO items below
 */
/*
TODO: Some todo items that apply here but also on the original normalization techniques on which it is modeled {@link L2ScoreNormalizationTechnique} and {@link MinMaxScoreNormalizationTechnique}
1. Random access to abstract list object is a bad practice both stylistically and from performance perspective and should be removed
2. Identical sub queries and their distribution between shards is currently completely implicit based on ordering and should be explicit based on identifier
3. Implicit calculation of numOfSubQueries instead of having a more explicit upstream indicator/metadata regarding it
 */
@ToString(onlyExplicitlyIncluded = true)
public class ZScoreNormalizationTechnique implements ScoreNormalizationTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = "z_score";
    private static final float SINGLE_RESULT_SCORE = 1.0f;

    @Override
    public void normalize(final List<CompoundTopDocs> queryTopDocs) {
        /*
           TODO: There is an implicit assumption in this calculation that probably need to be made clearer by passing some metadata with the results.
           Currently assuming that finding a single non empty shard result will contain all sub query results with 0 hits.
         */
        final Optional<CompoundTopDocs> maybeCompoundTopDocs = queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> topDocs.getTopDocs().size() > 0)
            .findAny();

        final int numOfSubQueries = maybeCompoundTopDocs.map(compoundTopDocs -> compoundTopDocs.getTopDocs().size()).orElse(0);

        // to be done for each subquery
        float[] sumPerSubquery = findScoreSumPerSubQuery(queryTopDocs, numOfSubQueries);
        long[] elementsPerSubquery = findNumberOfElementsPerSubQuery(queryTopDocs, numOfSubQueries);
        float[] meanPerSubQuery = findMeanPerSubquery(sumPerSubquery, elementsPerSubquery);
        float[] stdPerSubquery = findStdPerSubquery(queryTopDocs, meanPerSubQuery, elementsPerSubquery, numOfSubQueries);

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

    static private float[] findScoreSumPerSubQuery(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        final float[] sumOfScorePerSubQuery = new float[numOfScores];
        Arrays.fill(sumOfScorePerSubQuery, 0);
        // TODO: make this syntactically clearer regarding performance by avoiding List.get(j) with an abstract List type
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                sumOfScorePerSubQuery[j] += sumScoreDocsArray(topDocsPerSubQuery.get(j).scoreDocs);
            }
        }

        return sumOfScorePerSubQuery;
    }

    static private long[] findNumberOfElementsPerSubQuery(final List<CompoundTopDocs> queryTopDocs, final int numOfScores) {
        final long[] numberOfElementsPerSubQuery = new long[numOfScores];
        Arrays.fill(numberOfElementsPerSubQuery, 0);
        // TODO: make this syntactically clearer regarding performance by avoiding List.get(j) with an abstract List type
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                numberOfElementsPerSubQuery[j] += topDocsPerSubQuery.get(j).totalHits.value;
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
        // TODO: make this syntactically clearer regarding performance by avoiding List.get(j) with an abstract List type
        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            if (Objects.isNull(compoundQueryTopDocs)) {
                continue;
            }
            List<TopDocs> topDocsPerSubQuery = compoundQueryTopDocs.getTopDocs();
            for (int j = 0; j < topDocsPerSubQuery.size(); j++) {
                for (ScoreDoc scoreDoc : topDocsPerSubQuery.get(j).scoreDocs) {
                    deltaSumPerSubquery[j] += Math.pow(scoreDoc.score - meanPerSubQuery[j], 2);
                }
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
        // edge case when there is only one score and min and max scores are same
        if (Floats.compare(mean, score) == 0) {
            return SINGLE_RESULT_SCORE;
        }
        return (score - mean) / standardDeviation;
    }
}
