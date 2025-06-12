/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.normalization;

import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@ToString(onlyExplicitlyIncluded = true)
@Log4j2
public class MaxNormalizationTechnique implements ScoreNormalizationTechnique {

    @ToString.Include
    public static final String TECHNIQUE_NAME = "max";

    @Override
    public void normalize(List<CompoundTopDocs> queryTopDocs) {
        int numOfSubqueries = queryTopDocs.stream()
            .filter(Objects::nonNull)
            .filter(topDocs -> topDocs.getTopDocs().size() > 0)
            .findAny()
            .get()
            .getTopDocs()
            .size();
        if (numOfSubqueries < 2 || numOfSubqueries > 2) {
            throw new IllegalArgumentException("Number of subqueries cannot be grater or lessor than 2");
        }

        float[] maxScoresPerSubquery = getMaxScores(queryTopDocs, numOfSubqueries);

        float knnQueryMaxScore = maxScoresPerSubquery[0];
        float matchQueryMaxScore = maxScoresPerSubquery[1];

        float multiplier;

        if ((matchQueryMaxScore == 0.0f || matchQueryMaxScore == Float.MIN_VALUE)
            || knnQueryMaxScore == 0.0f
            || knnQueryMaxScore == Float.MIN_VALUE) {
            multiplier = 1;
        } else {
            multiplier = Math.round((matchQueryMaxScore / knnQueryMaxScore) * 1000f) / 1000f;
        }

        for (CompoundTopDocs compoundQueryTopDocs : queryTopDocs) {
            TopDocs topDocsOfKnnSubquery = compoundQueryTopDocs.getTopDocs().get(0);

            for (ScoreDoc scoreDoc : topDocsOfKnnSubquery.scoreDocs) {
                scoreDoc.score *= multiplier;
            }
        }
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
}
