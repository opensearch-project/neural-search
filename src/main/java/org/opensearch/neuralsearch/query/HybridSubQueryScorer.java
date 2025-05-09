/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Data;
import org.apache.lucene.search.Scorable;

import java.io.IOException;
import java.util.Arrays;

/**
 * Scorer implementation for Hybrid Query. This object is light and expected to be re-used between different doc ids
 */
@Data
public class HybridSubQueryScorer extends Scorable {
    // array of scores from all sub-queries for a single doc id
    private final float[] subQueryScores;
    // array of min competitive scores, score is shard level
    private final float[] minScores;

    public HybridSubQueryScorer(int numOfSubQueries) {
        this.minScores = new float[numOfSubQueries];
        this.subQueryScores = new float[numOfSubQueries];
    }

    @Override
    public float score() throws IOException {
        // for scenarios when scorer is needed (like in aggregations) for one doc id return sum of sub-query scores
        float totalScore = 0.0f;
        for (float score : subQueryScores) {
            totalScore += score;
        }
        return totalScore;
    }

    /**
     * Reset sub-query scores to 0.0f so this scorer can be reused for next doc id
     */
    public void resetScores() {
        Arrays.fill(subQueryScores, 0.0f);
    }

    public int getNumOfSubQueries() {
        return subQueryScores.length;
    }
}
