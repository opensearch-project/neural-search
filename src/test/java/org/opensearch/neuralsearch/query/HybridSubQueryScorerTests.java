/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import org.opensearch.test.OpenSearchTestCase;

public class HybridSubQueryScorerTests extends OpenSearchTestCase {

    private static final int NUM_SUB_QUERIES = 2;

    public void testGetSubQueryScores_whenInitialized_thenReturnCorrectSize() {
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(NUM_SUB_QUERIES);
        float[] scores = scorer.getSubQueryScores();

        assertEquals(NUM_SUB_QUERIES, scores.length);
        assertEquals(NUM_SUB_QUERIES, scorer.getNumOfSubQueries());
    }

    public void testResetScores_whenScoresSet_thenAllScoresZero() {
        HybridSubQueryScorer scorer = new HybridSubQueryScorer(NUM_SUB_QUERIES);
        float[] scores = scorer.getSubQueryScores();
        scores[0] = 0.5f;
        scores[1] = 1.0f;

        scorer.resetScores();

        // verify all scores are reset to 0
        for (float score : scorer.getSubQueryScores()) {
            assertEquals(0.0f, score, 0.0f);
        }
    }
}
