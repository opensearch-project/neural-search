/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

/**
 * Collection of techniques for score combination
 */
public enum ScoreCombinationTechnique {

    /**
     * Arithmetic mean method for combining scores.
     * cscore = (score1 + score2 +...+ scoreN)/N
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    ARITHMETIC_MEAN {

        @Override
        public float combine(float[] scores) {
            float combinedScore = 0.0f;
            int count = 0;
            for (float score : scores) {
                if (score >= 0.0) {
                    combinedScore += score;
                    count++;
                }
            }
            return combinedScore / count;
        }
    };

    public static final ScoreCombinationTechnique DEFAULT = ARITHMETIC_MEAN;

    /**
     * Defines combination function specific to this technique
     * @param scores array of collected original scores
     * @return combined score
     */
    abstract float combine(float[] scores);
}
