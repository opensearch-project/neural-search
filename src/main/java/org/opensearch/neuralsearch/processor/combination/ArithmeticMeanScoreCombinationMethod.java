/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Abstracts combination of scores based on arithmetic mean method
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ArithmeticMeanScoreCombinationMethod implements ScoreCombinationMethod {

    private static final ArithmeticMeanScoreCombinationMethod INSTANCE = new ArithmeticMeanScoreCombinationMethod();

    public static ArithmeticMeanScoreCombinationMethod getInstance() {
        return INSTANCE;
    }

    /**
     * Arithmetic mean method for combining scores.
     * cscore = (score1 + score2 +...+ scoreN)/N
     *
     * Zero (0.0) scores are excluded from number of scores N
     */
    @Override
    public float combine(final float[] scores) {
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
}
