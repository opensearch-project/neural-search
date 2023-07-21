/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor.combination;

import lombok.AllArgsConstructor;

/**
 * Collection of techniques for score combination
 */
@AllArgsConstructor
public enum ScoreCombinationTechnique {

    /**
     * Arithmetic mean method for combining scores.
     */
    ARITHMETIC_MEAN(ArithmeticMeanScoreCombinationMethod.getInstance());

    public static final ScoreCombinationTechnique DEFAULT = ARITHMETIC_MEAN;
    private final ScoreCombinationMethod method;

    public float combine(final float[] scores) {
        return method.combine(scores);
    }
}
