/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

public interface ScoreCombinationTechnique {

    /**
     * Defines combination function specific to this technique
     * @param scores array of collected original scores
     * @return combined score
     */
    float combine(final float[] scores);

    String techniqueName();
}
