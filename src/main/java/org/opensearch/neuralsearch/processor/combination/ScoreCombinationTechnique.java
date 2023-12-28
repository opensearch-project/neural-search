/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.neuralsearch.processor.combination;

public interface ScoreCombinationTechnique {

    /**
     * Defines combination function specific to this technique
     * @param scores array of collected original scores
     * @return combined score
     */
    float combine(final float[] scores);
}
