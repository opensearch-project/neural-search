/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import org.opensearch.neuralsearch.fusion.math.ScoreCombinationCore;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

/**
 * A combination technique as the classic processors see it: the shared {@link ScoreCombinationCore} arithmetic plus the
 * processor-side explain contract. The arithmetic is inherited from the core so that anything written against the core
 * accepts a classic technique as well.
 */
public interface ScoreCombinationTechnique extends ScoreCombinationCore, ExplainableTechnique {

    /**
     * Defines combination function specific to this technique
     * @param scores - array of collected original scores
     * @return combined score
     */
    @Override
    float combine(final float[] scores);

    @Override
    String techniqueName();

    /** Both parents supply a default description; the technique renders its own, with its weights. */
    @Override
    String describe();
}
