/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

/**
 * A way of combining a hybrid query's per-sub-query scores into one score.
 *
 * <p>Extends {@link ExplainableTechnique} so that every combination technique is describable by construction. Both of
 * that interface's methods are {@code default}, so this costs an implementer nothing — and it is what lets the explain
 * paths call {@code describe()} directly instead of casting. Before it, the classic and fused explain paths each cast to
 * {@code ExplainableTechnique} unguarded, so a technique that omitted the interface compiled and then threw a
 * {@code ClassCastException} at runtime, on explained requests only.
 */
public interface ScoreCombinationTechnique extends ExplainableTechnique {

    /**
     * Defines combination function specific to this technique
     * @param scores array of collected original scores
     * @return combined score
     */
    float combine(final float[] scores);

    /**
     * Returns the name of the combination technique.
     */
    String techniqueName();
}
