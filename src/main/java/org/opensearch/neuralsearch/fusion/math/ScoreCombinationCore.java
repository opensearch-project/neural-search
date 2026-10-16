/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

/**
 * The per-document half of a combination technique: turn one document's normalized leg scores into its fused score.
 * A leg the document did not match contributes {@code 0.0f} in its slot, as every classic technique has always read it.
 *
 * <p>Implementations hold only their validated {@link CombinationWeights}; they know nothing about documents, shards or
 * requests, so one instance serves everything that uses the technique with the same weights.
 */
public interface ScoreCombinationCore {

    /** @return the technique's request-facing name, one of the combination constants in {@link FusionTechniqueNames} */
    String techniqueName();

    /**
     * @param normalizedScores one slot per leg, {@code 0.0f} where the document is absent from the leg
     * @return the document's fused score
     * @throws IllegalArgumentException when weights were given and their count differs from the number of legs
     */
    float combine(float[] normalizedScores);

    /** @return the weights this core combines with */
    CombinationWeights weights();

    /** @return the one-line description explain output prints for this technique, {@code "<name>, weights [...]"} */
    default String describe() {
        return FusionTechniqueDescriptions.describeCombination(techniqueName(), weights().asList());
    }
}
