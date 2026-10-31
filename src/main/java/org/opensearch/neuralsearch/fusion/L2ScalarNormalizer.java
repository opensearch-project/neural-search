/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * {@code l2} implementation of {@link ScalarNormalizer}. Computes the leg's L2 norm and divides each score by it through
 * the shared {@link L2ScoreNormalizer} — the same arithmetic the classic shard-side path runs.
 *
 * <p>One parity caveat, inherited from the formula rather than introduced here: {@code float} addition is not associative,
 * and the sum of squares is accumulated in {@code float} (deliberately — see {@link L2ScoreNormalizer.NormAccumulator}).
 * Classic accumulates per shard while this accumulates over the already-merged leg, so with a single shard the two norms
 * are bit-identical, and across shards they can differ in the last bit. That is a difference in accumulation order, not in
 * the formula.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class L2ScalarNormalizer implements ScalarNormalizer {

    public static final L2ScalarNormalizer INSTANCE = new L2ScalarNormalizer();

    @Override
    public Map<String, Float> normalizeLeg(final Map<String, Float> legRawScores) {
        float l2Norm = L2ScoreNormalizer.l2Norm(legRawScores.values());

        final Map<String, Float> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Float> entry : legRawScores.entrySet()) {
            normalized.put(entry.getKey(), L2ScoreNormalizer.normalizeSingleScore(entry.getValue(), l2Norm));
        }
        return normalized;
    }

    @Override
    public String techniqueName() {
        return L2ScoreNormalizationTechnique.TECHNIQUE_NAME;
    }
}
