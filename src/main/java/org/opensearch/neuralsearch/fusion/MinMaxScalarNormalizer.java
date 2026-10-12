/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizer;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * {@code min_max} implementation of {@link ScalarNormalizer}. Gathers the leg's min/max with classic's exact
 * {@code Float.MAX_VALUE}/{@code Float.MIN_VALUE} seeding (see {@link MinMaxScoreNormalizationTechnique}'s
 * {@code getMinScores}/{@code getMaxScores}), then rescales each score through the shared
 * {@link MinMaxScoreNormalizer} — the same arithmetic the classic shard-side path runs. That shared math is what makes
 * fused and classic produce identical fused scores for an identical hit set (asserted by the differential test).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MinMaxScalarNormalizer implements ScalarNormalizer {

    public static final MinMaxScalarNormalizer INSTANCE = new MinMaxScalarNormalizer();

    @Override
    public Map<String, Float> normalizeLeg(final Map<String, Float> legRawScores) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        for (float raw : legRawScores.values()) {
            min = Math.min(min, raw);
            max = Math.max(max, raw);
        }
        final Map<String, Float> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Float> entry : legRawScores.entrySet()) {
            normalized.put(entry.getKey(), MinMaxScoreNormalizer.normalizeSingleScore(entry.getValue(), min, max));
        }
        return normalized;
    }

    @Override
    public String techniqueName() {
        return MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME;
    }
}
