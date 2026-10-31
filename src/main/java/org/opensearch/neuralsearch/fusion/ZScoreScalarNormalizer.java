/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ZScoreNormalizer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * {@code z_score} implementation of {@link ScalarNormalizer}. Accumulates the leg's mean, standard deviation, max and min
 * through the shared {@link ZScoreNormalizer.StatsAccumulator}, then rescales each score through the shared
 * {@link ZScoreNormalizer} — the same arithmetic the classic shard-side path runs. Sharing the accumulator as well as the
 * formula means the two paths cannot disagree on how a statistic is defined, only on the hit set it is computed over.
 *
 * <p>On the coordinator a leg's map is already the merged across-shard result set, so the statistics are computed in one
 * pass with no mergeable accumulator needed (see {@link ScalarNormalizer}). Classic computes the same statistics over the
 * same values; {@code DescriptiveStatistics} accumulates in {@code double} before narrowing, so the two agree closely, and
 * exactly on a single shard where the traversal order matches.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ZScoreScalarNormalizer implements ScalarNormalizer {

    public static final ZScoreScalarNormalizer INSTANCE = new ZScoreScalarNormalizer();

    @Override
    public Map<String, Float> normalizeLeg(final Map<String, Float> legRawScores) {
        ZScoreNormalizer.StatsAccumulator statistics = new ZScoreNormalizer.StatsAccumulator();
        for (float raw : legRawScores.values()) {
            statistics.add(raw);
        }
        // Read the statistics once rather than per score; each getter narrows a double, so hoisting also keeps every score
        // in the leg measured against exactly the same float values.
        float mean = statistics.mean();
        float standardDeviation = statistics.standardDeviation();
        float max = statistics.max();
        float min = statistics.min();

        final Map<String, Float> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Float> entry : legRawScores.entrySet()) {
            normalized.put(entry.getKey(), ZScoreNormalizer.normalizeSingleScore(entry.getValue(), standardDeviation, mean, max, min));
        }
        return normalized;
    }

    @Override
    public String techniqueName() {
        return ZScoreNormalizationTechnique.TECHNIQUE_NAME;
    }
}
