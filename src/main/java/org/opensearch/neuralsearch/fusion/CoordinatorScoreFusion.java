/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Coordinator-side score fusion for the resolver (fused) mode, over an {@code _id}-keyed per-leg score view (shape
 * that coordinator has after fanning legs out as a {@code MultiSearch}). It deliberately reuses the same relevance math
 * as classic shard-side hybrid — {@link MinMaxScoreNormalizer} for normalization and the caller-supplied
 * {@link ScoreCombinationTechnique#combine(float[])} for combination — so that, for an identical hit set, this path and
 * classic produce identical fused scores to float precision. Only the data shape differs ({@code _id} map here vs.
 * {@code CompoundTopDocs} there), never the arithmetic.
 *
 * <p>Two classic behaviors are reproduced exactly:
 * <ul>
 *   <li>per-leg min/max is gathered with classic's {@code Float.MAX_VALUE}/{@code Float.MIN_VALUE} seeding, over that
 *       leg's hits (which on the coordinator are already the merged across-shard set);</li>
 *   <li>per-doc combine input is a {@code float[legCount]} initialized to {@code 0.0}, with only the legs that
 *       matched the doc filled in — mirroring classic's {@code ScoreCombiner#getNormalizedScoresPerDocument}. A leg
 *       that did not match a doc therefore contributes a {@code 0.0} slot, which the arithmetic-mean combiner counts
 *       toward the denominator (its {@code score >= 0.0} participation rule), exactly as classic does.</li>
 * </ul>
 *
 * <p>Current scope: {@code min_max} normalization + the supplied combination technique (arithmetic_mean). Other techniques
 * (z_score, l2, RRF) join this path in a later change. This entry is not reachable from a live request yet — the
 * coordinator rewrite wiring lands with the execution path.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CoordinatorScoreFusion {

    /**
     * Fuse legs with {@code min_max} normalization followed by the given combination technique.
     *
     * @param legRawScores        one entry per leg, each an {@code _id -> raw score} map of that leg's hits (order is
     *                            the leg order; a doc absent from a leg's map did not match that leg)
     * @param combinationTechnique the same combination technique classic would use (e.g. arithmetic_mean with weights)
     * @return {@code _id -> fused score} for the union of all legs' docs
     */
    public static Map<String, Float> fuseMinMax(
        final List<Map<String, Float>> legRawScores,
        final ScoreCombinationTechnique combinationTechnique
    ) {
        final int legCount = legRawScores.size();

        // Per-leg min/max, seeded exactly as classic (getMinScores/getMaxScores)
        // picking array over List as it's slightly faster, we know size and access by index has identical behavior
        final float[] minPerLeg = new float[legCount];
        final float[] maxPerLeg = new float[legCount];
        for (int leg = 0; leg < legCount; leg++) {
            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;
            for (float raw : legRawScores.get(leg).values()) {
                min = Math.min(min, raw);
                max = Math.max(max, raw);
            }
            minPerLeg[leg] = min;
            maxPerLeg[leg] = max;
        }

        // Union of ids across legs, preserving first-seen order for deterministic output.
        final Set<String> allIds = new LinkedHashSet<>();
        for (Map<String, Float> leg : legRawScores) {
            allIds.addAll(leg.keySet());
        }

        final Map<String, Float> fused = new LinkedHashMap<>();
        for (String id : allIds) {
            // float[legCount] initialized to 0.0; only matching legs are filled (mirrors classic per-doc array).
            final float[] perLegNormalized = new float[legCount];
            for (int leg = 0; leg < legCount; leg++) {
                Float raw = legRawScores.get(leg).get(id);
                if (raw != null) {
                    perLegNormalized[leg] = MinMaxScoreNormalizer.normalizeSingleScore(raw, minPerLeg[leg], maxPerLeg[leg]);
                }
            }
            fused.put(id, combinationTechnique.combine(perLegNormalized));
        }
        return fused;
    }
}
