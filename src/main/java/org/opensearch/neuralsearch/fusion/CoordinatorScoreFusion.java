/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.ArrayList;
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
 * Coordinator-side score fusion for the resolver (fused) mode, over a per-leg score view keyed by document key (the
 * shape the coordinator has after fanning legs out as a {@code MultiSearch}). It deliberately reuses the same relevance
 * math as classic shard-side hybrid — {@link MinMaxScoreNormalizer} for normalization and the caller-supplied
 * {@link ScoreCombinationTechnique#combine(float[])} for combination — so that, for an identical hit set, this path and
 * classic produce identical fused scores to float precision. Only the data shape differs (a key→score map here vs.
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
 * <p>The normalization step is pluggable via {@link ScalarNormalizer} (resolved by name through
 * {@link ScalarNormalizers}); the combination step is the caller-supplied {@link ScoreCombinationTechnique}. This
 * class owns only the shape-level work that is identical for every technique: per-leg normalization dispatch, the doc
 * union, and building each doc's per-leg input array. Adding a technique therefore touches neither this class nor the
 * orchestrator — {@code rrf} joined this path that way, as a normalizer over ranks rather than a second fusion routine.
 * Current fused-mode scope is the score-normalization family + arithmetic_mean, plus rrf; the caller gates the rest at
 * rewrite.
 *
 * <p>Document keys are treated as <b>opaque strings</b> throughout — this class never parses or builds them — so the
 * caller can change its document identity scheme (e.g. to disambiguate same-{@code _id} docs across indices) without
 * touching fusion or any normalizer.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CoordinatorScoreFusion {

    /**
     * Fuse legs with {@code min_max} normalization followed by the given combination technique. Convenience wrapper over
     * {@link #fuse(List, ScalarNormalizer, ScoreCombinationTechnique)}; this is the pairing the classic-vs-fused
     * differential test pins.
     *
     * @param legRawScores        one entry per leg, each a {@code key -> raw score} map of that leg's hits (order is
     *                            the leg order; a doc absent from a leg's map did not match that leg)
     * @param combinationTechnique the same combination technique classic would use (e.g. arithmetic_mean with weights)
     * @return {@code key -> fused score} for the union of all legs' docs
     */
    public static Map<String, Float> fuseMinMax(
        final List<Map<String, Float>> legRawScores,
        final ScoreCombinationTechnique combinationTechnique
    ) {
        return fuse(legRawScores, MinMaxScalarNormalizer.INSTANCE, combinationTechnique);
    }

    /**
     * Fuse legs: normalize each leg with {@code normalizer}, then combine across legs per doc.
     *
     * @param legRawScores        one entry per leg, each a {@code key -> raw score} map of that leg's hits
     * @param normalizer          per-leg normalization step (e.g. {@code min_max})
     * @param combinationTechnique cross-leg combination step (e.g. arithmetic_mean with weights)
     * @return {@code key -> fused score} for the union of all legs' docs
     */
    public static Map<String, Float> fuse(
        final List<Map<String, Float>> legRawScores,
        final ScalarNormalizer normalizer,
        final ScoreCombinationTechnique combinationTechnique
    ) {
        return fuseDetailed(legRawScores, normalizer, combinationTechnique).fused();
    }

    /**
     * What fusion computed: the fused score per document, and the per-leg normalized scores it was computed from.
     *
     * <p>The normalized values are the only record of <i>how</i> a fused score was reached, and they exist nowhere else —
     * a leg hit carries its raw score and round 2 carries the fused one, with the step between them entirely on the
     * coordinator. Reporting them is what lets {@code explain} describe a fused score rather than merely restate it.
     *
     * @param fused               {@code key -> fused score} for the union of all legs' docs
     * @param legNormalizedScores one entry per leg, in leg order, each a {@code key -> normalized score} map over that
     *                            leg's own hits (a key absent from a leg's map did not match that leg)
     */
    public record FusionResult(Map<String, Float> fused, List<Map<String, Float>> legNormalizedScores) {
    }

    /**
     * As {@link #fuse}, additionally reporting the per-leg normalized scores. Same arithmetic and same allocation —
     * {@code fuse} is this method with the detail dropped — so there is no fast path to choose between and no way for the
     * explained and unexplained paths to compute different scores.
     *
     * @param legRawScores         one entry per leg, each a {@code key -> raw score} map of that leg's hits
     * @param normalizer           per-leg normalization step (e.g. {@code min_max})
     * @param combinationTechnique cross-leg combination step (e.g. arithmetic_mean with weights)
     * @return the fused scores and the per-leg normalized scores behind them
     */
    public static FusionResult fuseDetailed(
        final List<Map<String, Float>> legRawScores,
        final ScalarNormalizer normalizer,
        final ScoreCombinationTechnique combinationTechnique
    ) {
        final int legCount = legRawScores.size();

        // Normalize each leg independently. A leg's map is already the merged across-shard set, so the normalizer sees
        // every value it needs to compute its own statistics (min/max here) with no cross-shard merging.
        final List<Map<String, Float>> legNormalizedScores = new ArrayList<>(legCount);
        for (Map<String, Float> legScores : legRawScores) {
            legNormalizedScores.add(normalizer.normalizeLeg(legScores));
        }

        // Union of keys across legs, preserving first-seen order for deterministic output.
        final Set<String> allKeys = new LinkedHashSet<>();
        for (Map<String, Float> legScores : legRawScores) {
            allKeys.addAll(legScores.keySet());
        }

        final Map<String, Float> fused = new LinkedHashMap<>();
        for (String key : allKeys) {
            // float[legCount] initialized to 0.0; only matching legs are filled (mirrors classic per-doc array).
            final float[] docScoresPerLeg = new float[legCount];
            for (int leg = 0; leg < legCount; leg++) {
                Float normalizedScore = legNormalizedScores.get(leg).get(key);
                if (normalizedScore != null) {
                    docScoresPerLeg[leg] = normalizedScore;
                }
            }
            fused.put(key, combinationTechnique.combine(docScoresPerLeg));
        }
        return new FusionResult(fused, List.copyOf(legNormalizedScores));
    }
}
