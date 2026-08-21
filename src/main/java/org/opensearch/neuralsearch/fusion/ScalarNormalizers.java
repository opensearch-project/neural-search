/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Resolves a coordinator-side {@link ScalarNormalizer} by technique name — the extension point for widening fused-mode
 * normalization support. Adding a technique is a new {@link ScalarNormalizer} plus one entry here; neither
 * {@link CoordinatorScoreFusion} nor the orchestrator changes. A static holder rather than a factory, since nothing is
 * constructed: every technique is a stateless singleton, so the lookup hands back a shared instance.
 *
 * <p>The whole score-normalization family — {@code min_max}, {@code z_score}, {@code l2} — is wired, each delegating to the
 * shared scalar core the classic shard-side path also uses. The fused-mode scope gate in
 * {@code HybridQueryBuilder#requireSupportedTechniques} rejects anything else earlier, at rewrite, so the throw below is a
 * defense-in-depth backstop rather than the user-facing validation.
 *
 * <p>Still open, and deliberately left to whoever wires it:
 * <ul>
 *   <li><b>RRF</b> is rank-based and is modelled as {@code combination=rrf, normalization=none}, so it does not arrive
 *       here under a normalization name. It fits the {@link ScalarNormalizer} shape (sort the leg, emit
 *       {@code 1/(rank_constant + rank + 1)} by position) but needs its own routing plus a rank_constant parameter, and
 *       the classic {@code (doc, shardId)} rank tie-break has no equivalent over the coordinator's already-merged view —
 *       so a tie-break must be defined explicitly.</li>
 * </ul>
 *
 * <p>An earlier note here suggested geometric/harmonic mean would need an explicit presence mask rather than
 * {@link CoordinatorScoreFusion}'s {@code 0.0} "leg did not match" sentinel, because {@code l2} can legitimately normalize
 * a real score to {@code 0.0}. That turns out not to be a fused-vs-classic problem: classic's
 * {@code ScoreCombiner#getNormalizedScoresPerDocument} allocates {@code new float[legCount]} and fills only the legs that
 * matched, so <b>classic feeds the same {@code 0.0} for a non-matching leg</b>, and geometric/harmonic skip {@code <= 0}
 * identically on both paths. The residual ambiguity — a matched doc whose {@code l2} score really is {@code 0.0}, reachable
 * only when a leg's whole norm is 0 — exists identically in classic, so it is not a divergence. The sentinel stays.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ScalarNormalizers {

    private static final Map<String, ScalarNormalizer> NORMALIZERS = Map.of(
        MinMaxScalarNormalizer.INSTANCE.techniqueName(),
        MinMaxScalarNormalizer.INSTANCE,
        ZScoreScalarNormalizer.INSTANCE.techniqueName(),
        ZScoreScalarNormalizer.INSTANCE,
        L2ScalarNormalizer.INSTANCE.techniqueName(),
        L2ScalarNormalizer.INSTANCE
    );

    /**
     * Normalization technique names that have a coordinator-side implementation. Lets the caller's rewrite-time gate refuse
     * an unsupported technique with a single coherent message, instead of provoking {@link #forTechnique}'s backstop throw.
     *
     * @return the supported technique names
     */
    public static Set<String> supportedTechniques() {
        return NORMALIZERS.keySet();
    }

    /**
     * @param techniqueName normalization technique name from the resolved fusion config
     * @return the normalizer for that technique
     * @throws IllegalArgumentException when the technique has no coordinator-side implementation yet
     */
    public static ScalarNormalizer forTechnique(final String techniqueName) {
        ScalarNormalizer normalizer = Objects.isNull(techniqueName) ? null : NORMALIZERS.get(techniqueName);
        if (Objects.isNull(normalizer)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "normalization technique [%s] is not supported in fused mode; supported: %s",
                    techniqueName,
                    NORMALIZERS.keySet()
                )
            );
        }
        return normalizer;
    }
}
