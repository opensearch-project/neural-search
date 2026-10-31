/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.RRFScoreNormalizer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Resolves a coordinator-side {@link ScalarNormalizer} by technique name — the extension point for widening fused-mode
 * normalization support. Adding a technique is a new {@link ScalarNormalizer} plus one entry here; neither
 * {@link CoordinatorScoreFusion} nor the orchestrator changes.
 *
 * <p>Names map to factories rather than instances, mirroring classic's
 * {@link org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory}, because a technique may be
 * parameterized: {@code rrf} carries a rank_constant. The three score-based techniques are stateless, so their factories
 * hand back a shared singleton and ignore the parameters.
 *
 * <p>The whole score-normalization family — {@code min_max}, {@code z_score}, {@code l2} — is wired, each delegating to the
 * shared scalar core the classic shard-side path also uses, as is rank-based {@code rrf}. The fused-mode scope gate in
 * {@code HybridQueryBuilder#requireSupportedTechniques} rejects anything else earlier, at rewrite, so the throw below is a
 * defense-in-depth backstop rather than the user-facing validation.
 *
 * <p>{@code rrf} reaches this registry under a normalization name even though the score-ranker-processor has no
 * normalization clause, because that is what classic does too: {@code RRFProcessorFactory} builds an
 * {@link org.opensearch.neuralsearch.processor.normalization.RRFNormalizationTechnique}. So {@code combination=rrf}
 * resolves to {@code normalization=rrf} in {@code FusionSpec}, and rank-based fusion needs no routing of its own.
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

    private static final Map<String, Function<Map<String, Object>, ScalarNormalizer>> NORMALIZERS = Map.of(
        MinMaxScalarNormalizer.INSTANCE.techniqueName(),
        parameters -> MinMaxScalarNormalizer.INSTANCE,
        ZScoreScalarNormalizer.INSTANCE.techniqueName(),
        parameters -> ZScoreScalarNormalizer.INSTANCE,
        L2ScalarNormalizer.INSTANCE.techniqueName(),
        parameters -> L2ScalarNormalizer.INSTANCE,
        RRFNormalizationTechnique.TECHNIQUE_NAME,
        parameters -> new RrfScalarNormalizer(RRFScoreNormalizer.resolveRankConstant(parameters))
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
     * Resolve a technique that takes no parameters.
     *
     * @param techniqueName normalization technique name from the resolved fusion config
     * @return the normalizer for that technique
     * @throws IllegalArgumentException when the technique has no coordinator-side implementation yet
     */
    public static ScalarNormalizer forTechnique(final String techniqueName) {
        return forTechnique(techniqueName, Map.of());
    }

    /**
     * @param techniqueName normalization technique name from the resolved fusion config
     * @param parameters technique parameters ({@code rank_constant} for {@code rrf}); ignored by the score-based techniques
     * @return the normalizer for that technique
     * @throws IllegalArgumentException when the technique has no coordinator-side implementation yet
     */
    public static ScalarNormalizer forTechnique(final String techniqueName, final Map<String, Object> parameters) {
        Function<Map<String, Object>, ScalarNormalizer> factory = Objects.isNull(techniqueName) ? null : NORMALIZERS.get(techniqueName);
        if (Objects.isNull(factory)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "normalization technique [%s] is not supported in fused mode; supported: %s",
                    techniqueName,
                    NORMALIZERS.keySet()
                )
            );
        }
        return factory.apply(parameters);
    }
}
