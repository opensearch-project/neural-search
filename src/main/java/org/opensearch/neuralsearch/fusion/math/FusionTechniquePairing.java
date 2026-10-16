/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Which combination techniques may follow which normalization technique. This is the matrix the classic
 * normalization-processor has always enforced, lifted out so the fused path and any future executor read the same table
 * instead of a copy that could drift.
 *
 * <p>The table is keyed on the normalization technique, so it describes the normalization-processor's shape. The
 * score-ranker-processor pairs {@link FusionTechniqueNames#RRF} normalization with {@link FusionTechniqueNames#RRF_COMBINATION}
 * and is not expressed here; callers that accept that shape exempt it before consulting this matrix.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FusionTechniquePairing {

    private static final Set<String> THE_THREE_MEANS = Set.of(
        FusionTechniqueNames.ARITHMETIC_MEAN,
        FusionTechniqueNames.GEOMETRIC_MEAN,
        FusionTechniqueNames.HARMONIC_MEAN
    );

    private static final Map<String, Set<String>> COMBINATIONS_BY_NORMALIZATION = Map.of(
        FusionTechniqueNames.MIN_MAX,
        THE_THREE_MEANS,
        FusionTechniqueNames.L2,
        THE_THREE_MEANS,
        FusionTechniqueNames.RRF,
        THE_THREE_MEANS,
        FusionTechniqueNames.Z_SCORE,
        Set.of(FusionTechniqueNames.ARITHMETIC_MEAN)
    );

    /**
     * @return the combination techniques classic allows after {@code normalizationTechnique}; empty for an unknown
     *         normalization technique
     */
    public static Set<String> combinationTechniquesFor(final String normalizationTechnique) {
        if (normalizationTechnique == null) {
            return Set.of();
        }
        return COMBINATIONS_BY_NORMALIZATION.getOrDefault(normalizationTechnique, Set.of());
    }

    /**
     * @return whether {@code combinationTechnique} may follow {@code normalizationTechnique}
     */
    public static boolean isCompatible(final String normalizationTechnique, final String combinationTechnique) {
        return combinationTechniquesFor(normalizationTechnique).contains(combinationTechnique);
    }

    /**
     * Fail with the classic message when the pairing is not allowed. The wording is part of the user-facing contract (tests
     * on both the classic and the fused path assert it), so every caller raises it from here.
     */
    public static void requireCompatible(final String normalizationTechnique, final String combinationTechnique) {
        if (isCompatible(normalizationTechnique, combinationTechnique) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "provided combination technique %s is not supported for normalization technique %s. Supported techniques are: %s",
                    combinationTechnique,
                    normalizationTechnique,
                    String.join(", ", new TreeSet<>(combinationTechniquesFor(normalizationTechnique)))
                )
            );
        }
    }
}
