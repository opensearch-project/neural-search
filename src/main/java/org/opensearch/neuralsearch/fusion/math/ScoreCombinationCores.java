/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Resolves a combination technique name to its {@link ScoreCombinationCore}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ScoreCombinationCores {

    private static final Map<String, Function<CombinationWeights, ScoreCombinationCore>> CORES = Map.of(
        FusionTechniqueNames.ARITHMETIC_MEAN,
        ArithmeticMeanCombinationCore::new,
        FusionTechniqueNames.GEOMETRIC_MEAN,
        GeometricMeanCombinationCore::new,
        FusionTechniqueNames.HARMONIC_MEAN,
        HarmonicMeanCombinationCore::new,
        FusionTechniqueNames.RRF_COMBINATION,
        RrfCombinationCore::new
    );

    /** @return every combination technique name, sorted */
    public static Set<String> supportedTechniques() {
        return new TreeSet<>(CORES.keySet());
    }

    /**
     * @param techniqueName combination technique name
     * @param weights       validated weights, {@link CombinationWeights#UNWEIGHTED} when none
     * @return the core
     * @throws IllegalArgumentException for an unknown technique
     */
    public static ScoreCombinationCore forTechnique(final String techniqueName, final CombinationWeights weights) {
        Function<CombinationWeights, ScoreCombinationCore> factory = Objects.isNull(techniqueName) ? null : CORES.get(techniqueName);
        if (Objects.isNull(factory)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "combination technique [%s] is not supported; supported: %s",
                    techniqueName,
                    supportedTechniques()
                )
            );
        }
        return factory.apply(Objects.requireNonNull(weights, "weights must not be null"));
    }

    /** Unweighted form of {@link #forTechnique(String, CombinationWeights)}. */
    public static ScoreCombinationCore forTechnique(final String techniqueName) {
        return forTechnique(techniqueName, CombinationWeights.UNWEIGHTED);
    }
}
