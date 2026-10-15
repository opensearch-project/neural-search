/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * The one-line technique descriptions that explain output prints. Kept here, next to the arithmetic, so that the classic
 * explanation processor and any other executor render a technique with the same words.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FusionTechniqueDescriptions {

    /**
     * @param techniqueName combination technique name
     * @param weights       the weights, or empty / {@code null} when unweighted
     * @return {@code "<name>, weights [w1, w2]"} when weighted, otherwise just the name
     */
    public static String describeCombination(final String techniqueName, final List<Float> weights) {
        if (Objects.isNull(techniqueName)) {
            throw new IllegalArgumentException("combination technique name cannot be null");
        }
        return Optional.ofNullable(weights)
            .filter(w -> w.isEmpty() == false)
            .map(w -> String.format(Locale.ROOT, "%s, weights %s", techniqueName, weights))
            .orElse(String.format(Locale.ROOT, "%s", techniqueName));
    }
}
