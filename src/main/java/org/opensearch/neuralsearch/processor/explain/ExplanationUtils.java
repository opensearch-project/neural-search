/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility class for explain functionality
 */
public class ExplanationUtils {

    /**
     * Creates map of DocIdAtQueryPhase to String containing source and normalized scores
     * @param normalizedScores map of DocIdAtQueryPhase to normalized scores
     * @return map of DocIdAtQueryPhase to String containing source and normalized scores
     */
    public static Map<DocIdAtSearchShard, ExplanationDetails> getDocIdAtQueryForNormalization(
        final Map<DocIdAtSearchShard, List<Float>> normalizedScores,
        final ExplainableTechnique technique
    ) {
        Map<DocIdAtSearchShard, ExplanationDetails> explain = normalizedScores.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                List<Float> normScores = normalizedScores.get(entry.getKey());
                List<Pair<Float, String>> explanations = normScores.stream()
                    .map(score -> Pair.of(score, String.format(Locale.ROOT, "%s normalization of:", technique.describe())))
                    .collect(Collectors.toList());
                return new ExplanationDetails(explanations);
            }));
        return explain;
    }

    /**
     * Creates a string describing the combination technique and its parameters
     * @param techniqueName the name of the combination technique
     * @param weights the weights used in the combination technique
     * @return a string describing the combination technique and its parameters
     */
    public static String describeCombinationTechnique(final String techniqueName, final List<Float> weights) {
        return Optional.ofNullable(weights)
            .filter(w -> !w.isEmpty())
            .map(w -> String.format(Locale.ROOT, "%s, weights %s", techniqueName, weights))
            .orElse(String.format(Locale.ROOT, "%s", techniqueName));
    }
}
