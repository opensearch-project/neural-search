/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
        Map<DocIdAtSearchShard, ExplanationDetails> explain = new HashMap<>();
        for (Map.Entry<DocIdAtSearchShard, List<Float>> entry : normalizedScores.entrySet()) {
            List<Float> normScores = normalizedScores.get(entry.getKey());
            List<Pair<Float, String>> explanations = new ArrayList<>();
            for (float score : normScores) {
                String description = String.format(Locale.ROOT, "%s normalization of:", technique.describe());
                explanations.add(Pair.of(score, description));
            }
            explain.put(entry.getKey(), new ExplanationDetails(explanations));
        }

        return explain;
    }

    /**
     * Creates a string describing the combination technique and its parameters
     * @param techniqueName the name of the combination technique
     * @param weights the weights used in the combination technique
     * @return a string describing the combination technique and its parameters
     */
    public static String describeCombinationTechnique(final String techniqueName, final List<Float> weights) {
        if (Objects.isNull(techniqueName)) {
            throw new IllegalArgumentException("combination technique name cannot be null");
        }
        return Optional.ofNullable(weights)
            .filter(w -> !w.isEmpty())
            .map(w -> String.format(Locale.ROOT, "%s, weights %s", techniqueName, weights))
            .orElse(String.format(Locale.ROOT, "%s", techniqueName));
    }
}
