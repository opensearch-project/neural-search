/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import org.opensearch.neuralsearch.processor.DocIdAtQueryPhase;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for explain functionality
 */
public class ExplainUtils {

    /**
     * Creates map of DocIdAtQueryPhase to String containing source and normalized scores
     * @param normalizedScores map of DocIdAtQueryPhase to normalized scores
     * @param sourceScores map of DocIdAtQueryPhase to source scores
     * @return map of DocIdAtQueryPhase to String containing source and normalized scores
     */
    public static Map<DocIdAtQueryPhase, String> getDocIdAtQueryPhaseStringMap(
        final Map<DocIdAtQueryPhase, List<Float>> normalizedScores,
        final Map<DocIdAtQueryPhase, List<Float>> sourceScores
    ) {
        Map<DocIdAtQueryPhase, String> explain = sourceScores.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            List<Float> srcScores = entry.getValue();
            List<Float> normScores = normalizedScores.get(entry.getKey());
            return String.format("source scores %s normalized scores %s", srcScores, normScores);
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
        return String.format(Locale.ROOT, "combination technique [%s] with optional parameters [%s]", techniqueName, weights);
    }
}
