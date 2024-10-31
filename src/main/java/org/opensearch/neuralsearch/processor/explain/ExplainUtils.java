/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import org.apache.lucene.search.Explanation;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;

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
    public static Map<DocIdAtSearchShard, ExplainDetails> getDocIdAtQueryForNormalization(
        final Map<DocIdAtSearchShard, List<Float>> normalizedScores,
        final Map<DocIdAtSearchShard, List<Float>> sourceScores
    ) {
        Map<DocIdAtSearchShard, ExplainDetails> explain = sourceScores.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                List<Float> srcScores = entry.getValue();
                List<Float> normScores = normalizedScores.get(entry.getKey());
                return new ExplainDetails(
                    normScores.stream().reduce(0.0f, Float::max),
                    String.format(Locale.ROOT, "source scores: %s normalized to scores: %s", srcScores, normScores)
                );
            }));
        return explain;
    }

    /**
     * Return the detailed score combination explain for the single document
     * @param docId
     * @param combinedNormalizedScoresByDocId
     * @param normalizedScoresPerDoc
     * @return
     */
    public static ExplainDetails getScoreCombinationExplainDetailsForDocument(
        final Integer docId,
        final Map<Integer, Float> combinedNormalizedScoresByDocId,
        final float[] normalizedScoresPerDoc
    ) {
        float combinedScore = combinedNormalizedScoresByDocId.get(docId);
        return new ExplainDetails(
            combinedScore,
            String.format(
                Locale.ROOT,
                "normalized scores: %s combined to a final score: %s",
                Arrays.toString(normalizedScoresPerDoc),
                combinedScore
            ),
            docId
        );
    }

    /**
     * Creates a string describing the combination technique and its parameters
     * @param techniqueName the name of the combination technique
     * @param weights the weights used in the combination technique
     * @return a string describing the combination technique and its parameters
     */
    public static String describeCombinationTechnique(final String techniqueName, final List<Float> weights) {
        return String.format(Locale.ROOT, "combination [%s] with optional parameter [%s]: %s", techniqueName, PARAM_NAME_WEIGHTS, weights);
    }

    /**
     * Creates an Explanation object for the top-level explanation of the combined score
     * @param explainableNormalizationTechnique the normalization technique used
     * @param explainableCombinationTechnique the combination technique used
     * @return an Explanation object for the top-level explanation of the combined score
     */
    public static Explanation topLevelExpalantionForCombinedScore(
        final ExplainableTechnique explainableNormalizationTechnique,
        final ExplainableTechnique explainableCombinationTechnique
    ) {
        String explanationDetailsMessage = String.format(
            Locale.ROOT,
            "combined score with techniques: %s, %s",
            explainableNormalizationTechnique.describe(),
            explainableCombinationTechnique.describe()
        );

        return Explanation.match(0.0f, explanationDetailsMessage);
    }

}
